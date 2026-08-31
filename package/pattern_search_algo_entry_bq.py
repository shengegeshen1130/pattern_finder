import argparse
from base_driver_config import BaseDriverConfig,PatternSearchAlgorithmInput
from pattern_search_config import PatternSearchConfig
from output_config import OutputConfig
# from PatternSearchSpark import PatternSearch as patternSearch_v2
# from PatternSearch import PatternSearch as patternSearch_v1
from PatternSearch import PatternSearch
from PatternEvaluate import ReportMetrics,PatternEvaluator,MetricEvaluator
from PatternUtils import PatternsUtil
from job_log import JobLog
from joblib import Parallel, delayed
from util import read_bq
import os 
import pandas as pd
import pandas_gbq
import datetime 
import logging
import sys
import json
import gcsfs
import yaml

logger = logging.getLogger()
logger.setLevel(logging.INFO)
console = logging.StreamHandler(stream = sys.__stderr__)
console.setLevel(logging.INFO)
FORMAT = '%(asctime)s - %(process)d - %(thread)d - %(funcName)s[%(levelname)s]: %(message)s'
console.setFormatter(logging.Formatter(FORMAT))
logger.addHandler(console)

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--base_bq_db',default = 'pypl-bods.prd_intime_app', dest = 'base_bq_db')
    parser.add_argument('--base_table_name', dest = 'base_table_name')
    parser.add_argument('--acct_col', dest = 'acct_col')
    parser.add_argument('--cp_acct_col',default = None, dest = 'cp_acct_col')
    parser.add_argument('--ts_col', dest = 'ts_col')
    parser.add_argument('--key_col', dest = 'key_col')
    parser.add_argument('--tag_col', dest = 'tag_col')
    parser.add_argument('--amt_col',default = None, dest = 'amt_col')
    parser.add_argument('--gloss_col', default = None, dest = 'gloss_col')
    parser.add_argument('--nloss_col',default = None, dest = 'nloss_col')
    parser.add_argument('--tw',type=int,default = 7, dest = 'tw')
    parser.add_argument('--tz',default= 'PST', dest = 'tz')
    parser.add_argument('--ts_type',default='datetime', dest = 'ts_type')

    parser.add_argument('--ftr_config_file', dest = 'ftr_config_file')
    parser.add_argument('--eval_metric', dest = 'eval_metric')
    parser.add_argument('--min_support',type=float, dest = 'min_support')
    parser.add_argument('--eval_cutoff',type=float, dest = 'eval_cutoff')
    parser.add_argument('--dedup_cutoff',type=float, dest = 'dedup_cutoff')
    parser.add_argument('--rank_priority',default='target_rate+pattr_score', dest = 'rank_priority')
    parser.add_argument('--gini_cutoff',type=float,default=0.2, dest = 'gini_cutoff')
    parser.add_argument('--lvl_limit',default=6,type=int, dest = 'lvl_limit')
    parser.add_argument('--min_block_size',type=float,default=0.05, dest = 'min_block_size')
    parser.add_argument('--overlap',type=float,default=0.5, dest = 'overlap')
    parser.add_argument('--trunc_cutoff',type=float,default=800000, dest = 'trunc_cutoff')
    parser.add_argument('--trunc_cp_size',type=float,default=10, dest = 'trunc_cp_size')


    parser.add_argument('--bq_db_name',default = 'pypl-bods.prd_intime_app',dest = 'bq_db_name')
    parser.add_argument('--activity_table_name', dest = 'activity_table_name')
    parser.add_argument('--tag_table_name', dest = 'tag_table_name')
    parser.add_argument('--pattr_log_table', default = 'service_pattr_res', dest = 'pattr_log_table')
    parser.add_argument('--act_log_table', default = 'service_act_res', dest = 'act_log_table')
    parser.add_argument('--algo_log_table', default = 'service_pattr_algo_log', dest = 'algo_log_table')
    parser.add_argument('--retention_day',default = 7, type = int, dest = 'retention_day')

    parser.add_argument('--batch_id',default='0',dest = 'batch_id')
    parser.add_argument('--alert_id',default='0', dest = 'alert_id')
    parser.add_argument('--job_id',default='0', dest = 'job_id')
    parser.add_argument('--job_type',default='default', dest = 'job_type')

    parser.add_argument('--create-spark-session', help='Whether create a spark session', type=bool, dest='create_spark_session', default=True)
    parser.add_argument('--excl_seg_mapping_table_name',default=None, dest = 'excl_seg_mapping_table_name')
    parser.add_argument('--excl_segs',default=None, dest = 'excl_segs')
    parser.add_argument('--deprecated_vars', default=None, dest='deprecated_vars')
    parser.add_argument('--var_config_file', dest = 'var_config_file')
    parser.add_argument('--search_scope', default='single', dest='search_scope')

    return parser.parse_args()

def get_excl_map_dict(excl_seg_mapping_table_name,excl_segs):
    """
    load excluded segment mapping only if table name and excl_segs input are not None
    """
    if excl_seg_mapping_table_name and excl_segs:
        excl_seg_json = json.loads(excl_segs)
        filter_cond = ' or '.join(["(alert_segment = '{arg_name}' and alert_segment_value ='{arg_val}')".format(arg_name=pair['name'],arg_val=pair['value'])  for pair in excl_seg_json])
        sql = """
        select * from {map_table}
        where {filter_cond}
        """.format(map_table=excl_seg_mapping_table_name,filter_cond = filter_cond)
        df_map = read_bq(sql)
        excl_map_dict = df_map.groupby(['checkpoint','config_segment'])['config_segment_value'].apply(list).to_dict()
        return excl_map_dict
    else:
        return None

def get_deprecated_var_list(deprecated_vars):
    if deprecated_vars:
        return json.loads(deprecated_vars)
    else:
        return None
    
def load_config(filename):
    fs = gcsfs.GCSFileSystem()
    with fs.open(filename, 'r') as stream:
        var_config = yaml.safe_load(stream)
    return var_config

if __name__ == "__main__":
    args = parse_args()
    if args.create_spark_session:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()

    print(args)
    print(os.listdir())
    print("Current Directory:",os.getcwd())

    print("pandas_gbq: %s" % (pandas_gbq.__version__))
    print("pandas: %s" % (pd.__version__))
    print("*"*50)
    updt_dt = datetime.date.today()
    updt_ts = datetime.datetime.now()
    filter_condition = ''

    job_log = JobLog({'job_id': args.job_id,'update_dt': updt_dt,'update_ts':updt_ts},args.bq_db_name,args.algo_log_table)
    res_log = JobLog({'job_id': args.job_id,'update_dt': updt_dt,'update_ts':updt_ts},args.bq_db_name,args.pattr_log_table)
    act_log = JobLog({'job_id': args.job_id,'update_dt': updt_dt,'update_ts':updt_ts},args.bq_db_name,args.act_log_table)

    job_log.create_empty_table(type = 'service_log')
    res_log.create_empty_table(type = 'service_result')
    act_log.create_empty_table(type = 'service_act_res')
    job_log.clean_log()
    res_log.clean_log()
    act_log.clean_log()
    metric_config_file = './default_report_metrics.yaml'

    var_config = load_config(args.var_config_file)
    driver_config = BaseDriverConfig(args.base_bq_db,args.base_table_name,args.acct_col,args.cp_acct_col,args.ts_col,args.key_col,args.tag_col,args.amt_col,args.gloss_col,args.nloss_col,args.tw,args.tz,args.ts_type)
    algo_output = OutputConfig(args.bq_db_name,args.activity_table_name, args.tag_table_name,args.retention_day)
    act_table = algo_output.bq_db_name+'.'+algo_output.activity_table_name
    algo_input = PatternSearchAlgorithmInput(driver_config,act_table,filter_condition)
    algo_param = PatternSearchConfig(args.ftr_config_file, args.eval_metric, args.min_support, args.eval_cutoff, args.dedup_cutoff, args.rank_priority,metric_config_file,args.gini_cutoff,args.lvl_limit,args.min_block_size,args.overlap,args.trunc_cutoff,args.trunc_cp_size)


    report_metrics = ReportMetrics()

    report_metrics.load_config(metric_config_file)
    pattr_eval = PatternEvaluator(report_metrics,algo_input,algo_param)
    metric_eval = MetricEvaluator(report_metrics,algo_input,algo_param)

    excl_map_dict = get_excl_map_dict(args.excl_seg_mapping_table_name,args.excl_segs)

    deprecated_var_list = get_deprecated_var_list(args.deprecated_vars)

    algo = PatternSearch(algo_input,algo_param,algo_output,pattr_eval,metric_eval,job_log,res_log,act_log,excl_map_dict,deprecated_var_list,var_config,args.search_scope)
    print("************* start algo *************")
    algo.search()