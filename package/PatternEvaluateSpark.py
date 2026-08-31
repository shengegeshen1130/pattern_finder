import yaml
import gcsfs
import re
from util import fscore,ftr_delim,cp_delim
import numpy as np
from loguru import logger
from pandasql import sqldf
import pandas as pd
import os 

class ReportMetrics:
    def __init__(self):
        self.metrics = []

    def load_config(self, filename):
        current_directory = os.path.dirname(__file__)
        # 打印当前文件所在的目录
        print("Current file is located in:", current_directory)
        # 切换到当前文件所在的目录
#         os.chdir(current_directory)
        # fs = gcsfs.GCSFileSystem()
        with open(filename, 'r') as stream:
            config = yaml.safe_load(stream)
        
        self.metrics = config['report_metrics']
        self.kpi_map = config['kpi_map']

class Evaluator:
    def __init__(self, report, algo_input, algo_param, spark):
        self.metrics = report.metrics
        self.kpi_map = report.kpi_map
        self.base_driver = algo_input.base_driver
        self.activity_table = algo_input.activity_table
        self.prefix = algo_input.prefix
        self.key_col = algo_input.key_col
        self.tag_col = algo_input.tag_col
        self.amt_col = algo_input.amt_col
        self.gloss_col = algo_input.gloss_col
        self.nloss_col = algo_input.nloss_col
        self.eval_metric = algo_param.eval_metric
        self.min_support_cutoff = algo_param.min_support
        self.eval_cutoff = algo_param.eval_cutoff
        self.dedup_cutoff = algo_param.dedup_cutoff
        self.spark = spark


    def gen_metric_sql(self,assgin_metrics=None):
        metric_template = ""
        dep_cols_template = ""
        dep_cols = []
        metric_list = []
        if assgin_metrics:
            for metric in assgin_metrics:
                depFulfilled = True
                if self.metrics[metric]['dependency']:
                    for dep in self.metrics[metric]['dependency']:
                        if not getattr(self, dep):
                            depFulfilled = False
                            break
                        if dep not in dep_cols:
                            dep_cols.append(dep)
                if depFulfilled:
                    metric_list.append(self.metrics[metric]['calc'].format(bad_tag=self.tag_col,amt_col=self.amt_col,nloss_col=self.nloss_col))
            metric_template = '\n,'.join([metric_desp for metric_desp in metric_list])
            dep_cols_template = '\n,'.join(['max({col}) as {col}'.format(col=getattr(self,col)) for col in dep_cols])
        
                
        else:
            for metric,logic in self.metrics.items():
                depFulfilled = True
                if logic['dependency']:
                    for dep in logic['dependency']:
                        if not getattr(self, dep):
                            depFulfilled = False
                            break
                        if dep not in dep_cols:
                            dep_cols.append(dep)
                if depFulfilled:
                    metric_list.append(self.metrics[metric]['calc'].format(bad_tag=self.tag_col,amt_col=self.amt_col,nloss_col=self.nloss_col))
            metric_template = '\n,'.join([metric_desp for metric_desp in metric_list])
            dep_cols_template = '\n,'.join(['max({col}) as {col}'.format(col=getattr(self,col)) for col in dep_cols])
        return metric_template,dep_cols_template

class PatternEvaluator(Evaluator):
    def pattr_score_cal(self,pattr_content,cp_priority,ftr_priority):
        score ={
        1:0.5
        ,2:0.2
        ,3:0.1
        }
        cp_list = []
        ftr_list = []
        pattr_score = 0
        for pattr_sub in pattr_content:
            # pattr_substr = pattr_sub.split(' and ')
            pattr_substr = pattr_sub.split(ftr_delim)
            ftr_score_list = []
            for idx,substr in enumerate(pattr_substr):
                if idx ==0:
                    match_group = re.findall(r"checkpoint=\"(.+)\"",substr)
                    if len(match_group)>0:
                        cp = match_group[0]
                        cp_list.append(cp)

                        pattr_score+=score[cp_priority[cp]]
                else:
                    match_group = re.findall(r"(\w+)[ in|<=|>|=]", substr)
                    if len(match_group)>0:
                        ftr_list.append(match_group[0])
                        ftr_score_list.append(score[ftr_priority[cp][match_group[0]]])
            if len(ftr_score_list)>0:
                pattr_score+=np.mean(ftr_score_list)
        cp_str_list = '-'.join(cp_list)
        return pattr_score,cp_str_list
    
    
    def pattern_perf(self,pattr_content):
        ## Condition Part
        metric_template,dep_cols_template = self.gen_metric_sql()
        cond_part_list = []
        cond_flag_list = []
        for idx, act in enumerate(pattr_content):
            cond_part = "max(case when {cond} then 1 else 0 end) as {flag_name}".format(cond = act.replace("==","=").replace("=\"None\""," is null"),flag_name = 'act_'+str(idx))
            cond_part_list.append(cond_part)
            cond_flag_list.append('act_'+str(idx)+' = 1')
        pattern_cond = " and ".join(cond_flag_list)
        activity_cond = "\n,".join(cond_part_list)
        template = """
                select 
                case when {pattern_cond} then 1 else 0 end as pattern_flag
                ,{metric_template}
                from population_view base
                left join (
                    select 
                    {key_col}
                    ,{activity_cond}
                    from activity_view
                    group by 1) act
                on base.{key_col}=act.{key_col}
                group by 1
                """.format(pattern_cond = pattern_cond,activity_cond = activity_cond,key_col=self.key_col,bad_tag=self.tag_col,amt_col = self.amt_col,nloss_col=self.nloss_col,metric_template=metric_template,dep_cols = dep_cols_template)
        df_eval = self.spark.sql(template).toPandas()
        df_eval = df_eval.fillna(0)
        if len(df_eval)>0:
            if len(df_eval[df_eval['pattern_flag']==1])>0:
                target_catch = df_eval[df_eval['pattern_flag']==1][self.kpi_map[self.eval_metric]['target_catch']].head(1).iloc[0]
                target_rate = df_eval[df_eval['pattern_flag']==1][self.kpi_map[self.eval_metric]['target_rate']].head(1).iloc[0]
                tot_catch = df_eval[self.kpi_map[self.eval_metric]['target_catch']].sum()
                pattr_fscore = fscore(target_rate,target_catch/tot_catch)
                pattr_metrics = {}
                for col in self.metrics:
                    if col in df_eval.columns:
                        pattr_metrics[col] = df_eval[df_eval['pattern_flag']==1][col].head(1).iloc[0]   
            else:
                target_catch = 0
                target_rate = 0
                pattr_fscore = 0
                pattr_metrics = {}
        else:
            raise Exception("Error in executing query: {query}".format(query=template))
        return pattr_metrics,target_catch,target_rate,pattr_fscore
    
    def pattern_compute_sql(self,pattr_content,sql_key):
        ## Condition Part
        metric_template,dep_cols_template = self.gen_metric_sql()
        cond_part_list = []
        cond_flag_list = []
        for idx, act in enumerate(pattr_content):
            cond_part = "max(case when {cond} then 1 else 0 end) as {flag_name}".format(cond = act.replace("==","=").replace("=\"None\""," is null").replace(ftr_delim," and "),flag_name = 'act_'+str(idx))
            cond_part_list.append(cond_part)
            cond_flag_list.append('act_'+str(idx)+' = 1')
        pattern_cond = " and ".join(cond_flag_list)
        activity_cond = "\n,".join(cond_part_list)
        template = """
                select 
                '{sql_key}' as sql_key
                ,case when {pattern_cond} then 1 else 0 end as pattern_flag
                ,{metric_template}
                from population_view base
                left join (
                    select 
                    {key_col}
                    ,{activity_cond}
                    from activity_view
                    group by 1) act
                on base.{key_col}=act.{key_col}
                group by 1,2
                """.format(sql_key=sql_key,base_table=self.base_driver,activity_table=self.activity_table,pattern_cond = pattern_cond,activity_cond = activity_cond,key_col=self.key_col,metric_template=metric_template,dep_cols = dep_cols_template)
        return template
    
    def pattern_compute_batch(self,sql_list):
        # print("sql_list:")
        # print(sql_list)
        if len(sql_list)>0:
            idx_range = np.arange(0,len(sql_list),100)
            res_list = []
            for idx,item in enumerate(idx_range):
                if idx<len(idx_range)-1:
                    start_idx = idx_range[idx]
                    end_idx = idx_range[idx+1]
                    template = '\n union all \n'.join(sql_list[start_idx:end_idx])

                else:
                    start_idx = idx_range[idx]
                    template = '\n union all \n'.join(sql_list[start_idx:])
                try:
                    df_eval = self.spark.sql(template).toPandas()
                    df_eval = df_eval.fillna(0)
                    res_list.append(df_eval)
                except Exception as err:
                    print("error message: %s"%str(err))
                    # raise Exception("Error in executing batch query: {query}".format(query=template))
            df_eval_all = pd.concat(res_list).reset_index(0).drop(columns = ['index'])
            eval_res = {}
            for sql_key in df_eval_all['sql_key'].unique():
                df_eval = df_eval_all[df_eval_all['sql_key']==sql_key]
                eval_res[sql_key] = {}
                if len(df_eval[df_eval['pattern_flag']==1])>0:
                    target_catch = df_eval[df_eval['pattern_flag']==1][self.kpi_map[self.eval_metric]['target_catch']].head(1).iloc[0]
                    target_rate = df_eval[df_eval['pattern_flag']==1][self.kpi_map[self.eval_metric]['target_rate']].head(1).iloc[0]
                    tot_catch = df_eval[self.kpi_map[self.eval_metric]['target_catch']].sum()
                    pattr_fscore = fscore(target_rate,target_catch/tot_catch)
                    pattr_metrics = {}
                    for col in self.metrics:
                        if col in df_eval.columns:
                            pattr_metrics[col] = df_eval[df_eval['pattern_flag']==1][col].head(1).iloc[0]   
                else:
                    target_catch = 0
                    target_rate = 0
                    pattr_fscore = 0
                    pattr_metrics = {}
                eval_res[sql_key]['target_catch'] = float(target_catch)
                eval_res[sql_key]['target_rate'] = float(target_rate)
                eval_res[sql_key]['pattr_fscore'] = float(pattr_fscore)
                eval_res[sql_key]['pattr_metrics'] = pattr_metrics


            return eval_res

class MetricEvaluator(Evaluator):
    def metric_compute(self,filter_condition):
        kpi_metrics = [self.kpi_map[self.eval_metric]['target_catch'],self.kpi_map[self.eval_metric]['target_rate']]
        metric_template,dep_cols_template = self.gen_metric_sql(kpi_metrics)
        template = '''
            select
            {metric_template}
            from 
            (
                select 
                {key_col}
                ,{dep_cols_template}
                from activity_view
                where 1=1
                {filter_condition}
                group by {key_col}
            )tab1
            '''.format(metric_template = metric_template, dep_cols_template=dep_cols_template, key_col=self.key_col,filter_condition=filter_condition)
        df_eval = self.spark.sql(template).toPandas()
        df_eval = df_eval.fillna(0)
        if len(df_eval)>0:
            target_catch = df_eval[self.kpi_map[self.eval_metric]['target_catch']].head(1).iloc[0]
            target_rate = df_eval[self.kpi_map[self.eval_metric]['target_rate']].head(1).iloc[0]
        else:
            raise Exception("Error in executing query: {query}".format(query=template))
        return float(target_catch),float(target_rate)

    def metric_sql_generate(self):
        kpi_metrics = [self.kpi_map[self.eval_metric]['target_catch'],self.kpi_map[self.eval_metric]['target_rate']]
        metric_template,dep_cols_template = self.gen_metric_sql(kpi_metrics)
        return metric_template, dep_cols_template
    
    def metric_compute_sql(self,filter_condition,sql_key):
        kpi_metrics = [self.kpi_map[self.eval_metric]['target_catch'],self.kpi_map[self.eval_metric]['target_rate']]
        metric_template,dep_cols_template = self.gen_metric_sql(kpi_metrics)
        template = '''
            select
            '{sql_key}' as sql_key
            ,{metric_template}
            from 
            (
                select 
                {key_col}
                ,{dep_cols_template}
                from activity_view
                where 1=1
                {filter_condition}
                group by {key_col}
            )tab1
            '''.format(sql_key=sql_key,metric_template = metric_template, dep_cols_template=dep_cols_template, key_col=self.key_col,filter_condition=filter_condition.replace(ftr_delim," and "))
        return template
    
    def metric_compute_batch(self,sql_list):
        # print("sql_list:")
        # print(sql_list)
        if len(sql_list)>0:
            template = '\n union all \n'.join(sql_list)
    
            try:
                df_eval = self.spark.sql(template).toPandas()
                df_eval = df_eval.fillna(0)
                # object_cols = df_eval.select_dtypes(include=['object']).columns
                # df_eval[object_cols] = df_eval[object_cols].astype(float)
                df_eval = df_eval.rename(columns = {self.kpi_map[self.eval_metric]['target_catch']:'target_catch',self.kpi_map[self.eval_metric]['target_rate']:'target_rate'})
                df_eval['target_catch'] = df_eval['target_catch'].astype(float)
                df_eval['target_rate'] = df_eval['target_rate'].astype(float)
                df_eval = df_eval.set_index('sql_key')
                eval_res = df_eval.T.to_dict()
                return eval_res

            except Exception as err:
                logger.error("error message: %s"%str(err))
                raise Exception("Error in executing batch query: {query} ex:{ex}".format(query=template, ex=err))

    def baseline_compute(self):
        kpi_metrics = [self.kpi_map[self.eval_metric]['target_catch'],self.kpi_map[self.eval_metric]['target_rate']]
        metric_template,dep_cols_template = self.gen_metric_sql(kpi_metrics)
        template = '''
            select
            {metric_template}
            from population_view
            '''.format(metric_template = metric_template, key_col=self.key_col)
        df_eval = self.spark.sql(template).toPandas()
        df_eval = df_eval.fillna(0)
        if len(df_eval)>0:
            target_catch = df_eval[self.kpi_map[self.eval_metric]['target_catch']].head(1).iloc[0]
            target_rate = df_eval[self.kpi_map[self.eval_metric]['target_rate']].head(1).iloc[0]
        else:
            raise Exception("Error in executing query: {query}".format(query=template))
        return float(target_catch),float(target_rate)    
