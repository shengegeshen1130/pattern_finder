import numpy as np
import pandas as pd
import re
from joblib import Parallel, delayed
from functools import reduce
import copy
from google.cloud import storage
from google.cloud import bigquery
import pandas_gbq
from treelib import Tree, Node
from wfpgrowth import find_frequent_patterns
from loguru import logger
from base_driver_config import BaseDriverConfig,PatternSearchAlgorithmInput
from output_config import OutputConfig
from pattern_search_config import PatternSearchConfig
from PatternEvaluateSpark import ReportMetrics,PatternEvaluator,MetricEvaluator
from PatternUtils import Pattern,PatternsUtil
from util import fscore,read_bq,pandas_gbq_numeric_type_patch,ftr_delim,cp_delim
import sys
import datetime
from job_log import JobLog
from store_log import StoreLog
import json
import random
from itertools import combinations
import networkx as nx
from datetime import date

class PatternSearch:
    def __init__(self,algo_input,algo_param,algo_output,pattr_evaluator,metric_evaluator,job_log,result_log,act_log,excl_map_dict,deprecated_var_list,var_config,spark):
        # input:
        self.base_driver = algo_input.base_driver
        self.activity_table = algo_input.activity_table
        self.prefix = algo_input.prefix
        self.key_col = algo_input.key_col
        self.acct_col = algo_input.acct_col
        self.tag_col = algo_input.tag_col
        self.amt_col = algo_input.amt_col
        self.gloss_col = algo_input.gloss_col
        self.nloss_col = algo_input.nloss_col
        # algo param:
        self.ftr_config_file = algo_param.ftr_config_file
        self.trunc_cutoff = algo_param.trunc_cutoff
        self.trunc_cp_size = algo_param.trunc_cp_size
        self.eval_metric = algo_param.eval_metric
        self.min_support_cutoff = algo_param.min_support
        self.min_block_size = algo_param.min_block_size
        self.eval_cutoff = algo_param.eval_cutoff
        self.rank_priority = algo_param.rank_priority
        self.dedup_cutoff = algo_param.dedup_cutoff
        self.gini_cutoff = algo_param.gini_cutoff
        self.lvl_limit = algo_param.lvl_limit
        self.overlap = algo_param.overlap
        
        self.spark = spark
        
        ## pre-stop:
        self.max_combination = 300
        # algo output:
        self.bq_db_name = algo_output.bq_db_name
        self.tag_table = algo_output.tag_table_name
        self.retention_day = algo_output.retention_day

        self.pattr_evaluator = pattr_evaluator
        self.metric_evaluator = metric_evaluator
        
        self.job_log = job_log
        self.result_log = result_log
        self.act_log = act_log

        self.excl_map_dict = excl_map_dict
        self.deprecated_var_list = deprecated_var_list
        self.var_config = var_config

        if self.eval_metric not in ['target_amt','target_cnt']:
            raise Exception('eval_metric setting is wrong! Please choose "target_cnt" or "target_amt"')
        elif self.eval_metric == 'target_amt' and self.amt_col == None:
            raise Exception('Please set amt_col if you set eval_metric as "target_amt"')
    
    def preload_size_cal(self):
        """docstring
        calculate the size of activity data only for target population
        Args: None
        Return: None

        """
        query  ='''
        select count(1) as size from activity_view
        where {tag} = 1
        '''.format(tag=self.tag_col)
        df_size = self.spark.sql(query).toPandas()
        df_size = df_size.fillna(0)
        activity_size = df_size['size'].head(1).iloc[0]
        if activity_size > self.trunc_cutoff:
            logger.info("the activity table size is %d, exceeding the maximum truncat cutoff %d, we will only keep %d activity for each checkpoint!"%(activity_size, self.trunc_cutoff, self.trunc_cp_size))
            return False
        else:
            logger.info("the activity table size is %d"%(activity_size))
            return True
        
    def load_dataset(self):
        """docstring
        1. load activity data only for target population
        2. tranform data type of variables in activity df based on ftr configuration
        3. generate mapping dict for ftr_type,ftr_by_cp,ftr_priority,cp_priority
        Require: Pandas verion: 
        Args: None
        Return: 
        - df_activity(dataframe): activity data only for target population
        - ftr_type(dict): {var_name: var_type}
        - ftr_by_cp(dict): {cp:[var1,var2,var3]}
        - ftr_priority(dict): {var_name: priority}
        - cp_priority(dict): {cp:priority}

        """
        type_convert = {
        'string':str
        ,'numeric': float
        ,'bool':bool
        }
        size_check =  self.preload_size_cal()
        if size_check:
            query = '''
            select * from activity_view
            where {tag} = 1
            '''.format(tag=self.tag_col)
        else: 
            query = '''
            WITH ranked_activities AS (
            SELECT *,
                   row_number() over (partition by {key_col}, checkpoint order by timestamp desc) as rank_
            FROM activity_view
            WHERE {tag} = 1
                )

            SELECT * FROM ranked_activities
                WHERE rank_ <= {limit_num}
                LIMIT {trunc_size}
            '''.format(tag=self.tag_col,key_col=self.key_col,limit_num=str(int(self.trunc_cp_size)),trunc_size=str(int(self.trunc_cutoff)))

    #         print(query)
        try:
            df_activity = self.spark.sql(query).toPandas()
        except Exception as err:
            logger.error("error message: %s"%str(err))
            raise Exception("Error in executing query: {query}".format(query=query))

        null_cols = df_activity.columns[df_activity.isna().all()].tolist()
        df_activity = df_activity.drop(columns=null_cols)
        df_ftr = pd.read_csv(self.ftr_config_file)
        df_ftr['key_type'] = df_ftr['key_type'].str.split('/')
        df_ftr['key_value'] = df_ftr['key_value'].str.split('/')
        df_ftr = df_ftr.explode('key_type')
        df_ftr = df_ftr.explode('key_value')
        if self.search_scope == 'dual':
            key_type_map = {'sender': '_send','receiver':'_recv'}
            key_val_map = {'sender': '@sndr','receiver':'@rcvr'}
            df_ftr['checkpoint'] = df_ftr[['checkpoint','key_type','key_value']].apply(lambda x: x.checkpoint+key_type_map[x.key_type]+key_val_map[x.key_value],axis=1)
        df_ftr = df_ftr[~df_ftr['variable name'].isin(null_cols)]
        df_ftr['format_type'] = df_ftr['type'].apply(lambda x: type_convert[x])
        if self.search_scope == 'single':
            DISP_data = df_ftr[df_ftr['checkpoint'].str.contains('DisputeLifecycleAdjudication|DisputeLifecycleInitiation')]
            DISP_send = copy.deepcopy(DISP_data)
            DISP_rcve = copy.deepcopy(DISP_data)
            DISP_send['checkpoint'] = DISP_data['checkpoint']+'_send'
            DISP_rcve['checkpoint'] = DISP_data['checkpoint']+'_recv'
            df_ftr=pd.concat([df_ftr,DISP_send,DISP_rcve],axis = 0)
        ftr_type = dict(zip(df_ftr['variable name'],df_ftr['format_type']))
        ftr_priority = {k : g.priority.to_dict() for k, g in df_ftr.set_index('variable name').groupby('checkpoint')}
        ftr_by_cp = dict(df_ftr.groupby('checkpoint')['variable name'].unique())
        # df_cp = pd.read_csv(cp_config_file)
        cp_priority = dict(zip(df_ftr['checkpoint'],df_ftr['cp_priority']))
        # dtype_dict= copy.deepcopy(ftr_type)
        dtype_dict= {k:v for k,v in ftr_type.items() if v != bool}
        dtype_dict[self.key_col]=str

        if len(df_activity)>0:
            df_activity = df_activity.astype(dtype_dict)


        return df_activity,ftr_type,ftr_by_cp,ftr_priority,cp_priority
        
    
    def cal_baseline(self):
        """docstring
        calculate global metrics that will be used in many functions
        Args: None
        Return: 
        - self.N: count of total target population
        - baseline: % of target population in driverset
        - Sup_N: the minimal count of target population based on coverage requirement
        - min_block_size: the minimal count of target population within a block 
        
        """
        self.N,self.baseline = self.metric_evaluator.baseline_compute()
        self.Sup_N= self.min_support_cutoff*self.N
        if self.min_block_size < 1:
            self.min_block_size = max(self.min_block_size*self.N,1)

    
    def gini_cal(self,df,cp,var):
        if self.eval_metric == 'target_amt':
            df_acct = df[df['checkpoint']==cp].groupby([var,self.key_col])[self.amt_col].max().reset_index([0,1])
            df_distr = df_acct.groupby(var)[self.amt_col].sum().sort_values(ascending=False).reset_index(0).rename(columns = {self.amt_col:self.key_col})
        else:        
            df_distr = df[df['checkpoint']==cp].groupby(var)[self.key_col].nunique().sort_values(ascending=False).reset_index(0)
        df_distr['cumsum'] = df_distr[self.key_col].cumsum()
        bad_gini = abs(df_distr['cumsum'].sum()/(len(df_distr)*df_distr[self.key_col].sum()/2)-1)
        return bad_gini
        
    def str_var_compute(self,cp,var_name,val_list):
        """docstring
        1. given the checkpoint, variable and value list, create a filter condition of activity data
        2. calculate the target population catch and % of target population with the filter condition
        eg: cp="AddACH", var_name = "Routing_Number" val_list = [1,2,3,4], filter condition = 'and checkpoint = "AddACH" and Routing_Number in ("1","2","3","4")'
        Args: 
         - cp(str): checkpoint name
         - var_name(str): variable name
         - val_list(List):value list
        Return:
         - target_catch: # of target population captured by the filter condition
         - target_rate: % of target population of the total population with filter condition
        
        """
        val_list_str = ','.join(['\"'+val+'\"'for val in val_list])
        filter_cond = 'and checkpoint = \"{cp}\" and {var_name} in ({var_val})'.format(cp=cp,var_name=var_name,var_val=val_list_str)
        target_catch,target_rate = self.metric_evaluator.metric_compute(filter_cond)
        return target_catch,target_rate
    
    def str_var_compute_sql(self,cp,var_name,val_list,sql_key):
        """docstring
        1. given the checkpoint, variable and value list, create a filter condition of activity data
        2. return the sql to calculate the target population catch and % of target population with the filter condition
        eg: cp="AddACH", var_name = "Routing_Number" val_list = [1,2,3,4], filter condition = 'and checkpoint = "AddACH" and Routing_Number in ("1","2","3","4")'
        Args: 
         - cp(str): checkpoint name
         - var_name(str): variable name
         - val_list(List of str):value list
        Return:
         - sql(str): sql to calculate metrics (target_catch,target_rate)
        
        """
        
        val_list_str = ','.join(['\"'+val+'\"'for val in val_list])
        filter_cond = 'and checkpoint = \"{cp}\" and {var_name} in ({var_val})'.format(cp=cp,var_name=var_name,var_val=val_list_str)
        sql = self.metric_evaluator.metric_compute_sql(filter_cond,sql_key)
        return sql
    
    def str_bin_update(self,str_bins,new_val,target_rate,target_catch,cp,var_name):
        """docstring
        refresh bin group for categorical variable
        Args: 
         - str_bins(List of tuple): old bin group list, eg [('A','B'),('C')]
         - new_val(str): new value that need to decide which bin group to join
         - target_rate(float):the measure metric for new_val
         - target_catch(float): the measure metric for new_val
         - cp(str): checkpoint name
         - var_name(str): variable name
        Return:
         - str_bins(List of tuple): new bin group list, eg [('A','B'),('C','D')]
        
        """
        
        # print(str_bins)
        close_bin = min(str_bins, key = lambda t: abs(t[2]-target_rate))
        bin_idx = str_bins.index(close_bin)
        if abs(close_bin[2]-target_rate)/close_bin[2]<0.5 or (target_rate>self.eval_cutoff) or (close_bin[2]<0.01 and target_rate<close_bin[2]):
            new_list = close_bin[0]+[new_val]
            target_catch,target_rate = self.str_var_compute(cp,var_name,new_list)
            str_bins[bin_idx]=(new_list,target_catch,target_rate)
               
        else:
            str_bins.append(([new_val],target_catch,target_rate))
        return str_bins
    
    def str_var_merge(self,df,cp,var,priority):
        # print(cp,var)
        wh_bins = self.var_config['string']['ignore_values']

        # wh_bins = ['NO_MISMATCH','BEACON_MISSING_ELEMENT','NORMAL','NON_GIB','NON_BOGUS','CITY_MATCH','VALID_PHONE',
        #            'SHIP_NAME_MATCH_BILL_NAME','FULL_MATCH','SHIP_NAME_MATCH_ACCOUNT_NAME','STATE_MATCH',
        #            'REASONABLE_HOPPING','NO_HOPPING','OTHER','COUNTRY_MATCH','MATCH_COUNTRY','MATCH_CITY','MATCH_STATE','NO_DATA','CONS_TIMEZONE','NO_RCVR_ID','06_normal',
        #            'NO_FRAUDNET_DATA','REGION_MATCH','NEUTRAL','FULL_CTRY_MATCH','NULL','NO_PHONE_INFO','NO_RADD_DATA',
        #            'FULL_NAME_MATCH','GOOD','RCVR_IS_NOT_PEEKED','CONS_COUNTER_PARTY','YOUNG_RCVR','NEUTRAL',
        #            'NO_FRAUDNET_OR_MAGNES','0','NEUTRAL_PROB_COLLUSION','NO_BOT_RISK','NO_FRAUDNET_DATA',
        #            'MOBILE_FRAUDNET','NO_NEW_CTRY_MATCH_MULT_CTRY_ACCT','NO_NEW_CTRY_MATCH_SINGLE_CTRY_ACCT'] ## define whitelist bins
        if self.eval_metric == 'target_amt':
            df_acct = df[(df['checkpoint']==cp)&(~df[var].isin(wh_bins))].groupby([var,self.key_col]).agg({self.acct_col:'first',self.amt_col:'first'}).reset_index([0,1])
            df_distr = df_acct.groupby(var)[self.amt_col].sum().sort_values(ascending=False).head(10)
            df_distr = df_acct.groupby(var).agg({self.amt_col: 'sum', self.acct_col: 'nunique'}).sort_values(self.amt_col,ascending=False).head(10)
        else:
            df_distr = df[(df['checkpoint']==cp)&(~df[var].isin(wh_bins))].groupby(var).agg({self.key_col:'nunique',self.acct_col:'nunique'}).sort_values(self.key_col,ascending=False).head(10)
        
        ### check exclude seg
        if self.excl_map_dict:
            if (cp,var) in self.excl_map_dict.keys():
                excl_value_list = '|'.join(self.excl_map_dict[(cp,var)])
                df_distr = df_distr.reset_index(0)
                df_distr = df_distr[~df_distr[var].str.contains(excl_value_list)].set_index(var)
        if len(df_distr)>0:
            cat_bins = []
            batch_sqls = [self.str_var_compute_sql(cp,var,[cat.Index.replace('\r','').replace('\n','').replace('\t','')],'sql_'+str(idx)) for idx,cat in enumerate(df_distr.itertuples())]
            batch_res = self.metric_evaluator.metric_compute_batch(batch_sqls)
            for idx,item in enumerate(df_distr.itertuples()):
                # cat = cat.encode("ascii", errors="ignore").decode()
                cat = item.Index.replace('\r','').replace('\n','').replace('\t','')
                if cat not in ['NULL','UNKNOWN','None'] and getattr(item, self.acct_col)>1:
                    # target_catch,target_rate = self.str_var_compute(cp,var,[cat])
                    target_catch = batch_res['sql_'+str(idx)]['target_catch']
                    target_rate = batch_res['sql_'+str(idx)]['target_rate']
                    if target_rate > self.baseline and target_catch> self.min_block_size:
                        if len(cat_bins)>0:
                            cat_bins = self.str_bin_update(cat_bins,cat,target_rate,target_catch,cp,var)
                        else:
                            cat_bins.append(([cat],target_catch,target_rate))
            output_bins = []
            for cat in cat_bins:
                val_format = '{var} in ({val_list})'.format(var=var,val_list=','.join([ '\"'+val+'\"' for val in cat[0]]))
                output_bins.append((val_format,cat[1],cat[2],priority))
            output_bins = list(filter(lambda tup: tup[1]>=self.Sup_N ,output_bins))
        else:
            output_bins = []
        return output_bins


    @staticmethod
    def divide_bin(df_des,var):
        """docstring
        get the bin list for a numeric variable based on quantile segment
        Args: 
         - df_des(dataframe): the quantile description of var
         - var(str): variable name 
        Return:
         - cut_bins_list(List of float): [0,100,200,300,400]
        
        """
        cut_bins = []
        for des in df_des.index:
            if '%' in des:
                if df_des[des]<10:
                    cut_bins.append(round(df_des[des],1))
                elif df_des[des]>=10 and df_des[des]<100:
                    cut_bins.append(int(df_des[des]/10)*10)
                elif df_des[des]>=100 and df_des[des]<10000:
                    cut_bins.append(int(df_des[des]/100)*100)
                elif df_des[des]>=10000:
                    cut_bins.append(int(df_des[des]/10000)*10000)
        cut_bins_list = list(set(cut_bins))
        cut_bins_list.sort()
        return cut_bins_list

    @staticmethod
    def num_bin2str(var,l):
        """docstring
        translate the bin of numeric variable into sql condition logic
        Args: 
         - var(str): variable name 
         - l(tuple): value range
        Return:
         - var_syntax(str): condition logic based on value range
         eg: var = "var1", l = (100, 200) -> var_syntax= "var1<100 and var1<=200"
        
        """
        if l[0]== -np.inf:
            var_syntax = var+'<='+str(l[1])
        elif l[1]== np.inf:
            var_syntax = var+'>'+str(l[0])
        else:
            var_syntax = var+'>'+str(l[0])+' and '+var+'<='+str(l[1])
        return var_syntax   

    def num_var_compute(self,cp,var,l):
        """docstring
        1. given the checkpoint, variable and value list, create a filter condition of activity data
        2. calculate the target population catch and % of target population with the filter condition
        eg: cp="AddACH", var_name = "arm_score" val_list = (-np.inf,100), filter condition = 'and checkpoint = "AddACH" and arm_score<=100'
        Args: 
         - cp(str): checkpoint name
         - var_name(str): variable name
         - l(tuple):value range
        Return:
         - target_catch: # of target population captured by the filter condition
         - target_rate: % of target population of the total population with filter condition
        
        """
        var_syntax = self.num_bin2str(var,l)
        filter_cond = 'and checkpoint = '+'\"'+cp+'\"'+' and '+var_syntax
        target_catch,target_rate = self.metric_evaluator.metric_compute(filter_cond)
        return target_catch,target_rate

    def num_var_compute_sql(self,cp,var,l,sql_key):
        """docstring
        1. given the checkpoint, variable and value list, create a filter condition of activity data
        2. get the sql logic to calculate the target population catch and % of target population with the filter condition
        eg: cp="AddACH", var_name = "arm_score" val_list = (-np.inf,100), filter condition = 'and checkpoint = "AddACH" and arm_score<=100'
        Args: 
         - cp(str): checkpoint name
         - var_name(str): variable name
         - l(tuple):value range
        Return:
         - sql: sql logic to calculate target_catch, target_rate
        
        """
        var_syntax = self.num_bin2str(var,l)
        filter_cond = 'and checkpoint = '+'\"'+cp+'\"'+' and '+var_syntax
        sql = self.metric_evaluator.metric_compute_sql(filter_cond,sql_key)
        return sql

    def num_var_merge(self,df,cp,var,priority):
        """docstring
        get the qualified condition list for a numeric variable
        Args: 
         - df(dataframe): activity data only for target population
         - cp(str): checkpoint name
         - var(str): variable name
         - priority(int): the priority of variable 
        Return:
         - num_syntax_list(List of tuple): qualified condition list,tuple[0]: condition logic, tuple[1]:target_catch,tuple[2]:target_rate: eg [("var1>100 and var1<=200",100,0.2)]
        
        """
        df_des = df[(df[self.tag_col]==1)&(df['checkpoint']==cp)][var].describe(np.arange(self.min_support_cutoff,1,self.min_support_cutoff))
        df_des = df_des[df_des>0]
        seg_bins = []
        if self.var_config['number']['fix_bins_setting']:
            for fix_bin,seg in self.var_config['number']['fix_bins_setting'].items():
                if any(re.findall(r'{vars}'.format(vars=fix_bin), var.lower(), re.IGNORECASE)):
                    seg_bins = seg
        if len(seg_bins)>0:
            cut_bins = []
            for idx,val in enumerate(seg_bins):
                if val>=df_des['max'] or idx>=len(seg_bins)-1:
                    break
                else:
                    cut_bins.append(val)
            cut_bins.append(seg_bins[idx])

        else:
            cut_bins = self.divide_bin(df_des,var)
        
        candidate_list = []
        if len(cut_bins)>1:
            if any(re.findall(r'{vars}'.format(vars=self.var_config['number']['direction_setting']['up']), var.lower(), re.IGNORECASE)): ## for score/amount use > 
                batch_sqls = [self.num_var_compute_sql(cp,var,(val,np.inf),'sql_'+str(idx)) for idx,val in enumerate(cut_bins)]
                batch_res = self.metric_evaluator.metric_compute_batch(batch_sqls)
                best_option = {'val':0,'target_rate':0,'target_catch':0}
                for idx,val in enumerate(cut_bins):
                    target_catch = batch_res['sql_'+str(idx)]['target_catch']
                    target_rate = batch_res['sql_'+str(idx)]['target_rate']
                    # target_catch,target_rate = self.num_var_compute(cp,var,(val,np.inf))
                    # print(best_option)
                    # print(val,target_catch,target_rate)
                    if target_rate>self.eval_cutoff:
                        best_option['val']=val
                        best_option['target_rate']=target_rate
                        best_option['target_catch']=target_catch
                        break
                    elif target_rate > best_option['target_rate'] and target_catch>self.Sup_N:
                        best_option['val']=val
                        best_option['target_rate']=target_rate
                        best_option['target_catch']=target_catch
                if best_option['target_rate']>self.baseline and best_option['target_catch']>self.Sup_N:
                    candidate_list.append(((best_option['val'],np.inf),best_option['target_catch'],best_option['target_rate']))    
                    
            elif any(re.findall(r'{vars}'.format(vars=self.var_config['number']['direction_setting']['down']), var.lower(), re.IGNORECASE)): ## for days use <
                batch_sqls = [self.num_var_compute_sql(cp,var,(-np.inf,val),'sql_'+str(idx)) for idx,val in enumerate(cut_bins)]
                batch_res = self.metric_evaluator.metric_compute_batch(batch_sqls)
                best_option = {'val':0,'target_rate':0,'target_catch':0}
                for idx,val in enumerate(cut_bins):
                    target_catch = batch_res['sql_'+str(idx)]['target_catch']
                    target_rate = batch_res['sql_'+str(idx)]['target_rate']
                    # target_catch,target_rate = self.num_var_compute(cp,var,(val,np.inf))
                    # print(best_option)
                    # print(val,target_catch,target_rate)
                    if target_rate > min(best_option['target_rate'],self.eval_cutoff) and target_catch>self.Sup_N:
                        best_option['val']=val
                        best_option['target_rate']=target_rate
                        best_option['target_catch']=target_catch
                if best_option['target_rate']>self.baseline and best_option['target_catch']>self.Sup_N:
                    candidate_list.append(((-np.inf,best_option['val']),best_option['target_catch'],best_option['target_rate']))             
            else:
                for idx,val in enumerate(cut_bins):  
        #             print(candidate_list)
                    if idx ==0:
                        target_catch,target_rate = self.num_var_compute(cp,var,[-np.inf,val])
                        if target_rate>self.baseline:
                            candidate_list.append(((-np.inf,cut_bins[idx+1]),target_catch,target_rate))         
                    elif idx<len(cut_bins)-1:
                        target_catch,target_rate = self.num_var_compute(cp,var,[val,cut_bins[idx+1]])
                        if target_rate>=self.baseline:
                            if len(candidate_list)>0:
                                last_candidate = candidate_list[-1]
                                if (last_candidate[0][1] == val) and (abs(last_candidate[2]-target_rate)/last_candidate[2]<0.5 or (target_rate>self.eval_cutoff) or (last_candidate[2]<0.01 and target_rate<last_candidate[2])):
                                    upd_candidate = candidate_list.pop(-1)
                                    val_range = (upd_candidate[0][0],cut_bins[idx+1])
                                    target_catch,target_rate = self.num_var_compute(cp,var,val_range)

                                    candidate_list.append((val_range,target_catch,target_rate))
                                else:
                                    candidate_list.append(((val,cut_bins[idx+1]),target_catch,target_rate))
                            else:
                                candidate_list.append(((val,cut_bins[idx+1]),target_catch,target_rate))

                    elif idx==len(cut_bins)-1:
                        target_catch,target_rate = self.num_var_compute(cp,var,(val,np.inf))
                        if target_rate>=self.baseline:
                            candidate_list.append(((val,np.inf),target_catch,target_rate))
                
            if len(candidate_list)>0:
                num_syntax_list = [ (self.num_bin2str(var,subset[0]),subset[1],subset[2],priority) for subset in candidate_list if subset[1]>=self.Sup_N and subset[2]>=self.baseline]
                return num_syntax_list
            else:
                return []
        else:
            return []
    

    def bool_var_process(self,df,cp,var,priority):
        """docstring
        get the qualified condition list for a bool variable
        Args: 
         - df(dataframe): activity data only for target population
         - cp(str): checkpoint name
         - var(str): variable name
         - priority(int): the priority of variable 
        Return:
         - bool_val_list(List of tuple): qualified condition list,tuple[0]: condition logic, tuple[1]:target_catch,tuple[2]:target_rate: eg [("var1=True",100,0.2)]
        
        """
        if self.excl_map_dict and (cp,var) in self.excl_map_dict.keys():
            return []
        else:
            if var in self.var_config['bool']['false_vars']:
                var_syntax = var+'='+'False'
            else:
                var_syntax = var+'='+'True'
            query = 'and checkpoint ='+'\"'+cp+'\"'+' and '+var_syntax
            target_catch,target_rate = self.metric_evaluator.metric_compute(query)
            if target_catch>= self.Sup_N and target_rate>=self.baseline:
                bool_val_list=[(var_syntax,target_catch,target_rate,priority)]
                return bool_val_list
            else:
                return []     
    
    def ftr_selection(self,cp,var,ftr_type,priority,df):
        """docstring
        get the qualified condition list for a var based on different var type
        Args: 
         - df(dataframe): activity data only for target population
         - cp(str): checkpoint name
         - var(str): variable name
         - ftr_type(dict): {var_name:var_type}
         - priority(int): the priority of variable 
        Return:
         - val_list(List of tuple): qualified condition list,tuple[0]: condition logic, tuple[1]:target_catch,tuple[2]:target_rate: eg [("var1=True",100,0.2)]
        
        """
        if ftr_type[var]== str:
            val_list = self.str_var_merge(df,cp,var,priority) #### String Variable Process
        elif ftr_type[var]== float:
            val_list = self.num_var_merge(df,cp,var,priority) #### Numeric Variable Process
        elif ftr_type[var]== bool:
            val_list = self.bool_var_process(df,cp,var,priority) #### Bool Variable Process
            
        return val_list
    
    def search_ftr_by_cp(self,ftr_config,ftr_type,ftr_priority,df):
        """docstring
        get the qualified condition list for all the variables in a checkpoint
        Args: 
         - df(dataframe): activity data only for target population
         - ftr_config(dict): mapping dict for variable list of checkpoint {cp: [var1,var2,var3]}
         - ftr_type(dict): {var_name: var_type}
         - ftr_tftr_priorityype(dict): {var_name:priority}
        Return:
         - qualify_ftr_dict(Dict of List): qualified condition list for each checkpoint, tuple[0]: condition logic, tuple[1]:target_catch,tuple[2]:target_rate: eg {cp:[("var1=True",100,0.2)]}
        
        """
        qualify_ftr_dict = {}
        batch_sqls = [self.metric_evaluator.metric_compute_sql('and checkpoint ='+'\"'+key+'\"',key) for key in ftr_config.keys()]
        batch_res = self.metric_evaluator.metric_compute_batch(batch_sqls)
        for key in ftr_config.keys():
            # target_catch,target_rate = self.metric_evaluator.metric_compute('and checkpoint ='+'\"'+key+'\"')
            target_catch = batch_res[key]['target_catch']
            target_rate = batch_res[key]['target_rate']
            if target_catch>=self.Sup_N:
                qualify_ftr_dict[key]={}
                result_list = Parallel(n_jobs=-1, backend = 'threading')(delayed(self.ftr_selection)
                                    (key,var,ftr_type,ftr_priority[key][var],df)  for var in  ftr_config[key] if self.gini_cal(df,key,var)>=self.gini_cutoff)
                if len(result_list)>0:
                    qualify_ftr_dict[key] = reduce(lambda a, b: a+b, result_list)
                else:
                    print("key is %s"%(key))
                    print(ftr_config[key])
                    qualify_ftr_dict[key] = []


        return qualify_ftr_dict
    
    def best_parent(self,parent_list,condition,priority):
        new_rank_list = []
        batch_sqls = [self.metric_evaluator.metric_compute_sql('and '+parent.data[0]+ftr_delim+condition,'sql_'+str(idx)) for idx,parent in enumerate(parent_list)]
        batch_res = self.metric_evaluator.metric_compute_batch(batch_sqls)
        for idx,parent in enumerate(parent_list):
            # merge_condition = parent.data[0]+' and '+condition
            merge_condition = parent.data[0]+ftr_delim+condition
            # target_catch,target_rate = self.metric_evaluator.metric_compute('and '+merge_condition)
            target_catch = batch_res['sql_'+str(idx)]['target_catch']
            target_rate = batch_res['sql_'+str(idx)]['target_rate']
            if target_catch and target_rate:
                pattr_fscore = fscore(target_rate,target_catch/self.N)
            else:
                pattr_fscore = 0
                target_catch = 0
                target_rate = 0
                # print(merge_condition,target_catch,target_rate)
            new_rank_list.append((parent,target_rate,target_catch,pattr_fscore))
        overlap_list = list(filter(lambda tup: tup[2]>0.8*tup[0].data[2] and tup[1]>tup[0].data[1] ,new_rank_list))
        if len(overlap_list)>0:
            overlap_parent = max(overlap_list, key = lambda tup: tup[3])
            target_rate_inc =overlap_parent[1]-overlap_parent[0].data[1]
            if priority == 1:
                return overlap_parent,'Add'
            elif priority ==2 and target_rate_inc>0.1:
                return overlap_parent,'Add'
            elif priority ==3 and target_rate_inc>0.15:
                return overlap_parent,'Add'
            else:
                return None,None
        else:
            qualify_list = list(filter(lambda tup: tup[2]>=self.Sup_N and tup[1]>tup[0].data[1] ,new_rank_list))### tuple (parent,bad_rate,loss)
            if len(qualify_list)>0:
                best_parent = max(qualify_list, key = lambda tup: tup[3])
                target_rate_inc = best_parent[1]-best_parent[0].data[1]
                if priority==1:
                    return best_parent,'Add'
                elif priority ==2 and target_rate_inc>0.05:
                    return best_parent,'Add'
                elif priority ==3 and target_rate_inc>0.1:
                    return best_parent,'Add'
                else:
                    return None,None
            else:
                return None,None  
    

    def gen_ftr_tree_by_cp(self,qualify_ftr_dict,cp):
        ftr_tree = Tree()
        target_catch,target_rate = self.metric_evaluator.metric_compute('and checkpoint=\"'+cp+'\"')
        pattr_fscore = fscore(target_rate,target_catch/self.N)
        ftr_tree.create_node(tag='checkpoint=\"'+cp+'\"', identifier='checkpoint=\"'+cp+'\"', data=['checkpoint=\"'+cp+'\"', target_rate,target_catch,pattr_fscore])
        parent = ftr_tree.get_node('checkpoint=\"'+cp+'\"')
        feature_list = sorted(qualify_ftr_dict[cp],key=lambda tup: (-tup[3],tup[1]),reverse = True)
        path = [parent]
        idx = 0
        for val_tuple in feature_list:
#             print("%d round"%idx)
#             print("feature %s"%val_tuple[0])
#             ftr_tree.show()
            condition = val_tuple[0]
#             print(path)
            parent,flag =self.best_parent(path,val_tuple[0],val_tuple[3])
#             print(parent)
            if flag:
                # merge_condition = parent[0].data[0]+' and '+condition 
                merge_condition = parent[0].data[0]+ftr_delim+condition
                ftr_tree.create_node(tag=condition, identifier=condition,parent=parent[0], data = [merge_condition,parent[1],parent[2],parent[3]])
                path.append(ftr_tree.get_node(condition))

            idx+=1
        return {cp:ftr_tree}
    
    def gen_ftr_tree_dict(self,qualify_ftr_dict):
        result_list = Parallel(n_jobs=-1, backend = 'threading')(delayed(self.gen_ftr_tree_by_cp)
                                        (qualify_ftr_dict,key)  for key in  qualify_ftr_dict.keys())
        if len(result_list)>0:
            cp_tree_dict = reduce(lambda a, b: dict(a, **b), result_list)
        else:
            cp_tree_dict = {}
        return cp_tree_dict

    
    @staticmethod
    def get_candidates(cp_tree_dict):
        #### Initialize
        pattern_list = []
        for key in cp_tree_dict.keys():
            if cp_tree_dict[key].depth()>0:
                paths = cp_tree_dict[key].paths_to_leaves()
                pattern_list +=list(zip(paths,[key]*len(paths)))

        ### get the path of each leaf from each tree
        candidates = []
        for pattern in pattern_list:
            if pattern[0]:
                candiate_node = pattern[0][-1]
                candidate_pattern = cp_tree_dict[pattern[1]].get_node(candiate_node).data[0]
                candidates.append(candidate_pattern)
        return candidates
    
    def get_key_pattr_df(self,pattern_list,df):
        '''
        generate dataframe with columns: key_col, pattern(list object), weight
        '''
        df_key_pattern = pd.DataFrame()
        for pattern in pattern_list:
            try:
                pattr_qry = pattern.replace('checkpoint=','checkpoint==').replace('=False','==False').replace('=True','==True').replace(ftr_delim,' and ')
                if self.eval_metric == 'target_cnt':
                    df_key = df.query(pattr_qry).groupby([self.key_col,self.acct_col]).apply(lambda x: pd.Series({
                                'weight': 1
                                })).reset_index([0,1])
                elif self.eval_metric == 'target_amt':
                    df_key = df.query(pattr_qry).groupby([self.key_col,self.acct_col]).apply(lambda x: pd.Series({
                    'weight': x[self.amt_col].max()
                    })).reset_index([0,1])
                df_key['pattern'] = pattern
                df_key_pattern = pd.concat([df_key_pattern,df_key],axis = 0)
            except:
                print(pattern)
        pattern_by_key = df_key_pattern.groupby([self.key_col,self.acct_col]).apply(lambda x: pd.Series({
                        'pattern': x['pattern'].unique()
                        ,'weight': max(x['weight'].max(),0.11) ## to avoid weight = 0 for a sample
                    })).reset_index([0,1])
        return pattern_by_key
    
    @staticmethod
    def merge_item_list(origin_list,wgt_dict,delim=ftr_delim,max_cnt=3):
        cp_list = list(set([item.split(ftr_delim)[0] for item in origin_list]))
        filter_list = []
        for cp in cp_list:
            item_list = [item for item in origin_list if cp in item]
            item_list.sort(key=lambda x: wgt_dict[cp][x], reverse=True)
            filter_list+=item_list[:max_cnt]
        return filter_list
    
    @staticmethod
    def pattr_frequency(pattern_by_key):
        pattern_exploded = pattern_by_key.explode('pattern')
        pattern_exploded['cp'] = pattern_exploded['pattern'].apply(lambda x: x.split(ftr_delim)[0])
        pattern_wgt = pattern_exploded.groupby(['cp','pattern']).agg({'weight': 'sum'}).reset_index([0,1])
        pattern_wgt_dict = pattern_wgt.groupby('cp').apply(lambda x: x.set_index('pattern')['weight'].to_dict()).to_dict()
        return pattern_wgt_dict
    
    def gen_graph(self,pattr_list,df,wgt_dict):
        """"docstring
        this function is to generate graph based on pattr_list and activity data only for target population
        Args:
         - pattr_list(List): pattern list eg:[pattr1,pattr2,pattr3]
         - df(DataFrame): dataframe of activity data for target population
        Return:
         - G: graph of patterns node: pattern, edge: if the number of accounts shared pattr1 and pattr2 > coverage requirement, weight = # of pattr1 & pattr2 / # of pattr1|pattr2
        """
        G = nx.Graph()
        for idx,pattern in enumerate(pattr_list):
            pattr_qry = pattern.replace('checkpoint=','checkpoint==').replace('=False','==False').replace('=True','==True').replace(ftr_delim,' and ')
            key_list = list(df.query(pattr_qry)[self.key_col].unique())
            pattr_name = f'pattr_{str(idx)}'
            G.add_node(pattr_name,desc=pattern,size=sum([wgt_dict[k] for k in key_list]),details=key_list)

        all_nodes = list(G.nodes())
        pattr_pair = list(combinations(all_nodes, 2))   # get all pairs of two pattr

        for pair in pattr_pair:
            overlap = list(set(G.nodes[pair[0]]['details'])&set(G.nodes[pair[1]]['details']))
            union = list(set(G.nodes[pair[0]]['details'])|set(G.nodes[pair[1]]['details']))
            overlap_size = sum([wgt_dict[k] for k in overlap])
            union_size = sum([wgt_dict[k] for k in union])
            weight = round(overlap_size/union_size,2)
            if overlap_size>=self.Sup_N:
                G.add_edge(pair[0],pair[1] , weight=weight,details = overlap)
        return G
    

    def greedy_walk(self,G,start_node,cutoff,wgt_dict):
        num_steps = self.lvl_limit-1
        walk = [start_node]  # starting node
        intersect = G.nodes[start_node]['details'] ## intersect accts within the walk path
        cp = re.findall(r"checkpoint=\"(.+)\"", G.nodes[start_node]['desc'].split(ftr_delim)[0])[0] ## get the cp str
        walk_cp = [cp]
        for i in range(num_steps):
            neighbors = []
            for node in G.neighbors(start_node):
                size = sum([wgt_dict[k] for k in list(set(intersect)&set(G[start_node][node]['details']))])
                cp = re.findall(r"checkpoint=\"(.+)\"", G.nodes[node]['desc'].split(ftr_delim)[0])[0]
                if node not in walk and size>cutoff and cp not in walk_cp: # avoid choice repeat cp, pattern
                    neighbors.append((node,size))
            if len(neighbors)>0:
                # next_node = random.choices(neighbors, weights=neighbor_weights)[0]
                next_node = max(neighbors,key = lambda t: t[1])[0]
                overlap = list(set(intersect)&set(G[start_node][next_node]['details']))
                next_cp = re.findall(r"checkpoint=\"(.+)\"", G.nodes[next_node]['desc'].split(ftr_delim)[0])[0]
                walk.append(next_node)
                walk_cp.append(next_cp)
                start_node = next_node
                intersect = overlap

        pattr_pair = tuple(sorted([ G.nodes[node]['desc'] for node in walk]))
        pair_size = sum([wgt_dict[k] for k in intersect])

        return pattr_pair,pair_size
    
    def random_find_pattr(self,G,cutoff,wgt_dict):
        random.seed(42)
        connected_components = list(nx.connected_components(G))
        FreqPatternSet = {}
        for node in G.nodes():
            FreqPatternSet[(G.nodes[node]['desc'],)]=G.nodes[node]['size']
        for edge in G.edges():
            FreqPatternSet[(G.nodes[edge[0]]['desc'],G.nodes[edge[1]]['desc'])]=len(G[edge[0]][edge[1]]['details'])

        for com in connected_components:
            if len(com)>2:
                sub_G = G.subgraph(list(com))
                degree = list(sub_G.degree())
                seed_nodes = random.choices([t[0] for t in degree], weights=[t[1] for t in degree], k=min(self.max_combination,len(com)))
                for seed in seed_nodes:
                    k,v = self.greedy_walk(G,seed,cutoff,wgt_dict)
                    FreqPatternSet[k]=v
        return FreqPatternSet

    def random_find_pattr_v2(self,G,cutoff,wgt_dict):
        np.random.seed(42)
        connected_components = list(nx.connected_components(G))
        FreqPatternSet = {}
        # for node in G.nodes():
        #     FreqPatternSet[(G.nodes[node]['desc'],)]=G.nodes[node]['size']
        # for edge in G.edges():
        #     FreqPatternSet[(G.nodes[edge[0]]['desc'],G.nodes[edge[1]]['desc'])]=len(G[edge[0]][edge[1]]['details'])

        for com in connected_components:
            com = list(com)
            if len(com)==1:
                FreqPatternSet[(G.nodes[com[0]]['desc'],)]=G.nodes[com[0]]['size']
            elif len(com)==2:
                FreqPatternSet[(G.nodes[com[0]]['desc'],G.nodes[com[1]]['desc'])]=len(G[com[0]][com[1]]['details'])
            else: 
                sub_G = G.subgraph(com)
                degree = list(sub_G.degree())
                sum_wgt = sum([t[1] for t in degree])
                wgts=[t[1]/sum_wgt for t in degree]
                # seed_nodes = random.choices([t[0] for t in degree], weights=[t[1] for t in degree], k=min(algo.max_combination,len(com)))
                seed_nodes = np.random.choice([t[0] for t in degree],size=min(self.max_combination,len(connected_components[0])),p=wgts,replace=False ) # set replace=False
                for seed in seed_nodes:
                    k,v = self.greedy_walk(G,seed,cutoff,wgt_dict)
                    FreqPatternSet[k]=v
        return FreqPatternSet


    def get_itemsetlist(self,pattern_by_key,max_len=10):
        pattr_wgt_dict = self.pattr_frequency(pattern_by_key)
        itemSetList=[]
        merge_check = 0
        for idx,item in pattern_by_key.iterrows():
            if len(item.pattern)> max_len:
                merge_check+=1
                merge_item = self.merge_item_list(item.pattern,pattr_wgt_dict)
                itemSetList.append((merge_item,item.weight))
            else:
                itemSetList.append((list(item.pattern),item.weight))


        if merge_check>0:
            print("merge pattern for %d samples!"%merge_check)

        return itemSetList


    @staticmethod
    def count_dist_cp(pattr_list):
        '''
        get the distinct cp list of pattern, count distinct cp cnt within pattern
        '''
        cp_list = []
        for pattr in pattr_list:
            # cp = re.findall(r"checkpoint=\"(.+)\"", pattr.split('and')[0])[0]
            cp = re.findall(r"checkpoint=\"(.+)\"", pattr.split(ftr_delim)[0])[0]
            if cp not in cp_list:
                cp_list.append(cp)
        return len(cp_list),len(pattr_list),cp_list
    
    def filter_patternset(self,FreqPatternSet):
        '''
        exclude those pattern with duplicated cps. eg ['checkpoint=Login','checkpoint=Login'....]
        note: Login, RiskLiteLogin are the same activity category
        '''
        freqItemSetDict=[]
        for key in FreqPatternSet.keys():
            cp_cnt,pattr_cnt,cp_list = self.count_dist_cp(key)
            if cp_cnt == pattr_cnt and not (cp_cnt>1 and set(['Login','RiskLiteLogin']).issuperset(set(cp_list))):
                freqItemSetDict.append((list(key),FreqPatternSet[key],len(key)))
        return freqItemSetDict
    ######### pattern Evaluation ###########
    def get_qualified_pattr_list(self,freqItemSetDict,ftr_priority,cp_priority):
        if len(freqItemSetDict)>0:
            max_lvl = min(max(freqItemSetDict, key = lambda tup: tup[2])[2],self.lvl_limit)
            rm_sublists=[]
            pattern_comb_dict ={}
            for lvl in range(1,max_lvl+1):
                raw_pattr_list = [Pattern(tup[0]) for tup in filter(lambda tup: tup[2]==lvl,freqItemSetDict)]
                if len(raw_pattr_list)>0:
                    if lvl ==1:
                        eval_pattr_list = PatternsUtil.pattr_list_eval(raw_pattr_list,self.pattr_evaluator,cp_priority,ftr_priority)
                        qual_pattr_list = PatternsUtil.pattr_list_filter(eval_pattr_list,self.eval_cutoff)
                        if len(qual_pattr_list)>0:
                            for idx,pattr in enumerate(qual_pattr_list):
                                pattr.name = 'pattr_lvl'+str(lvl)+'_'+str(idx)
                                if pattr.cp_str in pattern_comb_dict.keys():
                                    pattern_comb_dict[pattr.cp_str].append(pattr)
                                else:
                                    pattern_comb_dict[pattr.cp_str]=[pattr]
                    else:
                        eval_pattr_list = PatternsUtil.pattr_list_eval(raw_pattr_list,self.pattr_evaluator,cp_priority,ftr_priority)
                        qual_pattr_list = PatternsUtil.pattr_list_filter(eval_pattr_list,self.eval_cutoff)
                        if len(qual_pattr_list)>0:
                            rm_sublists+=qual_pattr_list
                            freqItemSetDict = PatternsUtil.rm_superset(freqItemSetDict,rm_sublists)
                            for idx,pattr in enumerate(qual_pattr_list):
                                pattr.name = 'pattr_lvl'+str(lvl)+'_'+str(idx)
                                if pattr.cp_str in pattern_comb_dict.keys():
                                    pattern_comb_dict[pattr.cp_str].append(pattr)
                                else:
                                    pattern_comb_dict[pattr.cp_str]=[pattr]
            pattern_list =[]
            for key in pattern_comb_dict.keys():
                sel_pattern_list = PatternsUtil.rm_similar_pattrs(pattern_comb_dict[key])
                pattern_list+=sel_pattern_list
        else:
            pattern_list = []
            
        return pattern_list  
      
    def act_eval(self,content):
        target_catch,target_rate = self.metric_evaluator.metric_compute('and '+content)
        act = Pattern(content,target_rate=target_rate,target_catch=target_catch)
        return act

    @staticmethod
    def get_act_list(qual_pattr_list):
        act_list = list(set([item for pattr in qual_pattr_list for item in pattr.content]))

        return act_list

    def act_eval_parallel(self,act_list):
        batch_sqls = [(self.metric_evaluator.metric_compute_sql('and '+content,'sql_'+str(idx))) for idx,content in enumerate(act_list)]
        batch_res = self.metric_evaluator.metric_compute_batch(batch_sqls)
        act_eval_list = []
        for idx,content in  enumerate(act_list):
            target_rate = batch_res['sql_'+str(idx)]['target_rate']
            target_catch = batch_res['sql_'+str(idx)]['target_catch']
            act = Pattern(content,target_rate=target_rate,target_catch=target_catch)
            act_eval_list.append(act)
        return act_eval_list
    
    def log(self):
        create_dt = datetime.datetime.now()
        create_dt_str = create_dt.strftime('%Y-%m-%d')
        retire_dt = create_dt + datetime.timedelta(days = self.retention_day)
        retire_dt_str = retire_dt.strftime('%Y-%m-%d')

        content_map = {'pattr_table_name': self.tag_table, 'create_dt': create_dt_str, 'retire_dt': retire_dt_str}
        content_map_json = json.dumps(content_map)
        self.job_log.row_map['content'] = content_map_json

        store_log = StoreLog(self.job_log)
        store_log.store()
    ####### trigger function #######

    def search(self):
        pandas_gbq_numeric_type_patch()
        logger.info("FreqPatternSearch Step1: Start to load dataset")
        df_activity,ftr_type, ftr_by_cp,ftr_priority,cp_priority = self.load_dataset()
        if len(df_activity)>0:
            self.cal_baseline()

            logger.info("Total Uniq Count: {N}, Minimal Support Count: {Sup_N}".format(N=self.N,Sup_N=self.Sup_N))
            logger.info("FreqPatternSearch Step2: Start to build qualified feature dict")
            qualify_ftr_dict = self.search_ftr_by_cp(ftr_by_cp,ftr_type,ftr_priority,df_activity)
            # print(qualify_ftr_dict)
            
            logger.info("FreqPatternSearch Step3: Start to build feature tree by cp")
            cp_tree_dict = self.gen_ftr_tree_dict(qualify_ftr_dict)
            
            if len(cp_tree_dict)>0:
                logger.info("FreqPatternSearch Step4: Start to search pattern combination with weight")
                candidates = self.get_candidates(cp_tree_dict)
                pattern_by_key = self.get_key_pattr_df(candidates,df_activity)
                pattern_by_key['pattern_str'] = pattern_by_key['pattern'].apply(lambda x: cp_delim.join(x))
                wgt_dict = dict(zip(pattern_by_key[self.key_col],pattern_by_key['weight']))
                G = self.gen_graph(candidates,df_activity,wgt_dict)
                if len(G.edges())>self.max_combination: # 触发gready_walk
                    logger.info("Execute Greedy Walk")
                    FreqPatternSet = self.random_find_pattr_v2(G,self.Sup_N,wgt_dict)
                else:
                    logger.info("Execute FP-Growth")
                    itemSetList = self.get_itemsetlist(pattern_by_key)
                    FreqPatternSet = find_frequent_patterns(itemSetList,self.Sup_N,self.max_combination)

                freqItemSetDict = self.filter_patternset(FreqPatternSet)
                logger.info("count of combination: %d"%len(freqItemSetDict))
                logger.info("FreqPatternSearch Step5: Start to filter qualified pattern")
                if len(freqItemSetDict)>0:
                    qual_pattr_list = self.get_qualified_pattr_list(freqItemSetDict,ftr_priority,cp_priority)
                else:
                    qual_pattr_list = []
                logger.info("count of qualified patterns: %d"%len(qual_pattr_list))
                if len(qual_pattr_list)>0:
                    logger.info("FreqPatternSearch Step6: Generate pattern tagging table")
                    PatternsUtil.gen_pattr_tag_tab(qual_pattr_list,self.bq_db_name,self.key_col,self.base_driver
                            ,self.activity_table,self.prefix,self.tag_table,self.retention_day)
                    logger.info("FreqPatternSearch Step7: Dedup patterns")
                    dedup_pattr_list = PatternsUtil.dedup_pattrs(qual_pattr_list,self.bq_db_name,self.tag_table,self.tag_col
                                                                ,self.amt_col,self.eval_metric,self.N,self.dedup_cutoff,self.rank_priority)
                    self.log()
                    PatternsUtil.pattr_log(self.result_log,qual_pattr_list,dedup_pattr_list)
                    logger.info("FreqPatternSearch Step8: Store Activity Level Info")
                    act_content_list = self.get_act_list(qual_pattr_list)
                    act_list = self.act_eval_parallel(act_content_list)
                    PatternsUtil.act_log(self.act_log,act_list)
                    return True
                    
                else:
                    logger.error("Cannot find any qualified patterns")
                    return False
            else:
                    logger.error("Cannot find any qualified patterns")
                    return False
        else:
            logger.error("Cannot find any qualified patterns")
            return False        