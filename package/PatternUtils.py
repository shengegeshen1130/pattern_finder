import numpy as np 
import pandas as pd
import sys
from joblib import Parallel, delayed
import copy
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import pandas_gbq
import datetime
from loguru import logger
import itertools
import json
from util import fscore,query_bq,read_bq,ftr_delim,cp_delim
from store_log import StoreLog
from decimal import Decimal
import uuid

# Define a custom JSON encoder class
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)

# logger = logging.getLogger()
# FORMAT = '%(asctime)s - %(funcName)s[%(levelname)s]: %(message)s'
# console = logging.StreamHandler(stream = sys.__stderr__)
# console.setFormatter(logging.Formatter(FORMAT))
# console.setLevel(logging.INFO)
# logger.addHandler(console)
# logger.setLevel(logging.INFO)

class Pattern:
    def __init__(self,content,name=None,target_rate=None,target_catch=None,pattr_fscore=None,pattr_score=None, cp_str=None,pattr_metrics=None):
        self.name = name
        self.content = content
        self.target_rate = target_rate
        self.target_catch = target_catch
        self.pattr_fscore = pattr_fscore
        self.pattr_score = pattr_score
        self.cp_str = cp_str
        self.pattr_metrics = pattr_metrics
    
    def pattern_eval(self,pattr_evaluator,cp_priority,ftr_priority):
        pattr_metrics,target_catch,target_rate,pattr_fscore = pattr_evaluator.pattern_perf(self.content)
        pattr_score,cp_str = pattr_evaluator.pattr_score_cal(self.content,cp_priority,ftr_priority)
        self.target_catch = target_catch
        self.target_rate = target_rate
        self.pattr_fscore = pattr_fscore
        self.pattr_score = pattr_score
        self.pattr_metrics = pattr_metrics
        self.cp_str = cp_str
        
    def gen_pattr_table(self,bq_db_name,key_col,activity_table,prefix):
    ## Condition Part
        cond_part_list = []
        cond_flag_list = []
        for idx, act in enumerate(self.content):
            cond_part = "max(case when {cond} then 1 else 0 end) as {flag_name}".format(cond = act.replace("==","=").replace("&"," and ").replace(ftr_delim," and ").replace("=\"None\""," is null"),flag_name = 'act_'+str(idx))
            cond_part_list.append(cond_part)
            cond_flag_list.append('act_'+str(idx)+' = 1')
        pattern_cond = " and ".join(cond_flag_list)
        activity_cond = "\n,".join(cond_part_list)
        pattr_table_name = '{prefix}_{pattern_name}'.format(prefix = prefix,pattern_name = self.name)
        template = '''
        create or replace table {bq_db}.{pattr_table_name} OPTIONS(expiration_timestamp=timestamp_add(current_timestamp(), INTERVAL 1 DAY)) as (
        select 
        {key_column}
        ,case when {pattern_cond} then 1 else 0 end as {pattern_name} 
        from (
            select 
            {key_column}
            ,{activity_cond}
            from {act_table}
            group by 1) tab1
        );
        '''.format(bq_db=bq_db_name,pattr_table_name=pattr_table_name,pattern_cond = pattern_cond,pattern_name = self.name,
                   activity_cond = activity_cond,act_table = activity_table,key_column = key_col)
        # try:
        #     query_bq(template)
        #     logger.info("create pattr table %s"%pattr_table_name)
        #     return (self.name,pattr_table_name)
        # except Exception as err:
        #     logger.error("error message: %s"%str(err))
        #     raise Exception("Error in executing cmd: {cmd}".format(cmd=template))
        return (self.name,pattr_table_name,template)
        


class PatternsUtil:
#     def pattr_list_eval(pattr_list,pattr_evaluator,cp_priority,ftr_priority):
        
#         if len(pattr_list)>8:
#             Parallel(n_jobs=-1, backend = 'multiprocessing')(delayed(pattr.pattern_eval)
#                                             (pattr_evaluator,cp_priority,ftr_priority)  for pattr in  pattr_list)
#         else:
#             for pattr in  pattr_list:
#                 pattr.pattern_eval(pattr_evaluator,cp_priority,ftr_priority)

#         return pattr_list
    @staticmethod
    def pattr_list_eval(pattr_list,pattr_evaluator,cp_priority,ftr_priority):
        
        batch_sqls = [(pattr_evaluator.pattern_compute_sql(pattr.content,'sql_'+str(idx))) for idx,pattr in enumerate(pattr_list)]
        batch_res = pattr_evaluator.pattern_compute_batch(batch_sqls)
        for idx,pattr in  enumerate(pattr_list):
            pattr.pattr_metrics = batch_res['sql_'+str(idx)]['pattr_metrics']
            pattr.target_catch = batch_res['sql_'+str(idx)]['target_catch']
            pattr.target_rate = batch_res['sql_'+str(idx)]['target_rate']
            pattr.pattr_fscore = batch_res['sql_'+str(idx)]['pattr_fscore'] 
            pattr.pattr_score,pattr.cp_str = pattr_evaluator.pattr_score_cal(pattr.content,cp_priority,ftr_priority)
        return pattr_list
    
    
    @staticmethod
    def pattr_list_filter(pattr_list,eval_cutoff):
        qual_pattr_list = list(filter(lambda tup: tup.target_rate>=eval_cutoff,pattr_list))
        
        return qual_pattr_list
    
    @staticmethod
    def cal_similarity(Pattr_A,Pattr_B):
        # Pattr_A_uniq = set([item for pattr in Pattr_A.content for item in pattr.split(' and ')])
        # Pattr_B_uniq = set([item for pattr in Pattr_B.content for item in pattr.split(' and ')])
        Pattr_A_uniq = set([item for pattr in Pattr_A.content for item in pattr.split(ftr_delim)])
        Pattr_B_uniq = set([item for pattr in Pattr_B.content for item in pattr.split(ftr_delim)])

        sim = len(Pattr_A_uniq&Pattr_B_uniq)/len(Pattr_A_uniq|Pattr_B_uniq)
        return sim
    
    @staticmethod
    def update_pattr_list(pattr_list,new_pattr,overlap=0.5):
        similarity_list = []
        for pattr in pattr_list:
            similarity = PatternsUtil.cal_similarity(pattr,new_pattr)
            similarity_list.append(similarity)
        max_sim = np.max(similarity_list)
        if max_sim<=overlap:
            pattr_list.append(new_pattr)
        return pattr_list

    @staticmethod
    def rm_similar_pattrs(pattr_list,overlap=0.5):
        pattr_list = sorted(pattr_list,key=lambda tup: (tup.target_rate,tup.pattr_score),reverse = True)
        sel_pattr_list = []
        for pattr in pattr_list:
            if len(sel_pattr_list)>0:
                sel_pattr_list = PatternsUtil.update_pattr_list(sel_pattr_list,pattr,overlap)
            else:
                sel_pattr_list.append(pattr)
        return sel_pattr_list
    
    @staticmethod
    def rm_superset(all_combinations,sublists):
        result = [a for a in all_combinations if all(not set(a[0]).issuperset(set(b.content)) for b in sublists)]
        return result


#     def gen_pattr_tables(pattern_list,bq_db_name,key_col,activity_table,prefix):
#         if len(pattern_list)>8:
#             pattern_table_list = Parallel(n_jobs=-1,backend="threading")(delayed(pattr.gen_pattr_table)
#                                         (bq_db_name,key_col,activity_table,prefix)  for pattr in pattern_list) 
#         else:
#             pattern_table_list = []
#             for pattr in pattern_list:
#                 pattr_tbl = pattr.gen_pattr_table(bq_db_name,key_col,activity_table,prefix)
#                 pattern_table_list.append(pattr_tbl)

#         return pattern_table_list
    
    @staticmethod
    def gen_pattr_tables(pattern_list,bq_db_name,key_col,activity_table,prefix):
        batch_params = [pattr.gen_pattr_table(bq_db_name,key_col,activity_table,prefix) for pattr in pattern_list ]
        template = '\n'.join([batch[2] for batch in batch_params])
        try:
            query_bq(template)
            logger.info("create pattr table in batch")
            pattern_table_list = [(batch[0],batch[1]) for batch in batch_params]
            return pattern_table_list
        except Exception as err:
            logger.error("error message: %s"%str(err))
            raise Exception("Error in executing cmd: {cmd}".format(cmd=template))
    
    @staticmethod
    def gen_pattr_tag_tab(pattern_list,bq_db_name,key_col,base_driver,activity_table,prefix,tag_table_name,retention_day=14):
        pattern_table_list = PatternsUtil.gen_pattr_tables(pattern_list,bq_db_name,key_col,activity_table,prefix)
        logger.info("Start to merge pattern tables to tag table")
        join_part = []
        tag_part =[]
        drop_part = []
        for idx, pattr in enumerate(pattern_table_list):
            join_template = '''left join {bq_db}.{table_name} as tab{idx}
            on base.{key_column} =tab{idx}.{key_column}'''.format(bq_db = bq_db_name,key_column=key_col,table_name = pattr[1],idx = idx)
            tag_template = '''case when {pattr_name} is not null then {pattr_name} else 0 end as {pattr_name}'''.format(pattr_name=pattr[0])
            drop_template = '''drop table {bq_db}.{table_name};'''.format(bq_db = bq_db_name,table_name = pattr[1])
            join_part.append(join_template)
            tag_part.append(tag_template)
            drop_part.append(drop_template)

        final_template = '''
        create or replace table {bq_db}.{table_name} OPTIONS(expiration_timestamp=timestamp_add(current_timestamp(), INTERVAL {retention_day} DAY)) as (
        select 
        base.* 
        ,{tag_txt}
        from {base_driver} base
        {join_txt}
        );
        '''.format(retention_day = retention_day,bq_db = bq_db_name,table_name = tag_table_name,base_driver = base_driver,tag_txt = '\n,'.join(tag_part)
                   ,join_txt = '\n'.join(join_part))

        # drop_sql = '''
        # {drop_txt}
        # '''.format(drop_txt='\n'.join(drop_part))
        try:
            query_bq(final_template)
            logger.info(" Final tag table is done! table name: {tag_tab}".format(tag_tab=tag_table_name))
            # try:
            #     query_bq(drop_sql)
            #     logger.info(" {drop_sql}".format(drop_sql=drop_sql))
            # except Exception as err:
            #     logger.error("error message: %s"%str(err))
            #     raise Exception("Error in executing cmd: {cmd}".format(cmd=drop_sql))
        except Exception as err:
            logger.error("error message: %s"%str(err))
            raise Exception("Error in executing cmd: {cmd}".format(cmd=final_template))
        
        
    @staticmethod
    def pattr_catch_cal(pattr_list,bq_db_name,tag_table,tag_col,amt_col,eval_metric):
        if eval_metric =="target_cnt":
            amt_column = 1
        elif eval_metric=="target_amt":
            if amt_col:
                amt_column = amt_col
            else:
                logger.error("amt_col is None")
        
         
        pattr_name_list = [ pattr.name for pattr in pattr_list]
        query = '''
            select 
        {pattr_name_list}
        ,sum(case when {tag_col}=1 then {amt_column} else 0 end) as target_catch
        from {bq_db}.{tag_table}
        group by {group_list}
            '''.format(bq_db = bq_db_name,tag_table=tag_table,tag_col = tag_col,amt_column=amt_column,pattr_name_list = '\n,'.join(pattr_name_list),group_list = ','.join([str(idx+1) for idx in range(len(pattr_name_list))]))
        try:
            df = read_bq(query)
            df[pattr_name_list] = df[pattr_name_list].astype(int)
            
            df['target_catch'] = df['target_catch'].astype(float)
            return df
        except Exception as err:
            logger.error("error message: %s"%str(err))
            raise Exception("Error in executing query: {query}".format(query=query))
    
    @staticmethod
    def split_cond(df_catch,cond_list):
        df_catch_cp = copy.deepcopy(df_catch)
        range_list = [i*10 for i in range(int (len(cond_list)/10)+1)]
        for i in range(len(range_list )  ) :
            if i +1 < len(range_list):
                l1 = range_list[i] 
                l2 = range_list[i+1]
            else :
                l1= range_list[i] 
                l2 = len(cond_list)
            if cond_list[l1:l2]:
                query_str = '&'.join(cond_list[l1:l2])
                df_catch_cp = df_catch_cp.query(query_str)
        return df_catch_cp
    
    @staticmethod
    def inc_catch_rank(df_catch,tot_target,rank_pattern_list):
        for idx, pattr in enumerate(rank_pattern_list):
            hist_pattr_list = rank_pattern_list[:idx]
            if len(hist_pattr_list)>0:
                cond_list = [hist_pattr.name+'==0' for hist_pattr in hist_pattr_list if hist_pattr.name != pattr.name]
                df_query = PatternsUtil.split_cond(df_catch,cond_list)
                inc_catch = df_query[df_query[pattr.name]==1]['target_catch'].sum()

                pattr.inc_catch = inc_catch
                pattr.inc_catch_share = inc_catch/tot_target

            else:
                pattr.inc_catch = pattr.target_catch
                pattr.inc_catch_share = pattr.target_catch/tot_target

        return rank_pattern_list
    
    @staticmethod
    def dedup_pattrs(pattr_list,bq_db_name,tag_table,tag_col,amt_col,eval_metric,tot_target,dedup_cutoff,priority='target_rate+pattr_score'):
        '''
        priority: 
        1) target_catch: rank order by target caught by pattern
        2) target_rate: rank order by bad_rate of pattern
        3) fscore: rank by FScore calculated based on bad target_rate and target catch rate
        4) pattr_score: rank by pattr_score calculated based on checkpoint and feature importance of pattern
        5) target_rate+pattr_score: rank by target_rate,pattr_score 
        6) fscore+pattr_score: rank by fscore,pattr_score 
        7) target_catch+pattr_score: rank by target_catch,pattr_score 
        '''

        df_catch = PatternsUtil.pattr_catch_cal(pattr_list,bq_db_name,tag_table,tag_col,amt_col,eval_metric)
        if priority =='target_catch':
            rank_pattern_list = sorted(pattr_list, key=lambda x: x.target_catch, reverse=True)
        elif priority =='target_rate':
            rank_pattern_list = sorted(pattr_list, key=lambda x: x.target_rate, reverse=True)
        elif priority == 'fscore':
            rank_pattern_list = sorted(pattr_list, key=lambda x: x.pattr_fscore, reverse=True)
        elif priority == 'pattr_score':
            rank_pattern_list = sorted(pattr_list, key=lambda x: x.pattr_score, reverse=True)
        elif priority =='target_rate+pattr_score':
            rank_pattern_list = sorted(pattr_list, key=lambda x: (x.target_rate,x.pattr_score), reverse=True)
        elif priority =='fscore+pattr_score':
            rank_pattern_list = sorted(pattr_list, key=lambda x: (x.pattr_fscore,x.pattr_score), reverse=True)
        elif priority =='target_catch+pattr_score':
            rank_pattern_list = sorted(pattr_list, key=lambda x: (x.target_catch,x.pattr_score), reverse=True)
        else:
            raise Exception('priority setting is wrong!')
        rank_pattern_list = PatternsUtil.inc_catch_rank(df_catch,tot_target,rank_pattern_list)
        dedup_pattern_list = list(filter(lambda x: x.inc_catch_share > dedup_cutoff,rank_pattern_list))
        ranked_dedup_pattern_list = PatternsUtil.inc_catch_rank(df_catch,tot_target,dedup_pattern_list)
        return ranked_dedup_pattern_list

## write pattern to logging table
    @staticmethod
    def format_content(content):
        for k,v in content.items():
            if np.issubdtype(type(v), int):
                content[k] = int(v)
            elif np.issubdtype(type(v), float):
                content[k] = round(v,4)
        return content

    # def gen_row_map(qual_pattr_list,dedup_pattr_list):
    #     write_contents = []
    #     for pattr in qual_pattr_list:
    #         write_content = {
    #             'pattr_name':pattr.name
    #             ,'pattr_content':'|'.join(pattr.content).replace('"','\\"')
    #             ,'target_rate':pattr.target_rate
    #             ,'target_catch':pattr.target_catch
    #             ,'inc_catch':pattr.inc_catch
    #             ,'inc_catch_share':pattr.inc_catch_share
    #             ,'pattr_fscore':pattr.pattr_fscore
    #             ,'pattr_score':pattr.pattr_score
    #             }
    #         write_content.update(pattr.pattr_metrics)
    #         if pattr in dedup_pattr_list:
    #             write_content['after_dedup'] = 1
    #         else:
    #             write_content['after_dedup'] = 0
    #         write_content = PatternsUtil.format_content(write_content)
    #         write_contents.append(write_content)
    #     return write_contents

    
    # def pattr_log(job_log,qual_pattr_list,dedup_pattr_list):
    #     write_contents = PatternsUtil.gen_row_map(qual_pattr_list,dedup_pattr_list)
    #     for line in write_contents:
    #         content_map_json = json.dumps(line)
    #         job_log.row_map['content'] = content_map_json
    #         store_log = StoreLog(job_log)
    #         store_log.store()
    @staticmethod
    def pattr_log(job_log,qual_pattr_list,dedup_pattr_list):
        sql_list = []
        for pattr in qual_pattr_list:
            job_log.row_map['pattr_name'] = pattr.name
            job_log.row_map['pattr_id'] = 'pattr_'+str(uuid.uuid4()).replace('-', '')
            job_log.row_map['cp_str'] = pattr.cp_str
            job_log.row_map['pattr_content']= cp_delim.join(pattr.content).replace('"','\\"').replace("'","\\'")
            job_log.row_map['target_rate']= round(float(pattr.target_rate),4)
            job_log.row_map['target_catch']= round(float(pattr.target_catch),4)
            job_log.row_map['inc_catch']= round(float(pattr.inc_catch),4)
            job_log.row_map['inc_catch_share']= round(float(pattr.inc_catch_share),4)
            job_log.row_map['pattr_fscore']= round(float(pattr.pattr_fscore),4)
            job_log.row_map['pattr_score']= round(float(pattr.pattr_score),4)
            other_metrics = PatternsUtil.format_content(pattr.pattr_metrics)
            other_metrics_json = json.dumps(other_metrics, cls=DecimalEncoder)
            job_log.row_map['other_metrics'] = other_metrics_json
            if pattr in dedup_pattr_list:
                job_log.row_map['after_dedup'] = True
            else:
                job_log.row_map['after_dedup'] = False
            updt_dt = datetime.date.today()
            updt_ts = datetime.datetime.now()
            job_log.row_map['update_dt'] = updt_dt
            job_log.row_map['update_ts'] = updt_ts

            store_log = StoreLog(job_log)
            sql = store_log.store_sql()
            sql_list.append(sql)
        
        template = '\n'.join(sql_list)
        try:
            query_bq(template)
        except Exception as e:
            logger.error("insert log failed, sql: {sql}".format(sql = template))
            raise Exception("insert log failed, e: {e}".format(e = e))
            return False
        else:
            
            logger.info("insert log:")
            logger.info(template)
            return True
        
    @staticmethod
    def act_log(job_log,act_list):
        sql_list = []
        for act in act_list:
            job_log.row_map['act_id'] = 'act_'+str(uuid.uuid4()).replace('-', '')
            job_log.row_map['act_content'] = act.content.replace('"','\\"').replace("'","\\'")
            job_log.row_map['target_rate']= round(float(act.target_rate),4)
            job_log.row_map['target_catch']= round(float(act.target_catch),4)
            updt_dt = datetime.date.today()
            updt_ts = datetime.datetime.now()
            job_log.row_map['update_dt'] = updt_dt
            job_log.row_map['update_ts'] = updt_ts

            store_log = StoreLog(job_log)
            sql = store_log.store_sql()
            sql_list.append(sql)
            
        template = '\n'.join(sql_list)
        try:
            query_bq(template)
        except Exception as e:
            logger.error("insert log failed, sql: {sql}".format(sql = template))
            raise Exception("insert log failed, e: {e}".format(e = e))
            return False
        else:
            
            logger.info("insert log:")
            logger.info(template)
            return True
