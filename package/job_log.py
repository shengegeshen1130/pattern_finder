from google.cloud import bigquery
from loguru import logger
import sys
from util import if_tbl_exists,query_bq

# logger = logging.getLogger()


class JobLog:
    def __init__(self,row_map,bq_db_name='123',table_name='test'):
        self.LOG_BQ_DB_NAME = bq_db_name
        self.LOG_BQ_TABLE_NAME = table_name
        self.row_map = row_map
        
    def clean_log(self):
        if 'job_id' in self.row_map.keys():
            sql = '''
            delete from {bq_db_name}.{table_name}
            where job_id = '{job_id}'
            '''.format(bq_db_name=self.LOG_BQ_DB_NAME,table_name=self.LOG_BQ_TABLE_NAME,job_id=self.row_map['job_id'])
        elif 'batch_id' in self.row_map.keys() and 'alert_id' in self.row_map.keys():
            sql = '''
            delete from {bq_db_name}.{table_name}
            where batch_id = '{batch_id}' and alert_id = '{alert_id}'
            '''.format(bq_db_name=self.LOG_BQ_DB_NAME,table_name=self.LOG_BQ_TABLE_NAME,batch_id=self.row_map['batch_id'],alert_id=self.row_map['alert_id'])
        try:
            query_bq(sql)
        except Exception as e:
            logger.error("delete log failed, e: {e}".format(e = e))
            raise Exception("delete log failed, e: {e}".format(e = e))
            return False
        else:
            
            logger.info("delete log")
            logger.info(sql)
            return True
    
    def create_empty_table(self,type = 'fp_result'):
        
        tbl_name = self.LOG_BQ_DB_NAME+'.'+self.LOG_BQ_TABLE_NAME
        logger.info("Pattern Logging Table Check: Checking and Creating {table_name}".format(table_name=tbl_name))
        if not if_tbl_exists(tbl_name):
            if type == 'fp_result':
                create_table_sql = """
                                    create table {tbl_name} (
                                    batch_id string
                                    ,alert_id string
                                    ,pattr_name string
                                    ,pattr_id string
                                    ,pattr_content string
                                    ,cp_str string 
                                    ,target_rate float64
                                    ,target_catch float64
                                    ,inc_catch float64
                                    ,inc_catch_share float64
                                    ,pattr_fscore float64
                                    ,pattr_score float64
                                    ,other_metrics string
                                    ,after_dedup bool
                                    ,update_dt date
                                    ,update_ts datetime
                                );""".format(tbl_name=tbl_name)
            elif type =='service_result':
                create_table_sql = """
                                    create table {tbl_name} (
                                    job_id string
                                    ,pattr_name string
                                    ,pattr_id string
                                    ,pattr_content string
                                    ,cp_str string 
                                    ,target_rate float64
                                    ,target_catch float64
                                    ,inc_catch float64
                                    ,inc_catch_share float64
                                    ,pattr_fscore float64
                                    ,pattr_score float64
                                    ,other_metrics string
                                    ,after_dedup bool
                                    ,update_dt date
                                    ,update_ts datetime
                                );""".format(tbl_name=tbl_name)
            elif type =='service_act_res':
                create_table_sql = """
                                    create table {tbl_name} (
                                    job_id string
                                    ,act_id string
                                    ,act_content string
                                    ,target_rate float64
                                    ,target_catch float64
                                    ,update_dt date
                                    ,update_ts datetime
                                );""".format(tbl_name=tbl_name)
            elif type =='fp_act_res':
                create_table_sql = """
                                    create table {tbl_name} (
                                    batch_id string
                                    ,alert_id string
                                    ,act_id string
                                    ,act_content string
                                    ,target_rate float64
                                    ,target_catch float64
                                    ,update_dt date
                                    ,update_ts datetime
                                );""".format(tbl_name=tbl_name)
                
            elif type == 'fp_log':
                 create_table_sql = """
                                    create table {tbl_name} (
                                    batch_id string
                                    ,alert_id string
                                    ,content string
                                    ,update_dt date
                                    ,update_ts datetime
                                );""".format(tbl_name=tbl_name)
            
            elif type == 'service_log':
                 create_table_sql = """
                                    create table {tbl_name} (
                                    job_id string
                                    ,content string
                                    ,update_dt date
                                    ,update_ts datetime
                                );""".format(tbl_name=tbl_name)
                    

            else:
                logger.error("type setting is wrong!")
            try:
                query_bq(create_table_sql)
            except Exception as e:
                logger.error("create table {tbl_name} failed, e: {e}".format(tbl_name=tbl_name,e = e))
                raise Exception("create table {tbl_name} failed, e: {e}".format(tbl_name=tbl_name,e = e))
                return False
            else:
                
                logger.info("create table {tbl_name} sucessfully".format(tbl_name=tbl_name))
        else:
            logger.info("table {tbl_name} exists".format(tbl_name=tbl_name))







