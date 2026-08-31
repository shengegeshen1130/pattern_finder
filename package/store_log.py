from google.cloud import bigquery
from loguru import logger
import sys
import numpy as np
import datetime
from util import query_bq
from decimal import Decimal

# logger = logging.getLogger()
# logger.setLevel(logging.INFO)

# console = logging.StreamHandler(stream = sys.__stderr__)
# console.setLevel(logging.INFO)

# FORMAT = '%(asctime)s - %(funcName)s[%(levelname)s]: %(message)s'
# console.setFormatter(logging.Formatter(FORMAT))

# logger.addHandler(console)

class StoreLog:
    def __init__(self, job_log):
        self.job_log = job_log

    def concat_col_list(self):
        col_list = list(self.job_log.row_map.keys())
        return ", ".join(col_list)

    # def concat_value_list(self):
    #     value_list = list(self.job_log.row_map.values())
    #     return ", ".join("'{0}'".format(x) for x in value_list)

    def concat_value_list(self):
        value_list = list(self.job_log.row_map.values())
        log_list = []
        for x in value_list:
            if np.issubdtype(type(x),float) or np.issubdtype(type(x),int) or np.issubdtype(type(x),bool) or isinstance(x, Decimal):
                log_list.append("{0}".format(x))
            elif isinstance(x,(datetime.datetime)):
                log_list.append("datetime\'%s\'"%(x.strftime('%Y-%m-%d %H:%M:%S')))
                
            elif isinstance(x,(datetime.date)):
                log_list.append("date\'%s\'"%(x.strftime('%Y-%m-%d')))
            
            else:
                log_list.append("'{0}'".format(x))

        return ", ".join(log_list)

    def concat_sql(self, col_list, value_list):
        sql = """
        insert into %s.%s (%s)
        values (%s);
        """ % (self.job_log.LOG_BQ_DB_NAME, self.job_log.LOG_BQ_TABLE_NAME, col_list, value_list)

        return sql

    def store(self):
        col_list = self.concat_col_list()
        value_list = self.concat_value_list()
        sql = self.concat_sql(col_list, value_list)
        
        try:
            query_bq(sql)
        except Exception as e:
            logger.error("insert log failed, sql: {sql}".format(sql = sql))
            raise Exception("insert log failed, e: {e}".format(e = e))
            return False
        else:
            
            logger.info("insert log:")
            logger.info(sql)
            return True
        
    def store_sql(self):
        col_list = self.concat_col_list()
        value_list = self.concat_value_list()
        sql = self.concat_sql(col_list, value_list)
        return sql
    
    def store_batch(self,sql_list):
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
    

        

