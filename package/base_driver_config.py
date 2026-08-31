class BaseDriverConfig:
    def __init__(self, base_bq_db, base_table_name, acct_col,cp_acct_col, ts_col, key_col, tag_col, amt_col= None, gloss_col = None, nloss_col = None, tw = 7, tz = 'PST', ts_type = 'timestamp', af_tw = None, limit_num = 400):
        self.base_bq_db = base_bq_db
        self.base_table_name = base_table_name
        self.acct_col = acct_col
        self.cp_acct_col = cp_acct_col
        self.ts_col = ts_col
        self.key_col = key_col
        self.tag_col = tag_col
        self.amt_col = amt_col
        self.gloss_col = gloss_col
        self.nloss_col = nloss_col
        self.tw = tw
        self.tz = tz
        self.ts_type = ts_type
        self.af_tw = af_tw
        self.limit_num = limit_num

class PatternSearchAlgorithmInput:
    def __init__(self, driver, activity_table, filter_cond = None):
        if filter_cond:
            self.activity_table = '''
            (
            select * 
            from {table_name}
                where 1=1
                {filter_cond} 
                ) 
            '''.format(table_name=activity_table,filter_cond=filter_cond)

            self.base_driver = '''
            (
            select * 
            from {base_bq_db}.{table_name}
                where 1=1
                {filter_cond} 
                ) 
            '''.format(base_bq_db= driver.base_bq_db, table_name=driver.base_table_name,filter_cond=filter_cond)

        else:
            self.activity_table = activity_table
            self.base_driver = driver.base_bq_db+'.'+driver.base_table_name
        self.prefix = driver.base_table_name
        self.key_col = driver.key_col
        self.acct_col = driver.acct_col
        self.tag_col = driver.tag_col
        self.amt_col = driver.amt_col
        self.gloss_col = driver.gloss_col
        self.nloss_col = driver.nloss_col