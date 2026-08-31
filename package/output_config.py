class OutputConfig:
    def __init__(self, bq_db_name, activity_table_name, tag_table_name='', retention_day = 14, replace = False):
        self.bq_db_name = bq_db_name
        self.activity_table_name = activity_table_name
        self.tag_table_name = tag_table_name
        self.retention_day = retention_day
        self.replace = replace