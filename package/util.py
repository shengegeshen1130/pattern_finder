from google.cloud import storage
from google.cloud import bigquery
from google.cloud.bigquery import QueryJobConfig, QueryPriority
from io import StringIO
import pandas as pd
import pandas_gbq
from time import time 
from google.cloud.exceptions import NotFound
from loguru import logger

ftr_delim = chr(6)
cp_delim = chr(7)


def fscore(bad_rate,bad_catch,beta=1):
        if bad_rate>0 and bad_catch>0:
            F_Score = (1+beta**2)*(bad_rate*bad_catch)/(beta**2*bad_rate+bad_catch)
        else:
            F_Score = 0
        return F_Score

# def query_bq(create_and_insert_sql):
#     bqclient = bigquery.Client()
#     res = bqclient.query(create_and_insert_sql).result()
#     return res

def query_bq(create_and_insert_sql):
    # bqclient = bigquery.Client()
    default_conf = QueryJobConfig(priority=QueryPriority.BATCH)
    bqclient = bigquery.Client(default_query_job_config=default_conf)
    res = bqclient.query(create_and_insert_sql).result()
    return res
    

def read_bq(sql):
    df =pd.read_gbq(sql,use_bqstorage_api=True,configuration={'query': {'priority': 'BATCH'}})
    # df =pd.read_gbq(sql)
    return df
    
def to_gbq_workaround(df, table_id, project_id):
    temp_csv_string = df.to_csv(sep=";", index=False)
    temp_csv_string_IO = StringIO(temp_csv_string)
    # create new dataframe from string variable
    new_df = pd.read_csv(temp_csv_string_IO, sep=";")
    # this new df can be uploaded to BQ with no issues
    new_df.to_gbq(table_id, project_id = project_id, if_exists="append")

def pandas_gbq_numeric_type_patch():
    import pandas_gbq
    if hasattr(pandas_gbq.gbq, '__NUMERIC_PATCHED'):
        pandas_gbq.gbq.logger.info("NUMERIC type compatibility has been patched, do nothing")
        print("NUMERIC type compatibility has been patched, do nothing")
        return
    
    _bqschema_to_nullsafe_dtypes_ori  = pandas_gbq.gbq._bqschema_to_nullsafe_dtypes
    def _bqschema_to_nullsafe_dtypes_new(schema_fields):
        import copy
        c_schema_fields = copy.deepcopy(schema_fields)
        for field in c_schema_fields:
            if field['type'].upper() == 'NUMERIC' or field['type'].upper() == 'BIGNUMERIC':
                field['type'] = 'FLOAT'
        return _bqschema_to_nullsafe_dtypes_ori(c_schema_fields)
    pandas_gbq.gbq._bqschema_to_nullsafe_dtypes = _bqschema_to_nullsafe_dtypes_new
    
    setattr(pandas_gbq.gbq, '__NUMERIC_PATCHED', True)
    pandas_gbq.gbq.logger.info("NUMERIC type compatibility patched")
    print("NUMERIC type compatibility patched")

def write_bucket(content,file,bucket_name):
    storage_client = storage.Client()
    # The name for the  bucket
    bucket_name = bucket_name
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file)
    with blob.open('w') as f:
        f.write(content)
    print("write: %s"%content)


def read_bucket(file,bucket_name):
    storage_client = storage.Client()
    # The name for the  bucket
    bucket_name = bucket_name
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file)
    with blob.open('rt') as f:
        res = f.read()
    print("read: %s"%res)
    return res

def generate_batch_id(file,bucket_name,tag='fp'):
    label = tag+'_'+str(int(time()))
    write_bucket(label,file,bucket_name)
    return label

def if_tbl_exists(table_ref):
    bqclient = bigquery.Client()
    try:
        bqclient.get_table(table_ref)
        return True
    except NotFound:
        return False