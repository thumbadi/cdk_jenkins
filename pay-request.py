import sys
import subprocess
import boto3
import datetime
import logging
from botocore.exceptions import NoCredentialsError
import json
from functools import reduce
from awsglue.transforms import *
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col,lit
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
subprocess.call([sys.executable, "-m", "pip", "install", "--user", "psycopg2-binary"])
import psycopg2
import pandas as pd
import numpy as np
import os
from urllib.parse import urlparse
import re
import pyarrow as pa
from datetime import datetime
import pytz

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'MTF_DBNAME', 'S3_BUCKET', 'ENV'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger = logging.getLogger()
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.DEBUG)
glue_client = boto3.client('glue')
def postgres_query(jdbc_url,mtf_db,schema,table,**kwargs):    
    seq = kwargs.get("action")
    if seq == "read":
        
        query = f"""(SELECT
    CURRENT_DATE AS "TransactionDate",
    d.payee_id AS "PayeeId",
    c.internal_claim_num AS "TransactionId",
    x.mfr_ref_id AS "ManufacturerId",-- <<== replaced drug_id with mfr_ref_id
    c.drug_id AS "DrugId",  
    d.org_id AS "DispenserId",
    p.disb_amt AS "DisbursementAmount",
    pcr.pymt_category_desc AS "PaymentCategory",
    c.received_dt,
    c.received_id
FROM claim.mtf_claim c
JOIN claim.mtf_claim_manufacturer m
    ON c.received_dt = m.received_dt
    AND c.received_id = m.received_id
JOIN claim.mtf_claim_de_tpse d
    ON c.received_dt = d.received_dt
    AND c.received_id = d.received_id
JOIN claim.mtf_claim_pymt_request p
    ON c.received_dt = p.claim_received_dt
    AND c.received_id = p.claim_received_id
JOIN claim.pymt_category_ref pcr
    ON p.pymt_category_cd = pcr.pymt_category_cd
INNER JOIN (
    SELECT m.claim_loctn_cd
    FROM claim.mtf_claim_msg_loctn_map m
    WHERE m.claim_msg_cd = '151'
) l ON c.mtf_claim_curr_loctn_cd = l.claim_loctn_cd
LEFT JOIN (
    SELECT 
        a.mfr_id, 
        b.mfr_ref_id, 
        a.drug_id
    FROM shared.drug_dtl a
    LEFT JOIN shared.mfr_dtl b ON a.mfr_id = b.mfr_id
) x 
ON m.mfr_id::text = x.mfr_id::text)"""

        df = spark.read.format("jdbc") \
                .option("url", f"{jdbc_url}") \
                .option("dbtable", query) \
                .option("user", credentials['username']) \
                .option("password", credentials['password']) \
                .option("driver", "org.postgresql.Driver") \
                .load()
        return df
        
    elif seq == "update":
        id_list = kwargs.get("id")
    
        conn = psycopg2.connect(
        dbname=mtf_db,
        user=credentials['username'],
        password=credentials['password'],
        host=host,
        port=port
        )
        cursor = conn.cursor()
        
        query = """
            UPDATE claim.mtf_claim \
            SET mtf_claim_curr_loctn_cd = %s, update_ts = %s, update_user_id = %s\
            WHERE internal_claim_num = (%s) AND received_dt = %s
        """       
             
        if id_list:  # Prevent empty execution
            cursor.executemany(query,[tuple(item) for item in id_list])

        
        conn.commit()
        cursor.close()
        conn.close()
    
    elif seq == "delete":
        df = kwargs.get("dat")
        df = df[["received_dt", "received_id"]]
        delete_query = f"""
            DELETE FROM {schema}.{table} WHERE received_dt=%s AND received_id=%s
            """
        conn = psycopg2.connect(
            dbname=mtf_db, user=credentials['username'], password=credentials['password'], host=host, port=port
        )
        cursor = conn.cursor()
        cursor.executemany(delete_query, df.values.tolist())
        conn.commit()
        cursor.close()
        conn.close() 
        
    elif seq == "insert":

        df = kwargs.get("dat")
        df = df[["received_dt", "received_id", "TransactionId", "insert_ts"]]
        df["claim_msg_cd"] = "201"
        df["insert_user_id"] = -1
        insert_query = f"""
            INSERT INTO {schema}.{table} (received_dt, received_id, internal_claim_num, insert_ts, claim_msg_cd, insert_user_id)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        conn = psycopg2.connect(
            dbname=mtf_db, user=credentials['username'], password=credentials['password'], host=host, port=port
        )
        cursor = conn.cursor()
        cursor.executemany(insert_query, df.values.tolist())
        conn.commit()
        cursor.close()
        conn.close()
        
    elif seq == "insert2":

        df = kwargs.get("dat")
        df = df[["received_dt", "received_id", "TransactionId", "insert_ts"]]
        df["claim_msg_cd"] = "201"
        df["claim_loctn_cd"] = "050"
        df["insert_user_id"] = -1
        df["claim_msg_cd_json"] = df["claim_msg_cd"].apply(lambda x: json.dumps([x]))
        insert_query = f"""
            INSERT INTO {schema}.{table} (received_dt, received_id, internal_claim_num, insert_ts, claim_msg_cd, claim_loctn_cd, insert_user_id, claim_msg_cd_json)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        conn = psycopg2.connect(
            dbname=mtf_db, user=credentials['username'], password=credentials['password'], host=host, port=port
        )
        cursor = conn.cursor()
        #cursor.executemany(insert_query, df.values.tolist())
        for i, row in enumerate(df.values.tolist()):
            try:
                print(f"*************Insert 2 on row {i}: {row}")
                cursor.execute(insert_query, row)
            except Exception as e:
                print(f"*************Error on row {i}: {row}")
        conn.commit()
        cursor.close()
        conn.close()

    elif seq == "meta":
        df = kwargs.get("dat")
        insert_query = f"""
            INSERT INTO {schema}.{table} (job_run_id, claim_file_type_cd, claim_file_name, claim_file_size, file_rec_cnt, claim_file_stus_cd, insert_user_id, insert_ts)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        conn = psycopg2.connect(
            dbname=mtf_db, user=credentials['username'], password=credentials['password'], host=host, port=port
        )
        cursor = conn.cursor()
        cursor.executemany(insert_query, df.values.tolist())
        conn.commit()
        cursor.close()
        conn.close()

    
def get_secret(secret_name):
    """Fetch secret from AWS Secrets Manager."""
    region_name = "us-east-1"  # Modify if needed
    client = boto3.client('secretsmanager')

    try:
        # Fetch the secret value
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)

        # If the secret is stored as a string
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            # If the secret is binary
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            secret = decoded_binary_secret.decode("utf-8")

        # Parse the secret (assuming itâs JSON)
        secret_dict = json.loads(secret)
        return secret_dict

    except Exception as e:
        print(f"Error retrieving secret: {e}")
        raise e

def get_GlueJob_id():
    # Fetch latest running job
    #glue_client = boto3.client('glue')
    response = glue_client.get_job_runs(JobName=args['JOB_NAME'])
    latest_job_run_id = response['JobRuns'][0]['Id'] if response['JobRuns'] else 'UNKNOWN'
    return latest_job_run_id

schema_data =  [
    {
    "read" : {
            "table_name" : "mtf_claim",
            "schema" : "claim"
    }},
    {"update" : {
            "table_name" : "mtf_claim",
            "schema" : "claim"
    }},
    {"delete" : {
            "table_name" : "mtf_claim_process_msg",
            "schema" : "claim"
    }},
    {"insert" : {
        "table_name" : "mtf_claim_process_msg",
        "schema" : "claim"
    }},
    {"insert2" : {
        "table_name" : "mtf_claim_process_loctn",
        "schema" : "claim"
    }},
    {"meta" : {
        "table_name" : "claim_file_metadata",
        "schema" : "claim"
    }}
    ]
    
payfile_schema = pa.schema([
    pa.field('TransactionDate', pa.date64(), nullable=False),
    pa.field('PayeeId', pa.string(), nullable=False),
    pa.field('TransactionId', pa.string(), nullable=False),
    pa.field('ManufacturerId', pa.string(), nullable=False),
    pa.field('DrugId', pa.string(), nullable=False),
    pa.field('DispenserId', pa.string(), nullable=False),
    pa.field('DisbursementAmount', pa.decimal128(14,2), nullable=False),
    pa.field('PaymentCategory', pa.string(), nullable=False)
])

mtf_connection = glue_client.get_connection(Name='MTFDMDataConnector')
mtf_connection_options = mtf_connection['Connection']['ConnectionProperties']
jdbc_url = mtf_connection['Connection']['ConnectionProperties']['JDBC_CONNECTION_URL']
credentials=get_secret(mtf_connection_options['SECRET_ID'])
mtf_db=args['MTF_DBNAME']
s3_bucket=args['S3_BUCKET']
env=args['ENV'].lower()
parsed=urlparse(jdbc_url.replace("jdbc:", ""))
port=parsed.port
host=None
pattern = r"([\w.-]+\.rds\.amazonaws\.com)"
match = re.search(pattern, jdbc_url)
if match:
    host= match.group(1)

df_final = pd.DataFrame()
stage_error = False
job_id = None
meta_info = []
utc_now = datetime.utcnow()
eastern = pytz.timezone('America/New_York')
utc_minus_4 = pytz.utc.localize(utc_now).astimezone(eastern)


for entry in schema_data:
    if stage_error == False:
        for key in entry.keys():        
            schema = entry[key]["schema"]
            table = entry[key]["table_name"]        
            bucket_name = s3_bucket
            if key == "read":
                view_df = postgres_query(jdbc_url,mtf_db,schema,table,action="read")
                if view_df.isEmpty() == False:
                    #ts = datetime.datetime.today().strftime("%Y%m%d_%H%M%S")
                    ts = utc_minus_4.strftime("%Y%m%d_%H%M%S")
                    file_name=f"mtfdm_{env}_PaymentRequestData_{ts}.parquet"
                    file_path = f"payrequest/ready/{file_name}"
                    df = view_df.toPandas()
                    columns_to_remove = ["received_dt","received_id"]
                    df_parquet = df.drop(columns=columns_to_remove)
                    df_parquet.to_parquet(f"/tmp/{file_name}",schema=payfile_schema)
                    insert_ts = utc_minus_4.strftime("%Y-%m-%d %H:%M:%S")
                    df['insert_ts'] = insert_ts
                    meta_file_size = os.path.getsize(f"/tmp/{file_name}")
                    s3_client = boto3.client('s3')
                    s3_client.upload_file(f"/tmp/{file_name}", bucket_name, file_path)
                    df_final = df
                    job_id = get_GlueJob_id()
                    meta_info.append([job_id,"008",file_name,meta_file_size,df.shape[0],"COMPLETED",-1,insert_ts])
                    logger.info('Parquet file has been created.')
                    continue
                else:
                    logger.info('No records found in mtf_claim table.')
                    stage_error = True
            elif key == "update":
                #if null_query_output == False:
                    df_final["update_ts"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                    df_final["update_user_id"] = -1
                    df_final["mtf_claim_curr_loctn_cd"] = '050'
                    claim_id = df_final[['mtf_claim_curr_loctn_cd','update_ts','update_user_id','TransactionId','received_dt']].values.tolist()
                    postgres_query(jdbc_url,mtf_db,schema,table,id = claim_id, action="update")
            elif key == "delete":
                postgres_query(jdbc_url,mtf_db,schema,table,action="delete",dat=df_final)
            elif key == "insert":
               # if null_query_output == False:
                    postgres_query(jdbc_url,mtf_db,schema,table,action="insert", dat=df_final)
            elif key == "insert2":
               # if null_query_output == False:
                    postgres_query(jdbc_url,mtf_db,schema,table,action="insert2", dat=df_final)
            elif key == "meta":
                meta_cols = ["job_run_id","claim_file_type_cd","claim_file_name","claim_file_size","file_rec_cnt","claim_file_stus_cd","insert_user_id","insert_ts"]
                meta_df = pd.DataFrame(meta_info, columns=meta_cols)
                postgres_query(jdbc_url,mtf_db,schema,table,action="meta",dat=meta_df)
    else:
        break

job.commit()
