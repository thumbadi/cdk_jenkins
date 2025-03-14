import sys
import subprocess
import boto3
import datetime
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


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def postgres_query(db,schema,table,**kwargs):    
    seq = kwargs.get("action")
    if seq == "read":
        query = f"""(select c.internal_claim_num as "MTF_ICN", c.xref_internal_claim_num as "MTF_XREF_ICN", c.received_dt as "PROCESS_DT", 
c.src_claim_type_cd as "SRC_CLAIM_TYPE_CODE", c.medicare_src_of_coverage as "MEDICARE_SRC_OF_COVERAGE", c.srvc_dt as "SRVC_DT", 
c.rx_srvc_ref_num as "RX_SRVC_REF_NUM", coalesce(c.fill_num,'0') as "FILL_NUM", c.ncpdp_id as "NCPDP_ID", c.srvc_npi_num as "SRVC_PRVDR_ID", 
c.prescriber_id as "PRESCRIBER_ID", c.ndc_cd as "NDC_CD", c.quantity_dispensed as "QUANTITY_DISPENSED", c.days_supply  as "DAYS_SUPPLY", 
c.indicator_340b_yn as "340b_INDICATOR", c.orig_submitting_contract_num as "SUBMT_CONTRACT", b.wac_amt as "WAC", b.mfp_amt as "MFP",
b.sdra_amt as "SDRA", a.pymt_pref as "SRVC_PRVDR_PYMT_PREF",
null as "PREV_NDC_CD", null as "PREV_PYMT_AMT", null as "PREV_PYMT_DT", 
null as PREV_PYMT_QUANTITY, null as "PREV_PYMT_MTHD_CD", null as "MRA_ERR_CD_1", null as "MRA_ERR_CD_2", null as "MRA_ERR_CD_3",
null as "MRA_ERR_CD_4", null as "MRA_ERR_CD_5", null as "MRA_ERR_CD_6", null as "MRA_ERR_CD_7", null as "MRA_ERR_CD_8", null as "MRA_ERR_CD_9", 
null as "MRA_ERR_CD_10", null as "MTF_PM_IND", null as "PYMT_MTHD_CD", null as "PYMT_QUANTITY", null as "PYMT_AMT", 
null as "PYMT_ADJ_IND", null as "PYMT_TS", d.mfr_id as "MANUFACTURER_ID", d.mfr_name as "MANUFACTURER_NAME", b.received_id as "RECEIVED_ID"
from claim.mtf_claim c join claim.mtf_claim_de_tpse a on a.received_dt = c.received_dt and a.received_id = c.received_id
     join  claim.mtf_claim_pricing b on b.received_dt = c.received_dt and b.received_id = c.received_id
     join  claim.mtf_claim_manufacturer d on d.received_dt = c.received_dt and d.received_id = c.received_id
where c.mtf_curr_claim_stus_ref_cd = 'MRN')
            """
        df = spark.read.format("jdbc") \
            .option("url", f"jdbc:postgresql://{host}:{5432}/{db}") \
            .option("dbtable", query) \
            .option("user", credentials['username']) \
            .option("password", credentials['password']) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        return df
    elif seq == "update":
        id_list = kwargs.get("id")

        conn = psycopg2.connect(
        dbname=db,
        user=credentials['username'],
        password=credentials['password'],
        host=host,
        port=5432
        )
        cursor = conn.cursor()
        
        query = """
            UPDATE claim.mtf_claim \
            SET mtf_curr_claim_stus_ref_cd = 'RCD', update_ts = %s, update_user_id = %s\
            WHERE internal_claim_num = (%s) AND received_dt = %s
        """       
             
        if id_list:  # Prevent empty execution
            cursor.executemany(query,[tuple(item) for item in id_list])

        
        conn.commit()
        cursor.close()
        conn.close()

    elif seq == "insert":

        df = kwargs.get("dat")
        df = df[["process_dt", "received_id", "mtf_icn"]]
        df["mtf_claim_stus_ref_cd"] = "RCD"
        df["insert_user_id"] = -1
        #df = df.rename(columns={"internal_claim_num": "mtf_icn", "received_dt": "process_dt"})
        insert_query = f"""
            INSERT INTO {schema}.{table} (received_dt, received_id, internal_claim_num, mtf_claim_stus_ref_cd, insert_user_id)
            VALUES (%s, %s, %s, %s, %s)
        """
        conn = psycopg2.connect(
            dbname=db, user=credentials['username'], password=credentials['password'], host=host, port="5432"
        )
        cursor = conn.cursor()
        cursor.executemany(insert_query, df.values.tolist())
        conn.commit()
        cursor.close()
        conn.close()
        
    elif seq == "meta":
        df = kwargs.get("dat")
        insert_query = f"""
            INSERT INTO {schema}.{table} (job_run_id, claim_file_type_cd, claim_file_name, mfr_id, file_rec_cnt,claim_file_stus_cd,insert_user_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        conn = psycopg2.connect(
            dbname=db, user=credentials['username'], password=credentials['password'], host=host, port="5432"
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
    glue_client = boto3.client('glue')
    response = glue_client.get_job_runs(JobName=args['JOB_NAME'])
    latest_job_run_id = response['JobRuns'][0]['Id'] if response['JobRuns'] else 'UNKNOWN'

    # print(f"Latest Glue {args['JOB_NAME']} and Job Run ID: {latest_job_run_id}")
    # print("*****Test output")
    return latest_job_run_id
    
schema_data =  [
    {
    "read" : {
            "table_name" : "mtf_claim",
            "schema" : "claim",
            "database" : "mtf"
    }},
    {"update" : {
            "table_name" : "mtf_claim",
            "schema" : "claim",
            "database" : "mtf"
    }},
    {"insert" : {
        "table_name" : "mtf_claim_process_status",
        "schema" : "claim",
        "database" : "mtf"
    }},
    {"meta" : {
        "table_name" : "claim_file_metadata",
        "schema" : "claim",
        "database" : "mtf"
    }}
    ]

host = "dev-mtfdm-db-cluster.cluster-cjsa40wuo8ej.us-east-1.rds.amazonaws.com"
secret_name = "dev/rds/postgres/app/mtfdm"
mfr="mfr_"
credentials = get_secret(secret_name)
df_final = pd.DataFrame()
stage_error = False
job_id = None
meta_info = []
for entry in schema_data:
    if stage_error == False:
        for key in entry.keys():        
            database = entry[key]["database"]
            schema = entry[key]["schema"]
            table = entry[key]["table_name"]        

            if key == "read":
                mfg_result = postgres_query(database,schema,table,action="read")
                mfr_list = [row for row  in mfg_result.select("MANUFACTURER_ID", "MANUFACTURER_NAME").distinct().collect()]

                if len(mfr_list) == 0:
                    print("No records found in the result")
                    stage_error = True
                else:
                    for row in mfr_list:
                        id_value = row["MANUFACTURER_ID"]
                        mfg_name_value = row["MANUFACTURER_NAME"]
                        s3_folder_mfr = mfg_name_value.replace(" ","_").replace("-","_").lower()
                        df_filtered = mfg_result.filter((col("MANUFACTURER_ID") == id_value))
                        ts = datetime.datetime.today().strftime("%m%d%Y.%H%S")
                        file_path = f"{mfr}{s3_folder_mfr}-{id_value}/mrn/outbound/{s3_folder_mfr}.{ts}.parquet"
                        df = df_filtered.toPandas()
                        df = df.sort_values(by="SRVC_PRVDR_ID")
                        #df.columns = df.columns.str.upper()
                        
                        df.to_parquet(f"/tmp/{mfg_name_value}.{ts}.parquet")
                        bucket_name = "hhs-cms-mdrng-mtfdm-dev-mfr"

                        s3_client = boto3.client('s3')
                        s3_client.upload_file(f"/tmp/{mfg_name_value}.{ts}.parquet", bucket_name, file_path)
                        job_id = get_GlueJob_id()
                        meta_info.append([job_id,"MRN",f"{mfg_name_value}.{ts}.parquet",id_value,df.shape[0],"COMPLETED",-1])
                        df.columns = df.columns.str.lower()
                        if df_final.empty:
                            df_final = df
                        else:
                            df_final = pd.concat([df_final,df])
            elif key == "update":
                df_final["update_ts"] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                df_final["update_user_id"] = -1
                #df_final['received_dt']= df_final['received_dt'].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%d'))
                df_final['process_dt']= df_final['process_dt'].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%d'))
                claim_id = df_final[['update_ts','update_user_id','mtf_icn','process_dt']].values.tolist()
                postgres_query(database,schema,table,id = claim_id, action="update")
            elif key == "insert":
                postgres_query(database,schema,table,action="insert", dat=df_final)
            elif key == "meta":
                meta_cols = ["job_run_id","claim_file_type_cd","claim_file_name","mfr_id","file_rec_cnt","claim_file_stus_cd","insert_user_id"]
                meta_df = pd.DataFrame(meta_info, columns=meta_cols)
                postgres_query(database,schema,table,action="meta", dat=meta_df)
    else:
        break


job.commit()
