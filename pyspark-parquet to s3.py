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
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
subprocess.call([sys.executable, "-m", "pip", "install", "--user", "psycopg2-binary"])
import psycopg2
import pandas as pd
import numpy as np
import os
from urllib.parse import urlparse
import re

# Create a Glue client
logger = logging.getLogger()
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.DEBUG)
glue_client = boto3.client('glue')
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'MTF_DBNAME', 'S3_BUCKET', 'ENV'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def postgres_query(jdbc_url,mtf_db,schema,table,**kwargs):
    seq = kwargs.get("action")
    code = kwargs.get("code")
    if seq == "read":
        if code == "MRN":
            query = f"""(select c.internal_claim_num as "MTF_ICN", c.xref_internal_claim_num as "MTF_XREF_ICN", c.received_dt as "RECEIVED_DT", coalesce(c.mrn_process_dt, CURRENT_DATE) as "PROCESS_DT", 
case when c.src_claim_type_cd = 'O' then '01' else null end as "TRANSACTION_CD", c.medicare_src_of_coverage as "MEDICARE_SRC_OF_COVERAGE", c.srvc_dt as "SRVC_DT", 
c.rx_srvc_ref_num as "RX_SRVC_REF_NUM", coalesce(c.fill_num,'0') as "FILL_NUM", c.ncpdp_id as "NCPDP_ID", c.srvc_npi_num as "SRVC_PRVDR_ID", 
c.prescriber_id as "PRESCRIBER_ID", c.ndc_cd as "NDC_CD", (select drug_id from shared.ndc ndc where ndc.ndc_cd = c.ndc_cd) as "DRUG_ID", c.quantity_dispensed as "QUANTITY_DISPENSED", c.days_supply  as "DAYS_SUPPLY", 
c.indicator_340b_yn as "340b_INDICATOR", c.orig_submitting_contract_num as "SUBMT_CONTRACT", b.wac_amt as "WAC", b.mfp_amt as "MFP",
b.sdra_amt as "SDRA", a.pymt_pref as "SRVC_PRVDR_PYMT_PREF",
null as "PREV_NDC_CD", null as "PREV_PYMT_AMT", null as "PREV_PYMT_DT", 
null as "PREV_PYMT_QUANTITY", null as "PREV_PYMT_MTHD_CD", null as "MRA_ERR_CD_1", null as "MRA_ERR_CD_2", null as "MRA_ERR_CD_3",
null as "MRA_ERR_CD_4", null as "MRA_ERR_CD_5", null as "MRA_ERR_CD_6", null as "MRA_ERR_CD_7", null as "MRA_ERR_CD_8", null as "MRA_ERR_CD_9", 
null as "MRA_ERR_CD_10", null as "MTF_PM_IND", null as "PYMT_MTHD_CD", null as "PYMT_AMT", 
null as "PYMT_TS", d.mfr_id as "MANUFACTURER_ID", d.mfr_name as "MANUFACTURER_NAME", b.received_id as "RECEIVED_ID"
from claim.mtf_claim c join claim.mtf_claim_de_tpse a on a.received_dt = c.received_dt and a.received_id = c.received_id
     join  claim.mtf_claim_pricing b on b.received_dt = c.received_dt and b.received_id = c.received_id
     join  claim.mtf_claim_manufacturer d on d.received_dt = c.received_dt and d.received_id = c.received_id
where c.mtf_curr_claim_stus_ref_cd = 'MRN')
                            """
        elif code == "RAF":
            query = f"""
                    (select c.internal_claim_num as "MTF_ICN", c.xref_internal_claim_num as "MTF_XREF_ICN", c.received_dt as "RECEIVED_DT", coalesce(c.mrn_process_dt, CURRENT_DATE) as "PROCESS_DT",
case when c.src_claim_type_cd = 'O' then '01' else null end as "TRANSACTION_CD", c.medicare_src_of_coverage as "MEDICARE_SRC_OF_COVERAGE", c.srvc_dt as "SRVC_DT",
c.rx_srvc_ref_num as "RX_SRVC_REF_NUM", coalesce(c.fill_num,'0') as "FILL_NUM", '01' as "SRVC_PRVDR_ID_QUALIFIER", c.srvc_npi_num as "SRVC_PRVDR_ID",
c.prescriber_id as "PRESCRIBER_ID", c.ndc_cd as "NDC_CD", (select drug_id from shared.ndc ndc where ndc.ndc_cd = c.ndc_cd) as "DRUG_ID", c.quantity_dispensed as "QUANTITY_DISPENSED",
c.days_supply  as "DAYS_SUPPLY", c.indicator_340b_yn as "340b_INDICATOR", c.orig_submitting_contract_num as "SUBMT_CONTRACT", b.wac_amt as "WAC", b.mfp_amt as "MFP",
b.sdra_amt as "SDRA", a.pymt_pref as "SRVC_PRVDR_PYMT_PREF", null as "PREV_NDC_CD", null as "PREV_PYMT_AMT", null as "PREV_PYMT_DT", null as "PREV_PYMT_QUANTITY", null as "PREV_PYMT_MTHD_CD",
e.mra_error_cd_1 as "MRA_ERR_CD_1", e.mra_error_cd_2 as "MRA_ERR_CD_2", e.mra_error_cd_3 as "MRA_ERR_CD_3", e.mra_error_cd_4 as "MRA_ERR_CD_4", e.mra_error_cd_5 as "MRA_ERR_CD_5",
e.mra_error_cd_6 as "MRA_ERR_CD_6", e.mra_error_cd_7 as "MRA_ERR_CD_7", e.mra_error_cd_8 as "MRA_ERR_CD_8", e.mra_error_cd_9 as "MRA_ERR_CD_9", e.mra_error_cd_10 as "MRA_ERR_CD_10",
null as "MTF_PM_IND", null as "PYMT_MTHD_CD", null as "PYMT_AMT", null as "PYMT_TS", d.mfr_id as "MANUFACTURER_ID", d.mfr_name as "MANUFACTURER_Name", null as "RECEIVED_ID"
from claim.mtf_claim c join claim.mtf_claim_de_tpse a on a.received_dt = c.received_dt and a.received_id = c.received_id
     join  claim.mtf_claim_pricing b on b.received_dt = c.received_dt and b.received_id = c.received_id
     join  claim.mtf_claim_manufacturer d on d.received_dt = c.received_dt and d.received_id = c.received_id
     join  claim.mtf_claim_mra e on e.claim_received_dt = c.received_dt and e.claim_received_id = c.received_id
               and (e.mra_received_dt, e.mra_received_id) in (select max(mra_received_dt), max(mra_received_id) from claim.mtf_claim_mra f where e.claim_received_dt = f.claim_received_dt and e.claim_received_id = f.claim_received_id)
where c.mtf_curr_claim_stus_ref_cd = 'RAF')
                                """
        df = spark.read.format("jdbc") \
            .option("url", f"{jdbc_url}") \
            .option("dbtable", query) \
            .option("user", mtf_secret['username']) \
            .option("password", mtf_secret['password']) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        return df
    elif seq == "update":
        id_list = kwargs.get("id")

        conn = psycopg2.connect(
        dbname=mtf_db,
        user=mtf_secret['username'],
        password=mtf_secret['password'],
        host=host,
        port=port
        )
        cursor = conn.cursor()
        
        query = """
            UPDATE claim.mtf_claim \
            SET mtf_curr_claim_stus_ref_cd = 'SNT', update_ts = %s, update_user_id = %s, mrn_process_dt = %s\
            WHERE internal_claim_num = (%s) AND received_dt = %s
        """       
             
        if id_list:  # Prevent empty execution
            cursor.executemany(query,[tuple(item) for item in id_list])

        
        conn.commit()
        cursor.close()
        conn.close()

    elif seq == "insert":

        df = kwargs.get("dat")
        df = df[["received_dt", "received_id", "mtf_icn"]]
        df["mtf_claim_stus_ref_cd"] = "SNT"
        df["insert_user_id"] = -1
        insert_query = f"""
            INSERT INTO {schema}.{table} (received_dt, received_id, internal_claim_num, mtf_claim_stus_ref_cd, insert_user_id)
            VALUES (%s, %s, %s, %s, %s)
        """
        conn = psycopg2.connect(
            dbname=mtf_db, user=mtf_secret['username'], password=mtf_secret['password'], host=host, port=port
        )
        cursor = conn.cursor()
        cursor.executemany(insert_query, df.values.tolist())
        conn.commit()
        cursor.close()
        conn.close()
        
    elif seq == "meta":
        df = kwargs.get("dat")
        insert_query = f"""
            INSERT INTO {schema}.{table} (job_run_id, claim_file_type_cd, claim_file_name, claim_file_size, mfr_id, file_rec_cnt, claim_file_stus_cd, insert_user_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        conn = psycopg2.connect(
            dbname=mtf_db, user=mtf_secret['username'], password=mtf_secret['password'], host=host, port=port
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
    {"insert" : {
        "table_name" : "mtf_claim_process_status",
        "schema" : "claim"
    }},
    {"meta" : {
        "table_name" : "claim_file_metadata",
        "schema" : "claim"
    }}
    ]

mtf_connection = glue_client.get_connection(Name='MTFDMDataConnector')
mtf_connection_options = mtf_connection['Connection']['ConnectionProperties']
jdbc_url = mtf_connection['Connection']['ConnectionProperties']['JDBC_CONNECTION_URL']
mtf_secret=get_secret(mtf_connection_options['SECRET_ID'])
mtf_db=args['MTF_DBNAME']
s3_bucket=args['S3_BUCKET']
env=args['ENV']
parsed=urlparse(jdbc_url.replace("jdbc:", ""))
port=parsed.port
host=None
pattern = r"([\w.-]+\.rds\.amazonaws\.com)"
match = re.search(pattern, jdbc_url)
if match:
    host= match.group(1)

mrn="MRN_"
mfr="mfr"
df_final = pd.DataFrame()
stage_error = False
job_id = None
meta_info = []
for entry in schema_data:
    if stage_error == False:
        for key in entry.keys():        
            schema = entry[key]["schema"]
            table = entry[key]["table_name"]        
            start = False

            if key == "read":
                seq = ["MRN","RAF"]
                for cde in seq:
                    tmp_mfg_result = postgres_query(jdbc_url,mtf_db,schema,table,action="read",code = cde)
                    
                    if tmp_mfg_result.count() != 0:
                        mfg_result = tmp_mfg_result if start == False else mfg_result.union(tmp_mfg_result)
                        start = True
                if mfg_result.count() != 0:
                    mfr_list = [row for row  in mfg_result.select("MANUFACTURER_ID", "MANUFACTURER_NAME", "DRUG_ID").distinct().collect()]
                    if len(mfr_list) == 0:
                        print("No records found in the result")
                        stage_error = True
                    else:
                        for row in mfr_list:
                            drug_value = row["DRUG_ID"]
                            id_value = row["MANUFACTURER_ID"]
                            mfg_name_value = row["MANUFACTURER_NAME"]
                            s3_folder_mfr = mfg_name_value.replace(" ","_").replace("-","_").lower()
                            s3_folder_mfr_upper = s3_folder_mfr.upper()
                            df_filtered = mfg_result.filter((col("MANUFACTURER_ID") == id_value) & (col("DRUG_ID") == drug_value))
                            ts = datetime.datetime.today().strftime("%Y%m%d.%H%M%S")
                            file_name=f"{id_value}_{drug_value}_MRN_{env}_{ts}.parquet"
                            file_path = f"{mfr}-{id_value}/mrn/outbound/{id_value}_{drug_value}_MRN_{env}_{ts}.parquet"
                            df = df_filtered.toPandas()
                            df = df.sort_values(by="SRVC_PRVDR_ID")
                            columns_to_remove = ["MANUFACTURER_ID","MANUFACTURER_NAME","RECEIVED_ID","DRUG_ID","RECEIVED_DT"]
                            df_parquet = df.drop(columns=columns_to_remove)
                            df_parquet.to_parquet(f"/tmp/{mrn}{mfg_name_value}.{ts}.parquet")
                            bucket_name = s3_bucket
                            meta_file_size = os.path.getsize(f"/tmp/{mrn}{mfg_name_value}.{ts}.parquet")
                            s3_client = boto3.client('s3')
                            s3_client.upload_file(f"/tmp/{mrn}{mfg_name_value}.{ts}.parquet", bucket_name, file_path)
                            job_id = get_GlueJob_id()
                            meta_info.append([job_id,"004",file_name,meta_file_size,id_value,df.shape[0],"COMPLETED",-1])
                            df.columns = df.columns.str.lower()
                            if df_final.empty:
                                df_final = df
                            else:
                                df_final = pd.concat([df_final,df])
            elif key == "update":
                df_final["update_ts"] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                df_final["update_user_id"] = -1
                df_final['received_dt']= df_final['received_dt'].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%d'))
                df_final['process_dt']= df_final['process_dt'].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%d'))
                claim_id = df_final[['update_ts','update_user_id','process_dt','mtf_icn','received_dt']].values.tolist()
                postgres_query(jdbc_url,mtf_db,schema,table,id = claim_id, action="update")
            elif key == "insert":
                postgres_query(jdbc_url,mtf_db,schema,table,action="insert", dat=df_final)
            elif key == "meta":
                meta_cols = ["job_run_id","claim_file_type_cd","claim_file_name","claim_file_size","mfr_id","file_rec_cnt","claim_file_stus_cd","insert_user_id"]
                meta_df = pd.DataFrame(meta_info, columns=meta_cols)
                postgres_query(jdbc_url,mtf_db,schema,table,action="meta",dat=meta_df)
    else:
        break

job.commit()
