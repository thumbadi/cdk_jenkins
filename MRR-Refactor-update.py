import sys
import subprocess
import boto3
import datetime
import logging
from botocore.exceptions import NoCredentialsError
import json
from functools import reduce
# from awsglue.transforms import *
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
import pyarrow as pa
from datetime import datetime
import pytz


# Create a Glue client
logger = logging.getLogger()
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)
glue_client = boto3.client('glue')
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'MTF_DBNAME', 'S3_BUCKET', 'ENV'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

query_mfr_read = f"""
        (select distinct a.mfr_id, b.mfr_ref_id, a.drug_id, b.mfr_name from shared.drug_dtl a left join shared.mfr_dtl b on a.mfr_id = b.mfr_id  
            order by a.mfr_id) """

query_087 = f"""(select c.internal_claim_num as "MTF_ICN", c.xref_internal_claim_num as "MTF_XREF_ICN", c.received_dt as "RECEIVED_DT", c.mrn_process_dt as "PROCESS_DT",
    '200' as "TRANSACTION_CD", c.medicare_src_of_coverage as "MEDICARE_SRC_OF_COVERAGE", c.srvc_dt as "SRVC_DT",
    c.rx_srvc_ref_num as "RX_SRVC_REF_NUM", coalesce(c.fill_num,'0') as "FILL_NUM", '01' as "SRVC_PRVDR_ID_QUALIFIER", c.srvc_npi_num::varchar as "SRVC_PRVDR_ID",
    c.prescriber_id as "PRESCRIBER_ID", c.ndc_cd as "NDC_CD", c.drug_id as "DRUG_ID", c.quantity_dispensed as "QUANTITY_DISPENSED",
    c.days_supply  as "DAYS_SUPPLY", c.indicator_340b_yn as "SEC_340B_IND", c.orig_submitting_contract_num as "SUBMT_CONTRACT", c.mtf_claim_curr_loctn_cd as "LOCATION_CD", b.wac_amt as "WAC", b.mfp_amt as "MFP",
    b.sdra_amt as "SDRA", case when a.pymt_pref = 'Check' then 'CHK' when a.pymt_pref = 'ELEC' then 'ACH' when a.pymt_pref = 'ELCTRNC' then 'ACH'  when a.pymt_pref = 'EFT' then 'ACH' else null end as "SRVC_PRVDR_PYMT_PREF", b.prev_ndc_cd as "PREV_NDC_CD", b.prev_rfnd_amt as "PREV_PYMT_AMT", b.prev_rfnd_pymt_dt as "PREV_PYMT_DT", b.prev_quantity_dispensed as "PREV_PYMT_QUANTITY", b.prev_rfnd_calc_mthd_cd as "PREV_PYMT_MTHD_CD",
    e.mra_error_cd_1 as "MRA_ERR_CD_1", e.mra_error_cd_2 as "MRA_ERR_CD_2", e.mra_error_cd_3 as "MRA_ERR_CD_3", e.mra_error_cd_4 as "MRA_ERR_CD_4", e.mra_error_cd_5 as "MRA_ERR_CD_5",
    e.mra_error_cd_6 as "MRA_ERR_CD_6", e.mra_error_cd_7 as "MRA_ERR_CD_7", e.mra_error_cd_8 as "MRA_ERR_CD_8", e.mra_error_cd_9 as "MRA_ERR_CD_9", e.mra_error_cd_10 as "MRA_ERR_CD_10",
    e.mra_mtfpm_yn as "MTF_PM_IND", e.mra_rfnd_calc_mthd_cd as "PYMT_MTHD_CD", e.mra_rfnd_amt as "PYMT_AMT", e.mra_rfnd_dt as "PYMT_RPT_DT", e.mra_rfnd_ts as "PYMT_RPT_TIME", e.mra_received_dt as "MRA_RECEIPT_DT", 
    CURRENT_DATE::DATE as "CLAIM_FINAL_DT", CAST(NULL AS NUMERIC(11,2)) as "PYMT_NET_AMT", CAST(NULL AS NUMERIC(11,2)) as "PYMT_CREDIT", CAST(NULL AS NUMERIC(11,2)) as "CREDIT_BAL", CAST(NULL AS NUMERIC(11,2)) as "PYMT_MONIES", null as "EFT_NUM", CAST(NULL AS DATE) as "EFT_DT", CAST(NULL AS DATE) as "PYMT_PM_DT", CAST(NULL AS integer) as "SORT_KEY", d.mfr_id as "MANUFACTURER_ID", h.mfr_ref_id as "MFR_REF_ID", d.mfr_name as "MANUFACTURER_NAME", b.received_id as "RECEIVED_ID"
    from claim.mtf_claim c join claim.mtf_claim_manufacturer d on d.received_dt = c.received_dt and d.received_id = c.received_id
         join shared.mfr_dtl h on h.mfr_id = d.mfr_id::bigint
         join claim.mtf_claim_de_tpse a on a.received_dt = c.received_dt and a.received_id = c.received_id
         join claim.mtf_claim_pricing b on b.received_dt = c.received_dt and b.received_id = c.received_id
         join claim.mtf_claim_mra e on e.claim_received_dt = c.received_dt and e.claim_received_id = c.received_id
                   and (e.mra_received_dt, e.mra_received_id) in (select max(mra_received_dt), max(mra_received_id) from claim.mtf_claim_mra f where e.claim_received_dt = f.claim_received_dt and e.claim_received_id = f.claim_received_id)
                   where c.mtf_claim_curr_loctn_cd = '087' AND array_position(ARRAY[
        c.mrn_process_dt::text,
        c.internal_claim_num::text,
        c.medicare_src_of_coverage::text,
        c.srvc_dt::text,
        c.rx_srvc_ref_num::text,
        c.fill_num::text,
        c.srvc_npi_num::text,
        c.prescriber_id::text,
        c.ndc_cd::text,
        c.drug_id::text,
        c.quantity_dispensed::text,
        c.days_supply::text,
        c.orig_submitting_contract_num::text,
        b.mfp_amt::text,
        e.mra_mtfpm_yn::text,
        e.mra_rfnd_calc_mthd_cd::text,
        e.mra_rfnd_amt::text,
        e.mra_received_dt::text
      ], NULL) IS NULL)
                            """
query_083 = f"""(select c.internal_claim_num as "MTF_ICN", c.xref_internal_claim_num as "MTF_XREF_ICN", c.received_dt as "RECEIVED_DT", c.mrn_process_dt as "PROCESS_DT",
    '200' as "TRANSACTION_CD", c.medicare_src_of_coverage as "MEDICARE_SRC_OF_COVERAGE", c.srvc_dt as "SRVC_DT",
    c.rx_srvc_ref_num as "RX_SRVC_REF_NUM", coalesce(c.fill_num,'0') as "FILL_NUM", '01' as "SRVC_PRVDR_ID_QUALIFIER", c.srvc_npi_num::varchar as "SRVC_PRVDR_ID",
    c.prescriber_id as "PRESCRIBER_ID", c.ndc_cd as "NDC_CD", c.drug_id as "DRUG_ID", c.quantity_dispensed as "QUANTITY_DISPENSED",
    c.days_supply  as "DAYS_SUPPLY", c.indicator_340b_yn as "SEC_340B_IND", c.orig_submitting_contract_num as "SUBMT_CONTRACT", c.mtf_claim_curr_loctn_cd as "LOCATION_CD", b.wac_amt as "WAC", b.mfp_amt as "MFP",
    b.sdra_amt as "SDRA", case when a.pymt_pref = 'Check' then 'CHK' when a.pymt_pref = 'ELEC' then 'ACH' when a.pymt_pref = 'ELCTRNC' then 'ACH'  when a.pymt_pref = 'EFT' then 'ACH' else null end as "SRVC_PRVDR_PYMT_PREF", b.prev_ndc_cd as "PREV_NDC_CD", b.prev_rfnd_amt as "PREV_PYMT_AMT", b.prev_rfnd_pymt_dt as "PREV_PYMT_DT", b.prev_quantity_dispensed as "PREV_PYMT_QUANTITY", b.prev_rfnd_calc_mthd_cd as "PREV_PYMT_MTHD_CD",
    e.mra_error_cd_1 as "MRA_ERR_CD_1", e.mra_error_cd_2 as "MRA_ERR_CD_2", e.mra_error_cd_3 as "MRA_ERR_CD_3", e.mra_error_cd_4 as "MRA_ERR_CD_4", e.mra_error_cd_5 as "MRA_ERR_CD_5",
    e.mra_error_cd_6 as "MRA_ERR_CD_6", e.mra_error_cd_7 as "MRA_ERR_CD_7", e.mra_error_cd_8 as "MRA_ERR_CD_8", e.mra_error_cd_9 as "MRA_ERR_CD_9", e.mra_error_cd_10 as "MRA_ERR_CD_10",
    e.mra_mtfpm_yn as "MTF_PM_IND", e.mra_rfnd_calc_mthd_cd as "PYMT_MTHD_CD", e.mra_rfnd_amt as "PYMT_AMT", e.mra_rfnd_dt as "PYMT_RPT_DT", e.mra_rfnd_ts as "PYMT_RPT_TIME", e.mra_received_dt as "MRA_RECEIPT_DT", 
    CURRENT_DATE::DATE as "CLAIM_FINAL_DT", g.paid_amt as "PYMT_NET_AMT", g.credit_applied as "PYMT_CREDIT", g.credit_bal as "CREDIT_BAL", g.monies_paid as "PYMT_MONIES", g.mfr_820_trace_num as "EFT_NUM", g.mfr_820_pull_dt as "EFT_DT", g.pymt_stus_dt as "PYMT_PM_DT", g.sort_key as "SORT_KEY", d.mfr_id as "MANUFACTURER_ID", h.mfr_ref_id as "MFR_REF_ID", d.mfr_name as "MANUFACTURER_NAME", b.received_id as "RECEIVED_ID"
    from claim.mtf_claim c join claim.mtf_claim_manufacturer d on d.received_dt = c.received_dt and d.received_id = c.received_id
         join shared.mfr_dtl h on h.mfr_id = d.mfr_id::bigint
         join claim.mtf_claim_de_tpse a on a.received_dt = c.received_dt and a.received_id = c.received_id
         join claim.mtf_claim_pricing b on b.received_dt = c.received_dt and b.received_id = c.received_id
         join claim.mtf_claim_pymt_response g on g.claim_received_dt = c.received_dt and g.claim_received_id = c.received_id
         join claim.mtf_claim_mra e on e.claim_received_dt = c.received_dt and e.claim_received_id = c.received_id
                   and (e.mra_received_dt, e.mra_received_id) in (select max(mra_received_dt), max(mra_received_id) from claim.mtf_claim_mra f where e.claim_received_dt = f.claim_received_dt and e.claim_received_id = f.claim_received_id)
                   where c.mtf_claim_curr_loctn_cd = '083' AND array_position(ARRAY[
        c.mrn_process_dt::text,
        c.internal_claim_num::text,
        c.medicare_src_of_coverage::text,
        c.srvc_dt::text,
        c.rx_srvc_ref_num::text,
        c.fill_num::text,
        c.srvc_npi_num::text,
        c.prescriber_id::text,
        c.ndc_cd::text,
        c.drug_id::text,
        c.quantity_dispensed::text,
        c.days_supply::text,
        c.orig_submitting_contract_num::text,
        b.mfp_amt::text,
        e.mra_mtfpm_yn::text,
        e.mra_rfnd_calc_mthd_cd::text,
        e.mra_rfnd_amt::text,
        e.mra_received_dt::text,
        g.sort_key::text
      ], NULL) IS NULL)
                            """
query_loctn = f"""(select claim_msg_cd, claim_loctn_cd from claim.mtf_claim_msg_loctn_map)"""

def set_claim_location(icn_list, msg_code_list, conn):
    if not icn_list:
        logger.info("No ICNs to update; skipping stored procedure call.")
        return
    else:
        logger.info(f"Setting claim location for {len(icn_list)} ICNs with message codes: {msg_code_list}")
        with conn.cursor() as cursor:
            cursor.execute("CALL claim.update_claim_msg_loctn(%s::text[], %s::text[])", (icn_list, msg_code_list))

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
    response = glue_client.get_job_runs(JobName=args['JOB_NAME'])
    latest_job_run_id = response['JobRuns'][0]['Id'] if response['JobRuns'] else 'UNKNOWN'
    return latest_job_run_id

def postgres_query(cursor,query):
    cursor.execute(query)
    rows = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(rows, columns=colnames)
    return df


def main():
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

    mrr="MRR_"
    df_final = pd.DataFrame()
    stage_error = False
    mfg_result = None
    null_query_output = False
    job_id = None
    meta_info = []
    utc_now = datetime.utcnow()
    eastern = pytz.timezone('America/New_York')
    utc_minus_4 = pytz.utc.localize(utc_now).astimezone(eastern)

    conn = psycopg2.connect(
        dbname=mtf_db,
        user=mtf_secret['username'],
        password=mtf_secret['password'],
        host=host,
        port=port
    )
    cursor = conn.cursor()
    mfr_dtl = postgres_query(cursor,query_mfr_read)
    mfr_items = (mfr_dtl.select("mfr_name", "drug_id")
        .distinct().rdd.map(lambda row: f"{row['mfr_name']}_{row['drug_id']}")
        .distinct().collect())
    mfr_codes = {f"{row['mfr_name']}_{row['drug_id']}": {"mfr_id" : row["mfr_id"], "drug_id" : row['drug_id'], "mfr_ref_id" : row['mfr_ref_id']} for row in mfr_dtl.collect()}
    
    seq = ["query_083", "query_087"]
    for query in seq:
        tmp_mfg_result = postgres_query(cursor,query)

        if tmp_mfg_result.count() != 0:
            mfg_result = tmp_mfg_result if start == False else mfg_result.union(tmp_mfg_result)
            start = True
            null_query_output = False
        else:
            null_query_output = True
            logger.info('No Parquet files generated.')
            
    if mfg_result is not None:
        df_cols = mfg_result.columns
        df_mfr_names = mfg_result.select("MANUFACTURER_NAME", "DRUG_ID").distinct()
        exist_mfr_set = set(f"{row['MANUFACTURER_NAME']}_{row['DRUG_ID']}" for row in df_mfr_names.select("MANUFACTURER_NAME", "DRUG_ID").distinct().collect())
        missing_mfr_names = set(mfr_items) - exist_mfr_set
        mfr_list = [row for row in mfg_result.select("MANUFACTURER_ID", "MFR_REF_ID", "MANUFACTURER_NAME", "DRUG_ID").distinct().collect()]
        mfr_list = sorted(mfr_list, key=lambda row: int(row.MANUFACTURER_ID))
        for row in mfr_list:
            drug_value = row["DRUG_ID"]
            id_value = row["MANUFACTURER_ID"]
            mfr_ref_id = row["MFR_REF_ID"]
            mfg_name_value = row["MANUFACTURER_NAME"]
            df_filtered = mfg_result.filter((col("DRUG_ID") == drug_value) & (col("MANUFACTURER_ID") == id_value))
            ts = utc_minus_4.strftime("%Y%m%d_%H%M%S")
            insert_ts = utc_minus_4.strftime("%Y-%m-%d %H:%M:%S")
            file_name=f"{mfr_ref_id}_{drug_value}_MRR_{env}_{ts}.parquet"
            file_path = f"mfr-{id_value}/mrr/{drug_value}/{file_name}"
            df = df_filtered.toPandas()
            df["PROCESS_DT"] = df["PROCESS_DT"].ffill()
            df['PROCESS_DT'] = pd.to_datetime(df['PROCESS_DT'], errors='coerce')
            df['insert_ts'] = insert_ts
            df = df.sort_values(by="SRVC_PRVDR_ID")
            dataframevalues = df
            logger.info('Removing Columns which are not required in parquet file generation.')
            columns_to_remove = ["MANUFACTURER_ID","MANUFACTURER_NAME","RECEIVED_ID","DRUG_ID","RECEIVED_DT", "MFR_REF_ID"]
            df_parquet = df.drop(columns=columns_to_remove)
            df_parquet.columns = df_parquet.columns.str.upper()
            df_parquet.to_parquet(f"/tmp/{mrr}{mfg_name_value}.{ts}.parquet", schema=mrr_schema_icd)
            bucket_name = s3_bucket
            meta_file_size = os.path.getsize(f"/tmp/{mrr}{mfg_name_value}.{ts}.parquet")
            s3_client = boto3.client('s3')
            s3_client.upload_file(f"/tmp/{mrr}{mfg_name_value}.{ts}.parquet", bucket_name, file_path)
            job_id = get_GlueJob_id()
            meta_info.append([job_id,"005",file_name,meta_file_size,id_value,dataframevalues.shape[0],"COMPLETED",-1,insert_ts, drug_value])
            dataframevalues.columns = dataframevalues.columns.str.lower()
            if null_query_output == False and f"{mfg_name_value}_{drug_value}" not in missing_mfr_names:
                if df_final.empty:
                    df_final = dataframevalues
                else:
                    df_final = pd.concat([df_final,dataframevalues])
    
    # update section
    if null_query_output == False:
        location_df = postgres_query(cursor,query_loctn)
        location_code_row = (location_df.filter(col("claim_msg_cd").isin(["301", "310"])).select("claim_loctn_cd").first())
        location_code = location_code_row["claim_loctn_cd"] if location_code_row else None
        df_final["update_ts"] = utc_minus_4.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        df_final["update_user_id"] = -1
        df_final['claim_loctn_cd'] = location_code
        params = list(
            df_final[['claim_loctn_cd','update_ts','update_user_id','process_dt','mtf_icn','received_dt']]
            .itertuples(index=False, name=None)
        )
        set_claim_location(icn_list, msg_code_list, conn)
        logger.info("%d records are updated with mtf_claim_curr_loctn_cd in mtf_claim table.", len(params))
        
        # insert_process_msg section
        count = df_final.shape[0]
        postgres_query(jdbc_url,mtf_db,schema,table,action="insert_process_msg", dat=df_final)
        logger.info("%d records are inserted into insert_process_msg table.", count)

        # insert_process_loctn section
        count = df_final.shape[0]
        postgres_query(jdbc_url,mtf_db,schema,table,action="insert_process_loctn", dat=df_final)
        logger.info("%d records are inserted into insert_process_loctn table.", count)
    
    # meta insert section
    meta_cols = ["job_run_id","claim_file_type_cd","claim_file_name","claim_file_size","mfr_id","file_rec_cnt","claim_file_stus_cd","insert_user_id","insert_ts", "drug_id"]
    meta_df = pd.DataFrame(meta_info, columns=meta_cols)
    postgres_query(jdbc_url,mtf_db,schema,table,action="meta",dat=meta_df)
    logger.info('A transaction has been recorded in claim_file_metadata table.')

job.commit()