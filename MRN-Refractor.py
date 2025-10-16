import sys
import subprocess
import boto3
import datetime
import logging
from botocore.exceptions import NoCredentialsError
import json
from functools import reduce
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
subprocess.call([sys.executable, "-m", "pip", "install", "--user", "psycopg2-binary"])
import psycopg2
from psycopg2.extras import execute_batch
import pandas as pd
import numpy as np
import os
from urllib.parse import urlparse
import re
import pyarrow as pa
from datetime import datetime
import pytz

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Glue Client
glue_client = boto3.client('glue')
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'MTF_DBNAME', 'S3_BUCKET', 'ENV'])


def create_mra_error_temp_table(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TEMP TABLE temp_mra_error AS
       (select DISTINCT ON (e.internal_claim_num) e.internal_claim_num as "MTF_ICN", c.xref_internal_claim_num as "MTF_XREF_ICN", e.claim_received_dt as "RECEIVED_DT", c.mrn_process_dt as "PROCESS_DT",
'090' as "TRANSACTION_CD", c.medicare_src_of_coverage as "MEDICARE_SRC_OF_COVERAGE", c.srvc_dt as "SRVC_DT",
c.rx_srvc_ref_num as "RX_SRVC_REF_NUM", coalesce(c.fill_num,'0') as "FILL_NUM", '01' as "SRVC_PRVDR_ID_QUALIFIER", c.srvc_npi_num::varchar as "SRVC_PRVDR_ID",
c.prescriber_id as "PRESCRIBER_ID", c.ndc_cd as "NDC_CD", cfm.drug_id as "DRUG_ID", c.quantity_dispensed as "QUANTITY_DISPENSED",
c.days_supply  as "DAYS_SUPPLY", c.indicator_340b_yn as "SEC_340B_IND", c.orig_submitting_contract_num as "SUBMT_CONTRACT", b.wac_amt as "WAC", b.mfp_amt as "MFP",
b.sdra_amt as "SDRA", case when a.pymt_pref = 'Check' then 'CHK' when a.pymt_pref = 'ELEC' then 'ACH' when a.pymt_pref = 'ELCTRNC' then 'ACH'  when a.pymt_pref = 'EFT' then 'ACH' else null end as "SRVC_PRVDR_PYMT_PREF", b.prev_ndc_cd as "PREV_NDC_CD", 
b.prev_rfnd_amt as "PREV_PYMT_AMT", b.prev_rfnd_pymt_dt as "PREV_PYMT_DT", b.prev_quantity_dispensed as "PREV_PYMT_QUANTITY", b.prev_rfnd_calc_mthd_cd as "PREV_PYMT_MTHD_CD",
e.mra_error_cd as "MRA_ERR_CD_1", null as "MRA_ERR_CD_2", null as "MRA_ERR_CD_3", null as "MRA_ERR_CD_4", null as "MRA_ERR_CD_5",
null as "MRA_ERR_CD_6", null as "MRA_ERR_CD_7", null as "MRA_ERR_CD_8", null as "MRA_ERR_CD_9", null as "MRA_ERR_CD_10",
null as "MTF_PM_IND", null as "PYMT_MTHD_CD", null as "PYMT_AMT", null as "PYMT_RPT_DT", null as "PYMT_RPT_TIME", null as "MRA_RECEIPT_DT", null as "CLAIM_FINAL_DT", null as "PYMT_NET_AMT", null as "PYMT_CREDIT", 
null as "CREDIT_BAL", null as "PYMT_MONIES", null as "EFT_NUM", null as "EFT_DT", null as "PYMT_PM_DT", null as "SORT_KEY", 
g.mfr_id as "MANUFACTURER_ID", g.mfr_ref_id as "MFR_REF_ID", g.mfr_name as "MANUFACTURER_NAME", b.received_id as "RECEIVED_ID",
true as "KEEP_LOCATION"
from claim.mtf_claim_mra_error e 
    join claim.claim_file_metadata cfm on cfm.claim_file_metadata_id=e.claim_file_metadata_id
	left join claim.mtf_claim c on c.received_dt = e.claim_received_dt and c.received_id = e.claim_received_id 
     left join shared.mfr_dtl g on g.mfr_id = cfm.mfr_id::bigint
     left join claim.mtf_claim_de_tpse a on a.received_dt = c.received_dt and a.received_id = c.received_id
     left join  claim.mtf_claim_pricing b on b.received_dt = c.received_dt and b.received_id = c.received_id
where e.mrn_sent = false 
order by e.internal_claim_num, e.insert_ts desc)
    """)

def postgres_query(conn,**kwargs):
    seq = kwargs.get("action")
    code = kwargs.get("code")
    if seq == "mfr_read":
        query = f"""
                (select distinct a.mfr_id, b.mfr_ref_id, a.drug_id, b.mfr_name from shared.drug_dtl a left join shared.mfr_dtl b on a.mfr_id = b.mfr_id  
                order by a.mfr_id)
                """
        df = pd.read_sql_query(query, con=conn)
        return df

    elif seq == "read":
        if code == "MRN":
            query = f"""(select c.internal_claim_num as "MTF_ICN", c.xref_internal_claim_num as "MTF_XREF_ICN", c.received_dt as "RECEIVED_DT", coalesce(c.mrn_process_dt, CURRENT_DATE)::DATE as "PROCESS_DT", 
c.mrn_txn_cd as "TRANSACTION_CD", 
c.medicare_src_of_coverage as "MEDICARE_SRC_OF_COVERAGE", c.srvc_dt as "SRVC_DT", 
c.rx_srvc_ref_num as "RX_SRVC_REF_NUM", coalesce(c.fill_num,'0') as "FILL_NUM", '01' as "SRVC_PRVDR_ID_QUALIFIER", c.srvc_npi_num::varchar as "SRVC_PRVDR_ID", 
c.prescriber_id as "PRESCRIBER_ID", c.ndc_cd as "NDC_CD", c.drug_id as "DRUG_ID", c.quantity_dispensed as "QUANTITY_DISPENSED", c.days_supply  as "DAYS_SUPPLY", 
c.indicator_340b_yn as "SEC_340B_IND", c.orig_submitting_contract_num as "SUBMT_CONTRACT", b.wac_amt as "WAC", b.mfp_amt as "MFP",
b.sdra_amt as "SDRA", case when a.pymt_pref = 'Check' then 'CHK' when a.pymt_pref = 'ELEC' then 'ACH' when a.pymt_pref = 'ELCTRNC' then 'ACH'  when a.pymt_pref = 'EFT' then 'ACH' else null end as "SRVC_PRVDR_PYMT_PREF", b.prev_ndc_cd as "PREV_NDC_CD", b.prev_rfnd_amt as "PREV_PYMT_AMT", b.prev_rfnd_pymt_dt as "PREV_PYMT_DT", b.prev_quantity_dispensed as "PREV_PYMT_QUANTITY", b.prev_rfnd_calc_mthd_cd as "PREV_PYMT_MTHD_CD", null as "MRA_ERR_CD_1", null as "MRA_ERR_CD_2", null as "MRA_ERR_CD_3",
null as "MRA_ERR_CD_4", null as "MRA_ERR_CD_5", null as "MRA_ERR_CD_6", null as "MRA_ERR_CD_7", null as "MRA_ERR_CD_8", null as "MRA_ERR_CD_9", 
null as "MRA_ERR_CD_10", null as "MTF_PM_IND", null as "PYMT_MTHD_CD", null as "PYMT_AMT", 
null as "PYMT_RPT_DT", null as "PYMT_RPT_TIME", null as "MRA_RECEIPT_DT", null as "CLAIM_FINAL_DT", null as "PYMT_NET_AMT", null as "PYMT_CREDIT", null as "CREDIT_BAL", null as "PYMT_MONIES",
null as "EFT_NUM", null as "EFT_DT", null as "PYMT_PM_DT", null as "SORT_KEY", d.mfr_id as "MANUFACTURER_ID", e.mfr_ref_id as "MFR_REF_ID", d.mfr_name as "MANUFACTURER_NAME", b.received_id as "RECEIVED_ID",
false as "KEEP_LOCATION"
from claim.mtf_claim_loctn_lookup mcll join
     claim.mtf_claim c on c.received_dt=mcll.received_dt and c.received_id=mcll.received_id
     join  claim.mtf_claim_manufacturer d on d.received_dt = c.received_dt and d.received_id = c.received_id
	 full outer join claim.mtf_claim_de_tpse a on a.received_dt = c.received_dt and a.received_id = c.received_id
     full outer join  claim.mtf_claim_pricing b on b.received_dt = c.received_dt and b.received_id = c.received_id
     join shared.mfr_dtl e on e.mfr_id = d.mfr_id::bigint
where mcll.mtf_claim_curr_loctn_cd = '011')
                            """
        elif code == "RAF":
            query = f"""
                    (select c.internal_claim_num as "MTF_ICN", c.xref_internal_claim_num as "MTF_XREF_ICN", c.received_dt as "RECEIVED_DT", c.mrn_process_dt as "PROCESS_DT",
'090' as "TRANSACTION_CD", c.medicare_src_of_coverage as "MEDICARE_SRC_OF_COVERAGE", c.srvc_dt as "SRVC_DT",
c.rx_srvc_ref_num as "RX_SRVC_REF_NUM", coalesce(c.fill_num,'0') as "FILL_NUM", '01' as "SRVC_PRVDR_ID_QUALIFIER", c.srvc_npi_num::varchar as "SRVC_PRVDR_ID",
c.prescriber_id as "PRESCRIBER_ID", c.ndc_cd as "NDC_CD", c.drug_id as "DRUG_ID", c.quantity_dispensed as "QUANTITY_DISPENSED",
c.days_supply  as "DAYS_SUPPLY", c.indicator_340b_yn as "SEC_340B_IND", c.orig_submitting_contract_num as "SUBMT_CONTRACT", b.wac_amt as "WAC", b.mfp_amt as "MFP",
b.sdra_amt as "SDRA", case when a.pymt_pref = 'Check' then 'CHK' when a.pymt_pref = 'ELEC' then 'ACH' when a.pymt_pref = 'ELCTRNC' then 'ACH'  when a.pymt_pref = 'EFT' then 'ACH' else null end as "SRVC_PRVDR_PYMT_PREF", b.prev_ndc_cd as "PREV_NDC_CD", b.prev_rfnd_amt as "PREV_PYMT_AMT", b.prev_rfnd_pymt_dt as "PREV_PYMT_DT", b.prev_quantity_dispensed as "PREV_PYMT_QUANTITY", b.prev_rfnd_calc_mthd_cd as "PREV_PYMT_MTHD_CD",
e.mra_error_cd_1 as "MRA_ERR_CD_1", e.mra_error_cd_2 as "MRA_ERR_CD_2", e.mra_error_cd_3 as "MRA_ERR_CD_3", e.mra_error_cd_4 as "MRA_ERR_CD_4", e.mra_error_cd_5 as "MRA_ERR_CD_5",
e.mra_error_cd_6 as "MRA_ERR_CD_6", e.mra_error_cd_7 as "MRA_ERR_CD_7", e.mra_error_cd_8 as "MRA_ERR_CD_8", e.mra_error_cd_9 as "MRA_ERR_CD_9", e.mra_error_cd_10 as "MRA_ERR_CD_10",
null as "MTF_PM_IND", null as "PYMT_MTHD_CD", null as "PYMT_AMT", null as "PYMT_RPT_DT", null as "PYMT_RPT_TIME", null as "MRA_RECEIPT_DT", null as "CLAIM_FINAL_DT", null as "PYMT_NET_AMT", null as "PYMT_CREDIT", null as "CREDIT_BAL", null as "PYMT_MONIES", null as "EFT_NUM", null as "EFT_DT", null as "PYMT_PM_DT", null as "SORT_KEY", d.mfr_id as "MANUFACTURER_ID", g.mfr_ref_id as "MFR_REF_ID", d.mfr_name as "MANUFACTURER_NAME", b.received_id as "RECEIVED_ID",
false as "KEEP_LOCATION"
from claim.mtf_claim c join claim.mtf_claim_manufacturer d on d.received_dt = c.received_dt and d.received_id = c.received_id
     join shared.mfr_dtl g on g.mfr_id = d.mfr_id::bigint
     full outer join claim.mtf_claim_de_tpse a on a.received_dt = c.received_dt and a.received_id = c.received_id
     full outer join  claim.mtf_claim_pricing b on b.received_dt = c.received_dt and b.received_id = c.received_id
     join  claim.mtf_claim_mra e on e.claim_received_dt = c.received_dt and e.claim_received_id = c.received_id
               and (e.mra_received_dt, e.mra_received_id) in (select max(mra_received_dt), max(mra_received_id) from claim.mtf_claim_mra f where e.claim_received_dt = f.claim_received_dt and e.claim_received_id = f.claim_received_id)
where c.mtf_claim_curr_loctn_cd = '012')
                                """
        elif code == "MRAERR":
            query = f"""
select * from temp_mra_error
                     """            
        df = pd.read_sql_query(query, con=conn)
        return df
        
    elif seq == "meta":
        df = kwargs.get("dat")
        insert_query = f"""
            INSERT INTO claim.claim_file_metadata (job_run_id, claim_file_type_cd, claim_file_name, claim_file_size, mfr_id, file_rec_cnt, claim_file_stus_cd, insert_user_id, insert_ts, drug_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor = conn.cursor()
        cursor.executemany(insert_query, df.values.tolist())
        conn.commit()

def set_claim_location(icn_list, msg_code_list, conn):
    if not icn_list:
        logger.info("No ICNs to update; skipping stored procedure call.")
        return
    else:
        logger.info(f"Setting claim location for {len(icn_list)} ICNs with message codes: {msg_code_list}")
        with conn.cursor() as cursor:
            cursor.execute("CALL claim.update_claim_msg_loctn(%s::text[], %s::text[])", (icn_list, msg_code_list))


def send_messages_to_sqs(sqs, queue_url, claim_messages):
    """
        Send a message to the specified SQS queue.
    """

    try:
        response = sqs.send_message_batch(
            QueueUrl=queue_url,
            Entries=claim_messages
        )
        logger.info(f"{len(claim_messages)} Messages sent to SQS queue successfully.  Batch Message IDs: {[msg['MessageId'] for msg in response['Successful']]}")
    except Exception as e:
        logger.error(f"An error occurred while sending the message to SQS: {e}")

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

def update_mrn_process_dt(id_list, conn):
    from psycopg2.extras import execute_batch
    if id_list:
        # Only update the mrn_process_dt if it is currently NULL - we want to preserve the original date
        query = "UPDATE claim.mtf_claim SET update_ts = CURRENT_TIMESTAMP, mrn_process_dt = coalesce(mrn_process_dt, CURRENT_DATE) " \
        "        WHERE received_dt = %s AND internal_claim_num = %s "
        pairs = [(mtf_icn, received_dt) for (mtf_icn, received_dt) in id_list]
        execute_batch(conn.cursor(), query, pairs, page_size=5000)  # same query & tuples


mrn_schema_icd = pa.schema([
    pa.field('MTF_ICN', pa.string(), nullable=False),
    pa.field('MTF_XREF_ICN', pa.string(), nullable=True),
    pa.field('PROCESS_DT', pa.date64(), nullable=False),
    pa.field('TRANSACTION_CD', pa.string(), nullable=False),
    pa.field('MEDICARE_SRC_OF_COVERAGE', pa.string(), nullable=False),
    pa.field('SRVC_DT', pa.date64(), nullable=False),
    pa.field('RX_SRVC_REF_NUM', pa.string(), nullable=False),
    pa.field('FILL_NUM', pa.int8(), nullable=False),
    pa.field('SRVC_PRVDR_ID_QUALIFIER', pa.string(), nullable=False),
    pa.field('SRVC_PRVDR_ID', pa.string(), nullable=False),
    pa.field('PRESCRIBER_ID', pa.string(), nullable=False),
    pa.field('NDC_CD', pa.string(), nullable=False),
    pa.field('QUANTITY_DISPENSED', pa.decimal128(10,3), nullable=False),
    pa.field('DAYS_SUPPLY', pa.int64(), nullable=False),
    pa.field('SEC_340B_IND', pa.bool_(), nullable=True),
    pa.field('SUBMT_CONTRACT', pa.string(), nullable=False),
    pa.field('WAC', pa.decimal128(13,5), nullable=True),
    pa.field('MFP', pa.decimal128(14,6), nullable=False),
    pa.field('SDRA', pa.decimal128(11,2), nullable=True),
    pa.field('SRVC_PRVDR_PYMT_PREF', pa.string(), nullable=True),
    pa.field('PREV_NDC_CD', pa.string(), nullable=True),
    pa.field('PREV_PYMT_AMT', pa.decimal128(11,2), nullable=True),
    pa.field('PREV_PYMT_DT', pa.date64(), nullable=True),
    pa.field('PREV_PYMT_QUANTITY', pa.decimal128(10,3), nullable=True),
    pa.field('PREV_PYMT_MTHD_CD', pa.string(), nullable=True),
    pa.field('MRA_ERR_CD_1', pa.string(), nullable=True),
    pa.field('MRA_ERR_CD_2', pa.string(), nullable=True),
    pa.field('MRA_ERR_CD_3', pa.string(), nullable=True),
    pa.field('MRA_ERR_CD_4', pa.string(), nullable=True),
    pa.field('MRA_ERR_CD_5', pa.string(), nullable=True),
    pa.field('MRA_ERR_CD_6', pa.string(), nullable=True),
    pa.field('MRA_ERR_CD_7', pa.string(), nullable=True),
    pa.field('MRA_ERR_CD_8', pa.string(), nullable=True),
    pa.field('MRA_ERR_CD_9', pa.string(), nullable=True),
    pa.field('MRA_ERR_CD_10', pa.string(), nullable=True),
    pa.field('MTF_PM_IND', pa.bool_(), nullable=True),
    pa.field('PYMT_MTHD_CD', pa.string(), nullable=True),
    pa.field('PYMT_AMT', pa.decimal128(11,2), nullable=True),
    pa.field('PYMT_RPT_DT', pa.date64(), nullable=True),
    pa.field('PYMT_RPT_TIME', pa.timestamp('us', tz='America/New_York'), nullable=True),
    pa.field('MRA_RECEIPT_DT', pa.date64(), nullable=True),
    pa.field('CLAIM_FINAL_DT', pa.date64(), nullable=True),
    pa.field('PYMT_NET_AMT', pa.decimal128(11,2), nullable=True),
    pa.field('PYMT_CREDIT', pa.decimal128(11,2), nullable=True),
    pa.field('CREDIT_BAL', pa.decimal128(11,2), nullable=True),
    pa.field('PYMT_MONIES', pa.decimal128(11,2), nullable=True),
    pa.field('EFT_NUM', pa.string(), nullable=True),
    pa.field('EFT_DT', pa.date64(), nullable=True),
    pa.field('PYMT_PM_DT', pa.date64(), nullable=True),
    pa.field('SORT_KEY', pa.int8(), nullable=True)
    #pa.field('PYMT_TS', pa.timestamp('s', tz='America/New_York'), nullable=True)
])

mrn_schema_null = pa.schema([
    pa.field('MTF_ICN', pa.string(), nullable=False),
    pa.field('MTF_XREF_ICN', pa.string(), nullable=False),
    pa.field('PROCESS_DT', pa.date64(), nullable=False),
    pa.field('TRANSACTION_CD', pa.string(), nullable=False),
    pa.field('MEDICARE_SRC_OF_COVERAGE', pa.string(), nullable=True),
    pa.field('SRVC_DT', pa.date64(), nullable=True),
    pa.field('RX_SRVC_REF_NUM', pa.string(), nullable=True),
    pa.field('FILL_NUM', pa.int32(), nullable=True),
    pa.field('SRVC_PRVDR_ID_QUALIFIER', pa.string(), nullable=True),
    pa.field('SRVC_PRVDR_ID', pa.string(), nullable=True),
    pa.field('PRESCRIBER_ID', pa.string(), nullable=True),
    pa.field('NDC_CD', pa.string(), nullable=True),
    pa.field('QUANTITY_DISPENSED', pa.decimal128(10,3), nullable=True),
    pa.field('DAYS_SUPPLY', pa.int64(), nullable=True),
    pa.field('SEC_340B_IND', pa.bool_(), nullable=True),
    pa.field('SUBMT_CONTRACT', pa.string(), nullable=True),
    pa.field('WAC', pa.decimal128(13,5), nullable=True),
    pa.field('MFP', pa.decimal128(14,6), nullable=True),
    pa.field('SDRA', pa.decimal128(11,2), nullable=True),
    pa.field('SRVC_PRVDR_PYMT_PREF', pa.string(), nullable=True),
    pa.field('PREV_NDC_CD', pa.string(), nullable=True),
    pa.field('PREV_PYMT_AMT', pa.decimal128(11,2), nullable=True),
    pa.field('PREV_PYMT_DT', pa.date64(), nullable=True),
    pa.field('PREV_PYMT_QUANTITY', pa.decimal128(10,3), nullable=True),
    pa.field('PREV_PYMT_MTHD_CD', pa.string(), nullable=True),
    pa.field('MRA_ERR_CD_1', pa.string(), nullable=True),
    pa.field('MRA_ERR_CD_2', pa.string(), nullable=True),
    pa.field('MRA_ERR_CD_3', pa.string(), nullable=True),
    pa.field('MRA_ERR_CD_4', pa.string(), nullable=True),
    pa.field('MRA_ERR_CD_5', pa.string(), nullable=True),
    pa.field('MRA_ERR_CD_6', pa.string(), nullable=True),
    pa.field('MRA_ERR_CD_7', pa.string(), nullable=True),
    pa.field('MRA_ERR_CD_8', pa.string(), nullable=True),
    pa.field('MRA_ERR_CD_9', pa.string(), nullable=True),
    pa.field('MRA_ERR_CD_10', pa.string(), nullable=True),
    pa.field('MTF_PM_IND', pa.bool_(), nullable=True),
    pa.field('PYMT_MTHD_CD', pa.string(), nullable=True),
    pa.field('PYMT_AMT', pa.decimal128(11,2), nullable=True),
    pa.field('PYMT_RPT_DT', pa.date64(), nullable=True),
    pa.field('PYMT_RPT_TIME', pa.timestamp('us', tz='America/New_York'), nullable=True),
    pa.field('MRA_RECEIPT_DT', pa.date64(), nullable=True),
    pa.field('CLAIM_FINAL_DT', pa.date64(), nullable=True),
    pa.field('PYMT_NET_AMT', pa.decimal128(11,2), nullable=True),
    pa.field('PYMT_CREDIT', pa.decimal128(11,2), nullable=True),
    pa.field('CREDIT_BAL', pa.decimal128(11,2), nullable=True),
    pa.field('PYMT_MONIES', pa.decimal128(11,2), nullable=True),
    pa.field('EFT_NUM', pa.string(), nullable=True),
    pa.field('EFT_DT', pa.date64(), nullable=True),
    pa.field('PYMT_PM_DT', pa.date64(), nullable=True),
    pa.field('SORT_KEY', pa.int8(), nullable=True)
    #pa.field('PYMT_TS', pa.timestamp('s', tz='America/New_York'), nullable=True)
])

# Messages codes for moving messages
MSG_CODES_MRA_NOT_NEEDED = "052"
MSG_CODES_MRN_SENT = "051"

#SQS Client
queue_url = args['SQS_QUEUE_URL'] # https://sqs.us-east-1.amazonaws.com/194722421913/mtfdm-mtfdm-claimres-queue.fifo
mtf_connection = glue_client.get_connection(Name='MTFDMDataConnector')
mtf_connection_options = mtf_connection['Connection']['ConnectionProperties']
jdbc_url = mtf_connection['Connection']['ConnectionProperties']['JDBC_CONNECTION_URL']
mtf_secret=get_secret(mtf_connection_options['SECRET_ID'])
mtf_db=args['MTF_DBNAME']
s3_bucket=args['S3_BUCKET']
env=args['ENV']
parsed=urlparse(jdbc_url.replace("jdbc:", ""))
port=parsed.port

# Options for Pandas display
pd.set_option('display.max_rows', 10)   # Show 10 rows
pd.set_option('display.max_columns', None) # Show all columns
pd.set_option('display.width', None)      # No line wrapping
host=None
pattern = r"([\w.-]+\.rds\.amazonaws\.com)"
match = re.search(pattern, jdbc_url)
if match:
    host= match.group(1)
# Establish a connection to the PostgreSQL database
conn = psycopg2.connect(
        dbname=mtf_db,
        user=mtf_secret['username'],
        password=mtf_secret['password'],
        host=host,
        port=port
        )

mrn="MRN_"
df_final = pd.DataFrame()
stage_error = False
mfg_result = None
null_query_output = False
job_id = None
utc_now = datetime.utcnow()
eastern = pytz.timezone('America/New_York')
utc_minus_4 = pytz.utc.localize(utc_now).astimezone(eastern)
    
start = False
# MFR_READ KEY Block
mfr_dtl = postgres_query(conn,action="mfr_read")
#mfr_items = mfr_dtl.select("mfr_name").distinct().rdd.map(lambda row: row["mfr_name"]).collect()
mfr_items = (
    mfr_dtl[["mfr_name", "drug_id"]]           # select columns
    .drop_duplicates()                        # unique rows
    .apply(lambda row: f"{row['mfr_name']}_{row['drug_id']}", axis=1)  # map to string
    .drop_duplicates()                        # unique strings
    .tolist()                                 # convert to list
)
mfr_codes = {
    f"{row['mfr_name']}_{row['drug_id']}": {
        "mfr_id": row["mfr_id"],
        "drug_id": row["drug_id"],
        "mfr_ref_id": row["mfr_ref_id"]
    }
    for row in mfr_dtl[["mfr_name", "drug_id", "mfr_id", "mfr_ref_id"]].drop_duplicates().to_dict('records')
}

# TRANSACTION CODES KEY Block
txn_codes = postgres_query(conn,action="trans_codes")
# filter out transactions where mra_needed is true
txn_codes = txn_codes[txn_codes["mra_needed"] == True]

# READ KEY Block
seq = ["MRN","RAF", "MRAERR"]
for status in seq:
    tmp_mfg_result = postgres_query(conn,action="read",code = status)
    
    if (env != "prod"):
        print(tmp_mfg_result.head(10))
    
    if tmp_mfg_result.count() != 0:
        mfg_result = tmp_mfg_result if start == False else pd.concat([mfg_result, tmp_mfg_result], ignore_index=True)
        start = True

if mfg_result is not None:
    if (env != "prod"):
        print(mfg_result.head(10))

    df_cols = mfg_result.columns

    df_mfr_names = mfg_result[["MANUFACTURER_NAME", "DRUG_ID"]].drop_duplicates()
    exist_mfr_set = set(f"{row['MANUFACTURER_NAME']}_{row['DRUG_ID']}" 
                        for _, row in df_mfr_names[["MANUFACTURER_NAME", "DRUG_ID"]].drop_duplicates().iterrows())
    missing_mfr_names = set(mfr_items) - exist_mfr_set
    if len(missing_mfr_names) != 0:
        new_rows = []
        for mfrname in missing_mfr_names:
            temp_rows = [
                {
                    "MANUFACTURER_NAME": mfr.split('_')[0],
                    "MANUFACTURER_ID": value["mfr_id"],
                    "TRANSACTION_CD": '099',
                    "DRUG_ID": value["drug_id"],
                    "MFR_REF_ID": value["mfr_ref_id"]
                }
                for mfr, value in mfr_codes.items() if mfr == mfrname
            ]
            new_rows.extend(temp_rows)
        new_df = pd.DataFrame(new_rows)
else:
    null_query_output = True
    df_cols = ['MTF_ICN', 'MTF_XREF_ICN', 'RECEIVED_DT', 'PROCESS_DT', 'TRANSACTION_CD', 'MEDICARE_SRC_OF_COVERAGE', 'SRVC_DT', 'RX_SRVC_REF_NUM', 'FILL_NUM', 'SRVC_PRVDR_ID_QUALIFIER', 'SRVC_PRVDR_ID', 'PRESCRIBER_ID', 'NDC_CD', 'DRUG_ID', 'QUANTITY_DISPENSED', 'DAYS_SUPPLY', 'SEC_340B_IND', 'SUBMT_CONTRACT', 'WAC', 'MFP', 'SDRA', 'SRVC_PRVDR_PYMT_PREF', 'PREV_NDC_CD', 'PREV_PYMT_AMT', 'PREV_PYMT_DT', 'PREV_PYMT_QUANTITY', 'PREV_PYMT_MTHD_CD', 'MRA_ERR_CD_1', 'MRA_ERR_CD_2', 'MRA_ERR_CD_3', 'MRA_ERR_CD_4', 'MRA_ERR_CD_5', 'MRA_ERR_CD_6', 'MRA_ERR_CD_7', 'MRA_ERR_CD_8', 'MRA_ERR_CD_9', 'MRA_ERR_CD_10', 'MTF_PM_IND', 'PYMT_MTHD_CD', 'PYMT_AMT', 'PYMT_RPT_DT', 'PYMT_RPT_TIME', 'MRA_RECEIPT_DT', 'CLAIM_FINAL_DT', 'PYMT_NET_AMT', 'PYMT_CREDIT', 'CREDIT_BAL', 'PYMT_MONIES', 'EFT_NUM', 'EFT_DT', 'PYMT_PM_DT', 'SORT_KEY', 'PYMT_TS', 'MANUFACTURER_ID', 'MANUFACTURER_NAME', 'RECEIVED_ID', 'MFR_REF_ID']
    new_rows = [
            {"MANUFACTURER_NAME": mfr.split('_')[0], "MANUFACTURER_ID": value["mfr_id"],"TRANSACTION_CD": '099', "DRUG_ID" : value["drug_id"], "MFR_REF_ID" : value['mfr_ref_id'] }
            for mfr,value in mfr_codes.items()
        ]
    new_df = pd.DataFrame(new_rows)
if 'new_df' in locals():
    custom_cols = ['MTF_ICN','MTF_XREF_ICN']
    curr_date = datetime.now().strftime("%Y-%m-%d")
    for column in df_cols:
        if column not in new_df.columns:
            if column == 'PROCESS_DT':
                new_df[column]=curr_date
                new_df = new_df.withColumn(column, lit(curr_date))
            else:
                new_df[column] = pd.NA if column not in custom_cols else "999999999999999"
    new_df = new_df[df_cols]
    if mfg_result is not None:
        mfg_result = pd.concat([mfg_result, new_df], ignore_index=True)
    else:
        mfg_result = new_df
# Deduplicate on the selected columns (Spark .distinct())
unduplicate_manus = mfg_result[["MANUFACTURER_ID", "MFR_REF_ID", "MANUFACTURER_NAME", "DRUG_ID"]].drop_duplicates()
# Collect rows (like .collect()) as namedtuples so you can do row.MANUFACTURER_ID
mfr_list = list(unduplicate_manus.itertuples(index=False, name="Row"))
# Sort like Spark code: by MANUFACTURER_ID interpreted as int
mfr_list = sorted(mfr_list, key=lambda row: int(row.MANUFACTURER_ID))    

# Do one block at a time, one manufacturer at a time
for row in mfr_list:
    drug_value = row["DRUG_ID"]
    id_value = row["MANUFACTURER_ID"]
    mfr_ref_id = row["MFR_REF_ID"]
    mfg_name_value = row["MANUFACTURER_NAME"]
    # Limit to just this manufacturer and drug
    df_drugs_for_manu = mfg_result.query("DRUG_ID == @drug_value and MANUFACTURER_ID == @id_value")
    ts = utc_minus_4.strftime("%Y%m%d_%H%M%S")
    insert_ts = utc_minus_4.strftime("%Y-%m-%d %H:%M:%S")
    file_name=f"{mfr_ref_id}_{drug_value}_MRN_{env}_{ts}.parquet"
    file_path = f"mfr-{id_value}/mrn/{drug_value}/{file_name}"
    if (env != "prod"):
        print(df_drugs_for_manu.head(10))
        
    df = df_drugs_for_manu.copy()
    df["PROCESS_DT"] = df["PROCESS_DT"].ffill()
    df['PROCESS_DT'] = pd.to_datetime(df['PROCESS_DT'], errors='coerce')
    df['insert_ts'] = insert_ts
    df = df.sort_values(by="SRVC_PRVDR_ID")
    columns_to_remove = ["MANUFACTURER_ID","MANUFACTURER_NAME","RECEIVED_ID","DRUG_ID","RECEIVED_DT", "MFR_REF_ID", "KEEP_LOCATION"]
    df_parquet = df.drop(columns=columns_to_remove)
    df_parquet.columns = df_parquet.columns.str.upper()
    if not df[df['TRANSACTION_CD'] == '099'].empty:
        df_parquet.to_parquet(f"/tmp/{mrn}{mfg_name_value}.{ts}.parquet", schema=mrn_schema_null)
    else:
        df_parquet.to_parquet(f"/tmp/{mrn}{mfg_name_value}.{ts}.parquet", schema=mrn_schema_icd)
    bucket_name = s3_bucket
    meta_file_size = os.path.getsize(f"/tmp/{mrn}{mfg_name_value}.{ts}.parquet")
    job_id = get_GlueJob_id()
    meta_info = []
    meta_info.append([job_id,"004",file_name,meta_file_size,id_value,df.shape[0],"COMPLETED",-1,insert_ts,drug_value])

    df.columns = df.columns.str.lower()
    if null_query_output == False and f"{mfg_name_value}_{drug_value}" not in missing_mfr_names:
        # Filter out rows where KEEP_LOCATION is True - this is for the records in MRA_ERROR table
        # We want to make sure that we do not update the location for these records
        df_drugs_for_manu = df[df["KEEP_LOCATION"] == False]

        if df_final.empty:
            df_final = df_drugs_for_manu
        else:
            df_final = pd.concat([df_final, df_drugs_for_manu])   
        # Next there are 2 types of locations (1 for TXN Codes that require a MRA and one for the ones that do not)                     
        # filter out df_final when transaction_cd is in txn_codes DataFrame
        if not null_query_output and not df_drugs_for_manu.empty:
            df_mrn_sent = df_drugs_for_manu.join(
                txn_codes.select("mrn_txn_cd"), 
                df_drugs_for_manu["transaction_cd"] == col("mrn_txn_cd"), 
                "inner"
            )
            df_mrn_sent = df_mrn_sent.drop(columns=["mrn_txn_cd"])
            
            df_claims_resolution = df_drugs_for_manu.join(
                txn_codes.select("mrn_txn_cd"),
                df_drugs_for_manu["transaction_cd"] == col("mrn_txn_cd"),
                "left_anti"
            )
            df_claims_resolution = df_claims_resolution.drop(columns=["mrn_txn_cd"])

            # MRN_SENT_LOCATION KEY Block
            icns = (
                df_mrn_sent["MTF_ICN"]
                .dropna()
                .astype(str)
                .unique()
                .tolist()
            )
            msg_codes = [MSG_CODES_MRN_SENT]
            # TO DO - limit to 5000 ICNs at a time
            set_claim_location(icns, msg_codes)

            # MRA_NOT_NEEDED_LOCATION KEY Block
            icns = (
                df_claims_resolution["MTF_ICN"]
                .dropna()
                .astype(str)
                .unique()
                .tolist()
            )
            msg_codes = [MSG_CODES_MRA_NOT_NEEDED]
            set_claim_location(icns, msg_codes)

            # Update mrn_process_dt in mtf_claim table for all records sent to MRN
            id_list = list(df_drugs_for_manu[["mtf_icn", "received_dt"]].itertuples(index=False, name=None))
            if id_list:
                update_mrn_process_dt(id_list, conn)
                logger.info(f"mrn_process_dt updated for {len(id_list)} records in mtf_claim table.")


            #Now send messages to SQS queue for claims resolution
            if not df_claims_resolution.empty:
                sqs = boto3.client('sqs')
                claim_messages = []
                current_time = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
                message_group_id = f"mtfdm-mrn-claimres-group-{current_time}"
                for index, row in df_claims_resolution.iterrows():
                    message_body = {
                        "mra_received_id": -1,
                        "mra_received_dt": None,
                        "claim_received_id": row["received_id"],
                        "claim_received_dt": row["received_dt"].strftime("%Y-%m-%d"),
                        "internal_claim_num": row["mtf_icn"],
                        "claim_file_metadata_id": -1,
                    }
                    claim_messages.append({
                        'MessageBody': json.dumps(message_body),
                        'MessageGroupId': message_group_id,
                        'MessageDeduplicationId': f"{row['mtf_icn']}-{row['transaction_cd']}"
                    })
                    # Send messages in batches of 10
                    if len(claim_messages) == 10:
                        send_messages_to_sqs(sqs, queue_url, claim_messages)
                        claim_messages = []
                # Send any remaining messages
                if claim_messages:
                    send_messages_to_sqs(sqs, queue_url, claim_messages)
                logger.info(f"A total of {len(df_claims_resolution)} messages have been sent to the SQS queue.")

    # META KEY Block    
    meta_cols = ["job_run_id","claim_file_type_cd","claim_file_name","claim_file_size","mfr_id","file_rec_cnt","claim_file_stus_cd","insert_user_id","insert_ts","drug_id"]
    meta_df = pd.DataFrame(meta_info, columns=meta_cols)
    postgres_query(conn, action="meta",dat=meta_df)
    logger.info(f"Claim file metadata table updated for {mfg_name_value}.")

    # Lastly - we need to set the field mrn_sent on mra error table
    update_mra_error_table(df_drugs_for_manu, conn)

    # Commit after each manufacturer/drug file
    conn.commit()
    # Once transaction committed, upload the file to S3
    s3_client = boto3.client('s3')
    s3_client.upload_file(f"/tmp/{mrn}{mfg_name_value}.{ts}.parquet", bucket_name, file_path)
