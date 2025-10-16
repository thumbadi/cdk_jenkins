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
import pyarrow as pa
import os, re
from urllib.parse import urlparse
from datetime import datetime
import pytz

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
        logger.error(f"Error retrieving secret: {e}")
        raise e

def get_GlueJob_id():
    # Fetch latest running job
    response = glue_client.get_job_runs(JobName=args['JOB_NAME'])
    latest_job_run_id = response['JobRuns'][0]['Id'] if response['JobRuns'] else 'UNKNOWN'
    return latest_job_run_id


bankfile_schema = pa.schema([
    pa.field('RecordOperation', pa.string(), nullable=False),
    pa.field('OrganizationCode', pa.string(), nullable=False),
    pa.field('PayeeID', pa.string(), nullable=False),
    pa.field('OrganizationIdentifier', pa.string(), nullable=False),
    pa.field('OrganizationName', pa.string(), nullable=False),
    pa.field('OrganizationLegalName', pa.string(), nullable=False),
    pa.field('OrganizationTIN', pa.string(), nullable=True),
    pa.field('OrganizationTINType', pa.string(), nullable=True),
    pa.field('ProfitNonprofit', pa.string(), nullable=True),
    pa.field('OrganizationNPI', pa.string(), nullable=True),
    pa.field('PaymentMode', pa.string(), nullable=True),
    pa.field('RoutingTransitNumber', pa.string(), nullable=True),
    pa.field('AccountNumber', pa.string(), nullable=True),
    pa.field('AccountType', pa.string(), nullable=True),
    pa.field('EffectiveStartDate', pa.date64(), nullable=False),
    pa.field('EffectiveEndDate', pa.date64(), nullable=True),
    pa.field('AddressCode', pa.string(), nullable=True),
    pa.field('AddressLine1', pa.string(), nullable=True),
    pa.field('AddressLine2', pa.string(), nullable=True),
    pa.field('CityName', pa.string(), nullable=True),
    pa.field('State', pa.string(), nullable=True),
    pa.field('PostalCode', pa.string(), nullable=True),
    pa.field('ContactCode', pa.string(), nullable=True),
    pa.field('ContactFirstName', pa.string(), nullable=True),
    pa.field('ContactLastName', pa.string(), nullable=True),
    pa.field('ContactTitle', pa.string(), nullable=True),
    pa.field('ContactPhone', pa.string(), nullable=True),
    pa.field('ContactFax', pa.string(), nullable=True),
    pa.field('ContactOtherPhone', pa.string(), nullable=True),
    pa.field('ContactEmail', pa.string(), nullable=True)
])


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

    bucket_name = s3_bucket
    stage_error = False
    #df_final = pd.DataFrame()
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
        port=port,
        options="-c timezone=America/New_York"
    )
    cur = conn.cursor()
    logger.info('Database connection established.')
    logger.info('Refreshing bank file data by calling stored procedure.')

    cur.execute(f"CALL shared.refresh_bank_file_data();")  
    cur.close()

    bank_file_query = f"""(SELECT recordoperation as "RecordOperation", organizationcode as "OrganizationCode", payeeid as "PayeeID", organizationidentifier as "OrganizationIdentifier",
                    organizationname as "OrganizationName", organizationlegalname as "OrganizationLegalName", organizationtin as "OrganizationTIN",
                    organizationtintype as "OrganizationTINType", organizationnpi as "OrganizationNPI", profitnonprofit as "ProfitNonprofit", paymentmode as "PaymentMode", 
                    routingtransitnumber as "RoutingTransitNumber", accountnumber as "AccountNumber", accounttype as "AccountType", effectivestartdate as "EffectiveStartDate", 
                    effectiveenddate as "EffectiveEndDate", addresscode as "AddressCode", addressline1 as "AddressLine1",addressline2 as "AddressLine2", cityname as "CityName",
                    state as "State", postalcode as "PostalCode", contactcode as "ContactCode", contactfirstname as "ContactFirstName", contactlastname as "ContactLastName",
                    contacttitle as "ContactTitle", contactphone as "ContactPhone", contactfax as "ContactFax", contactotherphone as "ContactOtherPhone",
                    contactemail as "ContactEmail", refresh_ts FROM shared.bank_file_vw a
                    WHERE a.refresh_ts::timestamp > COALESCE((SELECT MAX(insert_ts)+interval '5 minute' FROM claim.claim_file_metadata where claim_file_type_cd = '006'),'1900-01-01'::timestamp)
                    )
                            """
    logger.info('Extracting data from mfr_bank_file_vw view.')
    
    bankfile_df = pd.read_sql(bank_file_query, con=conn)
        
    if bankfile_df.empty == False:
        columns_to_remove = ["refresh_ts"]
        df_parquet = bankfile_df.drop(columns=columns_to_remove)
        ts = utc_minus_4.strftime("%Y%m%d_%H%M%S")
        insert_ts = utc_minus_4.strftime("%Y-%m-%d %H:%M:%S")
        file_name = f"MTFDM_{env}_DMBankData_{ts}.parquet"
        file_path = f"bankfile/ready/{file_name}"
        df_parquet.to_parquet(f"/tmp/{file_name}",schema=bankfile_schema)
        meta_file_size = os.path.getsize(f"/tmp/{file_name}")
        s3_client = boto3.client('s3')
        s3_client.upload_file(f"/tmp/{file_name}", bucket_name, file_path)
        logger.info('Parquet file has been created.')
        job_id = get_GlueJob_id()
        meta_info.append([job_id,"006",file_name,meta_file_size,bankfile_df.shape[0],"COMPLETED",-1,insert_ts])
        file_meta_insert_query = """
            INSERT INTO claim.claim_file_metadata (
                job_run_id, claim_file_type_cd, claim_file_name, claim_file_size,
                file_rec_cnt, claim_file_stus_cd, insert_user_id, insert_ts
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        logger.info('Inserting metadata into claim_file_metadata table.')
        cursor = conn.cursor()
        cursor.executemany(file_meta_insert_query, meta_info)
        cursor.close()
    else:
        logger.info('No records found in mfr_bank_file_vw table.')
                
    logger.info('Job has been completed.')
    conn.commit()
    conn.close()
    job.commit()
# Entry point
if __name__ == "__main__":
    logger.info('starting bank file job')
    main()
