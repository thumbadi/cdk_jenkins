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
import os, re
from urllib.parse import urlparse

# logger = logging.getLogger()
# logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
# logger.setLevel(logging.DEBUG)
glue_client = boto3.client('glue')
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'MTF_DBNAME', 'S3_BUCKET', 'ENV'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def batch_update(records, db):
    """
    Function to update records in PostgreSQL using executemany().
    """
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
        SET mtf_curr_claim_stus_ref_cd = 'RCD' \
        WHERE internal_claim_num = ANY(%s)
    """
    
    data = [(row['internal_claim_num']) for row in records]
    
    if data:  # Prevent empty execution
        cursor.executemany(query, data)
    
    conn.commit()
    cursor.close()
    conn.close()


def postgres_query(jdbc_url,mtf_db,schema,table,**kwargs):
    seq = kwargs.get("action")

    if seq == "execute_sp":
        proc_name = kwargs.get("prcd")    
        conn = psycopg2.connect(
        dbname=mtf_db,
        user=mtf_secret['username'],
        password=mtf_secret['password'],
        host=host,
        port=5432
        )
        conn.autocommit = True
        cursor = conn.cursor()
        with conn:
            with conn.cursor() as cur:
                cur.execute(f"CALL {schema}.{proc_name}();")  # Python waits here until it's done
                print("Stored procedure completed successfully.")

    elif seq == "read_table":
        if table == "bank_file_vw":
            query = f"""(SELECT 'A' as "RecordOperation", organizationcode as "OrganizationCode", payeeid as "PayeeID", organizationidentifier as "OrganizationIdentifier",
                        organizationname as "OrganizationName", organizationlegalname as "OrganizationLegalName", organizationtin as "OrganizationTIN",
                        organizationtintype as "OrganizationTINType", organizationnpi as "OrganizationNPI", profitnonprofit as "ProfitNonprofit", paymentmode as "PaymentMode", 
                        routingtransitnumber as "RoutingTransitNumber", accountnumber as "AccountNumber", accounttype as "AccountType", effectivestartdate as "EffectiveStartDate", 
                        effectiveenddate as "EffectiveEndDate", addresscode as "AddressCode", addressline1 as "AddressLine1",addressline2 as "AddressLine2", cityname as "CityName",
                        state as "State", postalcode as "PostalCode", contactcode as "ContactCode", contactfirstname as "ContactFirstName", contactlastname as "ContactLastName",
                        contacttitle as "ContactTitle", contactphone as "ContactPhone", contactfax as "ContactFax", contactotherphone as "ContactOtherPhone",
                        contactemail as "ContactEmail", refresh_ts FROM shared.bank_file_vw)
                                """
        df = spark.read.format("jdbc") \
            .option("url", f"{jdbc_url}") \
            .option("dbtable", query) \
            .option("user", mtf_secret['username']) \
            .option("password", mtf_secret['password']) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        return df
    
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


schema_data =  [
    {
    "execute_sp" : {
            "table_name" : "",
            "schema" : "shared"
    }},
    {
    "read_table" : {
            "table_name" : "bank_file_vw",
            "schema" : "shared"
    }},
    {
    "meta" : {
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

bucket_name = s3_bucket
stage_error = False
#df_final = pd.DataFrame()
job_id = None
meta_info = []
for entry in schema_data:
    if stage_error == False:
        for key in entry.keys():        
            schema = entry[key]["schema"]
            table = entry[key]["table_name"]        
            if key == "read_table":
                view_detpse_df = postgres_query(jdbc_url,mtf_db,schema,table,action="read_table")
                if view_detpse_df.isEmpty() == False:
                    df = view_detpse_df.toPandas()
                    columns_to_remove = ["refresh_ts"]
                    df_parquet = df.drop(columns=columns_to_remove)
                    ts = datetime.datetime.today().strftime("%Y%m%d_%H%M%S")
                    file_name = f"MTFDM_{env}_DMBankData_{ts}.parquet"
                    file_path = f"bankfile/ready/{file_name}"
                    df_parquet.to_parquet(f"/tmp/{file_name}")
                    meta_file_size = os.path.getsize(f"/tmp/{file_name}")
                    s3_client = boto3.client('s3')
                    s3_client.upload_file(f"/tmp/{file_name}", bucket_name, file_path)
                    job_id = get_GlueJob_id()
                    meta_info.append([job_id,"006",file_name,meta_file_size,"Null",df.shape[0],"COMPLETED",-1])
                    # df.columns = df.columns.str.lower()
                else:
                    print('No records found in de_tpse_payee_dtl table.')
                    stage_error = True
            elif key == "meta" and stage_error == False:
                meta_cols = ["job_run_id","claim_file_type_cd","claim_file_name","claim_file_size","mfr_id","file_rec_cnt","claim_file_stus_cd","insert_user_id"]
                meta_df = pd.DataFrame(meta_info, columns=meta_cols)
                postgres_query(jdbc_url,mtf_db,schema,table,action="meta",dat=meta_df)

    else:
        break

job.commit()
