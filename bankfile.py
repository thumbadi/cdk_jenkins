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
    if seq == "read_mfr":
        query = f"""(select recordoperation AS "RecordOperation", organizationcode AS "OrganizationCode", payeeid AS "PayeeID", organizationidentifier AS "OrganizationIdentifier", organizationname AS "OrganizationName", organizationlegalname AS "OrganizationLegalName", organizationtin AS "OrganizationTIN", organizationtintype AS "OrganizationTINType", profit_nonprofit AS "Profit-Nonprofit", organizationnpi AS "OrganizationNPI", paymentmode AS "PaymentMode", routingtransitnumber AS "RoutingTransitNumber", accountnumber AS "AccountNumber", accounttype AS "AccountType", effectivestartdate AS "EffectiveStartDate", effectiveenddate AS "EffectiveEndDate", addresscode AS "AddressCode", addressline1 AS "AddressLine1", addressline2 AS "AddressLine2", cityname AS "CityName", state AS "State", postalcode AS "PostalCode", contactcode AS "ContactCode", contactfirstname AS "ContactFirstName", contactlastname AS "ContactLastName", contacttitle AS "ContactTitle", contactphone AS "ContactPhone", contactfax AS "ContactFax", contactotherphone AS "ContactOtherPhone", contactemail AS "ContactEmail"
            from mfr.mfr_bank_file_vw)"""
        df_mfr = spark.read.format("jdbc") \
            .option("url", f"{jdbc_url}") \
            .option("dbtable", query) \
            .option("user", credentials['username']) \
            .option("password", credentials['password']) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        return df_mfr
    elif seq == "read_de_tpse":
        query = f"""(SELECT org_type as "OrgType", de_id as "DespenserID", tpse_id as "TypeID", payee_id as "PayeeID", org_name as "OrganizationName", org_ftin_num as "OrganizationTIN", org_npi_num as "OrganizationNPI", profit_yn as "Profit-Nonprofit", pymt_pref_cd as "PaymentMode", pymt_routing_num as "RoutingTransitNumber", pymt_acct_num as "AccountNumber", pymt_acct_type as "AccountType", eff_dt as "EffectiveStartDate", end_dt as "EffectiveEndDate", pymt_adrs_cd as "AddressCode", pymt_adrs_line_1 as "AddressLine1", pymt_adrs_line_2 as "AddressLine2", pymt_adrs_city as "CityName", pymt_adrs_state as "State", pymt_adrs_zip_code as "PostalCode", contact_first_name as "ContactFirstName", contact_last_name as "ContactLastName", contact_title as "ContactTitle", contact_phone_num_1 as "ContactPhone", contact_phone_num_2 as "ContactOtherPhone", contact_email_adrs as "ContactEmail", xfr_req_yn FROM de_tpse.de_tpse_payee_dtl)"""
        df_de_tpse = spark.read.format("jdbc") \
            .option("url", f"{jdbc_url}") \
            .option("dbtable", query) \
            .option("user", credentials['username']) \
            .option("password", credentials['password']) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        return df_de_tpse
    
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

schema_data =  [
    {
    "read_mfr" : {
            "table_name" : "mfr_bank_file_vw",
            "schema" : "mfr"
    }},
    {
    "read_de_tpse" : {
            "table_name" : "de_tpse_payee_dtl",
            "schema" : "de_tpse"
    }}
    ]

mtf_connection = glue_client.get_connection(Name='MTFDMDataConnector')
print(mtf_connection)
mtf_connection_options = mtf_connection['Connection']['ConnectionProperties']
print(mtf_connection_options)
jdbc_url = mtf_connection['Connection']['ConnectionProperties']['JDBC_CONNECTION_URL']
credentials=get_secret(mtf_connection_options['SECRET_ID'])
mtf_db=args['MTF_DBNAME']
s3_bucket=args['S3_BUCKET']
env=args['ENV']
stage_error = False
for entry in schema_data:
    if stage_error == False:
        for key in entry.keys():        
            schema = entry[key]["schema"]
            table = entry[key]["table_name"]        
            bucket_name = s3_bucket
            if key == "read_mfr":
                view_mfr_df = postgres_query(jdbc_url,mtf_db,schema,table,action="read_mfr")
                if view_mfr_df.isEmpty() == False:
                    ts = datetime.datetime.today().strftime("%Y%m%d_%H%M%S")
                    file_path = f"bankfile/ready/MTFDM.{env}.BankData.{ts}.parquet"
                    df_mfr = view_mfr_df.toPandas()
                    df_mfr.to_parquet(f"/tmp/MTFDM.{env}.BankData.{ts}.parquet")
                    s3_client = boto3.client('s3')
                    s3_client.upload_file(f"/tmp/MTFDM.{env}.BankData.{ts}.parquet", bucket_name, file_path)
                    
                    logger.info('Parquet file has been created.')
                    continue
                else:
                    logger.info('No records found in mfr_bank_file_vw table.')
                    stage_error = True
            elif key == "read_de_tpse":
                    view_detpse_df = postgres_query(jdbc_url,mtf_db,schema,table,action="read_de_tpse")
                    if view_detpse_df.isEmpty() == False:
                        ts = datetime.datetime.today().strftime("%Y%m%d_%H%M%S")
                        file_path = f"bankfile/ready/MTFDM.{env}.BankData.{ts}.parquet"
                        df_mfr = view_detpse_df.toPandas()
                        df_mfr.to_parquet(f"/tmp/MTFDM.{env}.BankData.{ts}.parquet")
                        s3_client = boto3.client('s3')
                        s3_client.upload_file(f"/tmp/MTFDM.{env}.BankData.{ts}.parquet", bucket_name, file_path)
                        
                        logger.info('Parquet file has been created.')
                        continue
                    else:
                        logger.info('No records found in mfr_bank_file_vw table.')
                        stage_error = True
    else:
        break

job.commit()
