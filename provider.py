import sys
import subprocess
import boto3
import datetime
from botocore.exceptions import NoCredentialsError
import json
from functools import reduce
from awsglue.transforms import *
# from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col,lit
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DecimalType, DateType, TimestampType
from pyspark.sql.window import Window
subprocess.call([sys.executable, "-m", "pip", "install", "--user", "psycopg2-binary"])
import psycopg2

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Function to retrieve PostgreSQL credentials from AWS Secrets Manager
def get_secret(secret_name):
    """Fetch secret from AWS Secrets Manager."""
    region_name = "us-east-1"  # Modify if needed
    client = boto3.client('secretsmanager', region_name=region_name)

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

        # Parse the secret (assuming it’s JSON)
        secret_dict = json.loads(secret)
        return secret_dict

    except Exception as e:
        print(f"Error retrieving secret: {e}")
        raise e

def postgres_query(db,schema,table,**kwargs):
    key_table = kwargs.get("key")
    host = "dev-mtfdm-db-cluster.cluster-cjsa40wuo8ej.us-east-1.rds.amazonaws.com"
    if key_table == "view_table":
        query = f"""(SELECT prvdr_ncpdp_id, prvdr_ncpdp_dba_name, prvdr_ncpdp_lgl_busns_name, prvdr_ncpdp_store_num, prvdr_ncpdp_line_1_adr, prvdr_ncpdp_line_2_adr, \
            prvdr_ncpdp_invld_plc_name, prvdr_ncpdp_invld_state_cd, prvdr_ncpdp_invld_zip_cd, prvdr_ncpdp_invld_cnty_cd, geo_sk, geo_zip4_cd, prvdr_ncpdp_site_tel_num, \
            prvdr_ncpdp_site_extnsn_num, prvdr_ncpdp_site_fax_num, prvdr_ncpdp_site_email_adr, prvdr_ncpdp_store_open_dt, prvdr_ncpdp_store_clsr_dt, prvdr_ncpdp_mlg_line_1_adr, \
            prvdr_ncpdp_mlg_line_2_adr, prvdr_ncpdp_invld_mlg_plc_name, prvdr_ncpdp_invld_mlg_state_cd, prvdr_ncpdp_invld_mlg_zip_cd, geo_mlg_sk, geo_mlg_zip4_cd, \
            prvdr_ncpdp_cntct_1st_name, prvdr_ncpdp_cntct_mdl_name, prvdr_ncpdp_cntct_last_name, prvdr_ncpdp_cntct_title_name, prvdr_ncpdp_cntct_tel_num, prvdr_ncpdp_cntct_extnsn_num, \
            prvdr_ncpdp_cntct_email_adr, prvdr_ncpdp_dspnsr_cls_cd, prvdr_ncpdp_1_dspnsr_type_cd, prvdr_ncpdp_2_dspnsr_type_cd, prvdr_ncpdp_3_dspnsr_type_cd, prvdr_ncpdp_upin_id, \
            prvdr_ncpdp_npi, prvdr_ncpdp_fed_tax_num, prvdr_ncpdp_state_lcns_num, prvdr_ncpdp_state_tax_num, prvdr_ncpdp_dlt_dt, prvdr_ncpdp_hstry_vrsn_dt, prvdr_ncpdp_deactvtn_cd, \
            prvdr_ncpdp_physn_name, prvdr_ncpdp_reinstmt_cd, prvdr_ncpdp_reinstmt_dt, prvdr_ncpdp_stus_340b_cd, prvdr_ncpdp_stus_340b_sw FROM {schema}.{table}) AS temp"""
        df = spark.read.format("jdbc") \
            .option("url", f"jdbc:postgresql://{host}:{5432}/{db}") \
            .option("dbtable", query) \
            .option("user", credentials['username']) \
            .option("password", credentials['password']) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        print('df in if**************')
        print(df.show())
        return df
    elif key_table =="prvdr_ncpdp_table":
        data = kwargs.get("df")
        print('df**************')
        print(data)
        data.write \
                .format("jdbc") \
                .option("url", f"jdbc:postgresql://{host}:{5432}/{db}") \
                .option("dbtable", f"{schema}.{table}") \
                .option("user", credentials['username']) \
                .option("password", credentials['password']) \
                .option("driver", "org.postgresql.Driver") \
                .mode("overwrite") \
                .save()

schema_data = [
        {"view_table" : {
            "table_name" : "v2_mdcr_prvdr_ncpdp",
            "schema" : "cms_vdm_view_mdcr",
            "database" : "idr"
        }},
        {"prvdr_ncpdp_table" : {
            "table_name" : "prvdr_ncpdp",
            "schema" : "shared",
            "database" : "mtf_dba"
        }}
]

secret_name = "dev/rds/postgres/app/mtfdm"
credentials = get_secret(secret_name)
stage_error = False

for entry in schema_data:
    if stage_error == False:
        for key in entry.keys():        
            database = entry[key]["database"]
            schema = entry[key]["schema"]
            table = entry[key]["table_name"]
    
            if key == "view_table":
                view_df = postgres_query(database,schema,table,key=key)
                if view_df.isEmpty() == False:
                    print(view_df)
                    continue
                else:
                    print("No records found in the view.")
                    stage_error = True
            elif key == "prvdr_ncpdp_table":
                final_df = view_df.withColumn("insert_user_id", lit(-1)) \  # Assign -1 as a constant value
                    .withColumn("update_user_id", lit(None).cast("int")) \  # Assign empty string for update_user_id
                    .withColumn("update_ts", lit(None).cast("timestamp"))  #Assign empty string for update_ts
                
                postgres_query(database,schema,table,df=final_df,key=key)
                #postgres_query(database,schema,table,key=key)
                print(f"Total {final_df.count()} records inserted..")
    else:
        break
 
