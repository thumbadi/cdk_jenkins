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
    host = "database-1.ch8qiq2uct5o.us-east-1.rds.amazonaws.com"
    if key_table == "view_table":
        query = f"""(SELECT ndc_cd, ndc_rec_cd,ndc_price_dt, \
            ndc_unit_amt,idr_updt_ts \
                FROM {schema}.{table} WHERE ndc_rec_cd = 'Q01') AS temp"""
        df = spark.read.format("jdbc") \
            .option("url", f"jdbc:postgresql://{host}:{5432}/{db}") \
            .option("dbtable", query) \
            .option("user", credentials['username']) \
            .option("password", credentials['password']) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        return df
    elif key_table == "ndc_table":
        query = f"""(SELECT ndc_cd FROM {schema}.{table}) AS temp"""
        df = spark.read.format("jdbc") \
            .option("url", f"jdbc:postgresql://{host}:{5432}/{db}") \
            .option("dbtable", query) \
            .option("user", credentials['username']) \
            .option("password", credentials['password']) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        return df
    elif key_table == "mdb_table":
        data = kwargs.get("df")
        data.write \
                .format("jdbc") \
                .option("url", f"jdbc:postgresql://{host}:{5432}/{db}") \
                .option("dbtable", f"{schema}.{table}") \
                .option("user", credentials['username']) \
                .option("password", credentials['password']) \
                .option("driver", "org.postgresql.Driver") \
                .option("truncate", "true") \
                .mode("overwrite") \
                .save()

schema_data = [
        {"view_table" : {
            "table_name" : "v2_mdcr_ndc_mddb_price",
            "schema" : "mtf",
            "database" : "postgres"
        }},
        {"ndc_table" : {
            "table_name" : "ndc",
            "schema" : "mtf",
            "database" : "postgres"
        }},
        {"mdb_table" : {
            "table_name" : "mddb_dtl",
            "schema" : "mtf",
            "database" : "postgres"
        }}
]

secret_name = "rds!db-dcb3ad0e-5246-450e-9f85-44450fccbddb"
credentials = get_secret(secret_name)
stage_error= False

for entry in schema_data:
    if stage_error == False:
        for key in entry.keys():        
            database = entry[key]["database"]
            schema = entry[key]["schema"]
            table = entry[key]["table_name"]

            if key == "view_table":
                view_df = postgres_query(database,schema,table,key=key)
                if view_df.isEmpty() == False:
                    continue
                else:
                    print("No records found in view table..")
                    stage_error = True

            elif key == "ndc_table":
                ndc_df = postgres_query(database,schema,table,key=key)
                if ndc_df.isEmpty() == False:
                    continue
                else:
                    print("No records found in matching table..")
                    stage_error = True
                
            elif key == "mdb_table":

                final_df = (
                        view_df.join(ndc_df, on="ndc_cd", how="inner")
                        .selectExpr(
                            "ndc_cd",
                            "ndc_price_dt",
                            "ndc_unit_amt",
                            "idr_updt_ts as idr_update_ts"
                        )
                        .withColumn("insert_user_id", lit(-1))  # Assign -1 as a constant value
                        .withColumn("update_user_id", lit(None).cast("int"))  # Assign empty string for update_user_id
                        .withColumn("update_ts", lit(None).cast("timestamp"))  # Assign empty string for update_ts
                    )
                postgres_query(database,schema,table,df=final_df,key=key)
                print(f"Total {final_df.count()} records inserted..")
    else:
        break
