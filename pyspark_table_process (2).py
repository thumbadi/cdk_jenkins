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
from pyspark.sql import Row
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

def postgres_query(db,schema,table,*kwargs):
    host = "database-1.ch8qiq2uct5o.us-east-1.rds.amazonaws.com"
    if table != "mdb_table":
        query = f"SELECT ndc_cd, ndc_rec_cd FROM {schema}.{table} WHERE ndc_rec_cd = 'Q01'"
        df = spark.read.format("jdbc") \
            .option("url", f"jdbc:postgresql://{host}:{5432}/{db}") \
            .option("dbtable", query) \
            .option("user", credentials['username']) \
            .option("password", credentials['password']) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        return df
    else:
        data = kwargs.get("df")
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

for entry in schema_data:
    for key in entry.keys():        
        database = key["database"]
        schema = key["schema"]
        table = key["table_name"]

        if key == "view_table":
            view_df = postgres_query(database,schema,table)
            if view_df.isEmpty() == False:
                continue
        elif key == "ndc_table":
            ndc_df = postgres_query(database,schema,table)
            if view_df.isEmpty() == False:
                continue
        elif key == "mdb_table":
            postgres_query(database,schema,table,df=view_df)
            
 
def createTable_postgres(columns,table):
    # Retrieve credentials from Secrets Manager
    credentials = get_secret(secret_name)

    # PostgreSQL connection options (from secret)
    postgres_options = {
        "url": f"jdbc:postgresql://database-1.ch8qiq2uct5o.us-east-1.rds.amazonaws.com:5432/postgres",
        "dbtable": f"{schema}.{table}",
        "user": credentials['username'],
        "password": credentials['password'],
        "driver": "org.postgresql.Driver"
    }
    # Create Empty DataFrame
    df = spark.createDataFrame([], columns)

    return "success"

def postgres_legacy_mode(query, **kwargs):
    subprocess.call([sys.executable, "-m", "pip", "install", "--user", "psycopg2-binary"])
    import psycopg2
    credentials = get_secret(secret_name)
    host = "database-1.ch8qiq2uct5o.us-east-1.rds.amazonaws.com"
    db = "postgres"
    user = credentials['username']
    password = credentials['password']
    conn = psycopg2.connect(f"dbname={db} user={user} password={password} host={host}")
    cursor = conn.cursor()

    # Execute CREATE TABLE query
    create_table_query = query
    cursor.execute(create_table_query)
    conn.commit()

    # Close connection
    cursor.close()
    conn.close()
