import psycopg2
from io import StringIO
import boto3
# import pandas as pd
import json
import base64
import re
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import boto3
from botocore.exceptions import NoCredentialsError
from pyspark.sql.functions import col,lit

# JAR_PATH = "c:/csv/postgresql-42.7.5.jar"
# spark = SparkSession.builder \
#     .appName("ReadFromRedshift") \
#     .config("spark.driver.extraClassPath", JAR_PATH) \
#     .config("spark.executor.extraClassPath", JAR_PATH) \
#     .getOrCreate()

s3 = boto3.client("s3")

db = "mtf"
schema = "claim"
envir = "DEV"
bucket_name = "pyspark-demo12"
input_prefix = "input/"
reject_prefix = "error/"
invalid_prefix = "invalid/"
pattern = re.compile(
    r"^mtfdm_(?P<envir>[a-zA-Z0-9]+)_PaymentFeedback_(?P<date>\d{8})_(?P<time>\d{6})\.csv$"
)
download_path = "/tmp/file.csv" 

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

        # Parse the secret (assuming it’s JSON)
        secret_dict = json.loads(secret)
        return secret_dict

    except Exception as e:
        print(f"Error retrieving secret: {e}")
        raise e
    
def read_bank_details():
    query = f"""(SELECT payee_id from {schema}.bank_file_dtl)
            """
    ref_df = spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{host}:{5432}/{db}") \
        .option("dbtable", query) \
        .option("user", credentials['username']) \
        .option("password", credentials['password']) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    return ref_df

# Extract file names
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=input_prefix)
file_paths = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"] != input_prefix]

host = "database-1.ch8qiq2uct5o.us-east-1.rds.amazonaws.com"
secret_name = "rds!db-dcb3ad0e-5246-450e-9f85-44450fccbddb"
credentials = get_secret(secret_name)

for obj in file_paths:
    key = obj
    filename = key.split("/")[-1]
    if not pattern.match(filename):
        s3.copy_object(
            Bucket=bucket_name,
            CopySource={"Bucket": bucket_name, "Key": key},
            Key=reject_prefix + filename
        )
        s3.delete_object(Bucket=bucket_name, Key=key)
        print(f"Rejected file {filename}")
        continue
    
    file_path = f"s3://{bucket_name}/{key}"
    # for AWS Glue
    # incoming_df = spark.read.option("header", "true").csv(file_path)
    
    # for Local environment
    s3.download_file(bucket_name, key, download_path)
    incoming_df = spark.read.option("header", "true").csv(download_path)

    # read data from bank details table
    ref_df = read_bank_details()

    # column mapping with target table
    new_columns = ['payee_id','reason_txt','pymt_pref_cd','pymt_acct_num','pymt_routing_num','hold_dt','release_dt']
    index = 0
    for col_name in incoming_df.columns:
        if col_name in ['HoldDate','ReleaseDate']:
            incoming_df = incoming_df.withColumn(col_name,
            F.to_date(F.col(col_name), "M/d/yyyy"))
            incoming_df = incoming_df.withColumnRenamed(col_name,new_columns[index] )
        else:
            incoming_df = incoming_df.withColumnRenamed(col_name,new_columns[index] )
        index += 1

    joined_df = incoming_df.join(ref_df, on="payee_id", how="left_semi").withColumn("insert_user_id", lit(-1))
    not_matched_df = incoming_df.join(ref_df, on="payee_id", how="left_anti")

    # Save the data into feedback table
    if joined_df.count() > 0:
        joined_df.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{host}:{5432}/{db}") \
            .option("dbtable", "claim.bank_feedback_dtl") \
            .option("user", credentials['username']) \
            .option("password", credentials['password']) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        print(f"Inserted {joined_df.count()} valid records into Postgres")

    # Upload the not matched records into S3 bucket    
    not_matched_pdf = not_matched_df.toPandas()
    if not not_matched_pdf.empty:
        csv_buffer = StringIO()
        not_matched_pdf.to_csv(csv_buffer, index=False)
        fname = f"{invalid_prefix}{filename}"
        s3.put_object(
            Bucket=bucket_name,
            Key=fname,
            Body=csv_buffer.getvalue()
        )
            