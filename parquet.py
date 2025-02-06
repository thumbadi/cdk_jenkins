import sys
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
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'SECRET_NAME'])

# Define input/output paths
input_path = args['S3_INPUT_PATH']
# output_path = args['S3_OUTPUT_PATH']
secret_name = args['SECRET_NAME'] 

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
# Retrieve credentials from Secrets Manager
credentials = get_secret("rds!db-dcb3ad0e-5246-450e-9f85-44450fccbddb")

# PostgreSQL connection options (from secret)
postgres_options = {
    "url": f"jdbc:postgresql://database-1.ch8qiq2uct5o.us-east-1.rds.amazonaws.com:5432/postgres",
    "dbtable": "public.demo_loadtest",
    "user": credentials['username'],
    "password": credentials['password'],
    "driver": "org.postgresql.Driver"
}

def send_to_sqs_batch(data):
    # Initialize boto3 SQS client with explicit AWS credentials
    SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/585768147395/test-queue"
    AWS_REGION = "us-east-1"

    sqs = boto3.client('sqs', region_name=AWS_REGION)
    
    entries = []
    batch_json = [json.dumps(row.asDict()) for row in data]
    for i, json_data in enumerate(batch_json):
        entries.append({
            'Id': str(f"msg-{i+1}"),  # Unique ID for each message in the batch
            'MessageBody': json_data  # Message body
        })
        # print(entries)

    try:
        # Send the batch of messages to the SQS queue
        response = sqs.send_message_batch(
            QueueUrl=SQS_QUEUE_URL,
            Entries=entries
        )

        print(f"Sent {len(entries)} messages in a batch. Batch Message IDs: {[msg['MessageId'] for msg in response['Successful']]}")
    except Exception as e:
        print(f"Error sending batch to SQS: {e}")



# Read the text file into an RDD (line by line) from S3
rdd = spark.read.parquet(input_path).rdd

# Step 1: Extract the 'value' column from each Row
values = [row for row in rdd.collect()]

# from pyspark.sql import Row
df = spark.createDataFrame(values)

# Identify duplicate rows based on 'name' column
dup_records = False
duplicates = df.groupBy('claim_control_no').count().filter('count > 1')

if duplicates.isEmpty() == False:
    dup_records = True
    
    # Define a window specification based on the "id" column (or columns you want to check for duplicates)
    window_spec = Window.partitionBy("claim_control_no").orderBy("date_of_service")

    # Add a row number column to identify duplicate rows
    df_with_row_number = df.withColumn("row_number", row_number().over(window_spec))

    # Filter out duplicates (keep only the first occurrence of each group)
    df_no_duplicates = df_with_row_number.filter(col("row_number") == 1).drop("row_number")

   # Optionally, you can extract the duplicates (those that have row_number > 1)
    duplicate_rows = df_with_row_number.filter(col("row_number") > 1).drop("row_number")
    df_with_row_number.unpersist()
    # Extract rows that have duplicates (based on 'claim_control_no' column)
    # duplicate_rows = df.join(duplicates, on='claim_control_no', how='inner')
    # df_no_duplicates = df.dropDuplicates(['claim_control_no'])
else:
    df_no_duplicates = df
    df.unpersist()

# Regular expression to check for special characters
special_char_pattern = '[^a-zA-Z0-9 .-]'

# Loop through all columns and check for special characters
filter_expr = reduce(
    lambda a, b: a | b,
    [col(column).rlike(special_char_pattern) for column in df_no_duplicates.columns]
)
# Filter out rows with special characters in any column (invert the condition using ~)  
df_no_special_chars = df_no_duplicates.filter(~filter_expr)
df_with_special_chars = df_no_duplicates.filter(filter_expr)
dynamic_frame = DynamicFrame.fromDF(df_no_special_chars, glueContext, "dynamic_frame")

# Convert the DataFrame to a Glue DynamicFrame
# if dup_records == True:
#     dynamic_frame = DynamicFrame.fromDF(df_no_duplicates, glueContext, "dynamic_frame")
# elif df_with_special_chars.isEmpty() == False:
#     dynamic_frame = DynamicFrame.fromDF(df_no_special_chars, glueContext, "dynamic_frame")

# Write the data into the PostgreSQL database
glueContext.write_dynamic_frame.from_options(
    dynamic_frame,
    connection_type="postgresql",
    connection_options=postgres_options
)

#move TXT file after processed
s3_client = boto3.client('s3')
try:    
    final_dir = "completed"
    err_dir = "error"
    dup_dir = "duplicates"
    input_dir = "input"
    source_key = input_path.split("/")[4]
    bucket = input_path.split("/")[2]
    # dest_bucket = input_path.split("/")[2]
    # print(f"Copying file from {source_bucket}/{source_key} to {dest_bucket}/{source_key}")
    # Copy the file from input to completed
    s3_client.copy_object(
        CopySource={'Bucket': bucket, 'Key': f"{input_dir}/{source_key}"},
        Bucket=bucket,
        Key=f"{final_dir}/{source_key}"
    )
    
    # Once the copy is successful, delete the original file
    # print(f"Deleting original file {source_bucket}/{source_key}")
    s3_client.delete_object(Bucket=bucket, Key=f"{input_dir}/{source_key}")
    
    # print(f"File moved from {source_bucket}/{source_key} to {destination_bucket}/{destination_key} successfully.")
    dt_today = datetime.datetime.today().strftime("%Y%m%d")
    # Save duplicate rows to a CSV file
    if dup_records == True:
        duplicate_rows.coalesce(1).write.mode("overwrite").csv(f"s3://{bucket}/{dup_dir}/dup_{dt_today}.csv", header=True)
    # # Save rows with special characters to a CSV file
    if df_with_special_chars.isEmpty() == False:
        df_with_special_chars.coalesce(1).write.mode("overwrite").csv(f"s3://{bucket}/{err_dir}/special_char_{dt_today}.csv", header=True)
    
    rdd_sqs = df_no_special_chars.rdd
    iterator_rows = rdd_sqs.collect()
    batch = []
    row_size = 10
    for row in iterator_rows:
        batch.append(row)
        # print(row)
        if len(batch) == row_size:
            # print(batch)
            send_to_sqs_batch(batch)
            batch = []
    if batch:
        send_to_sqs_batch(batch)

    
except NoCredentialsError:
    print("Credentials not available.")
except Exception as e:
    print(f"Error occurred: {str(e)}")
    

job.commit()
