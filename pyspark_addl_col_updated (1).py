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
from pyspark.sql.functions import col, row_number, to_date, format_string, lpad, lit, last
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define bucket and folder path
bucket_name = "pyspark-demo12"
folder_prefix = "input/"  # Add trailing slash for folders
secret_name = "rds!db-dcb3ad0e-5246-450e-9f85-44450fccbddb"
# Define input/output paths
# input_path = args['S3_INPUT_PATH']
# output_path = args['S3_OUTPUT_PATH']
# secret_name = args['SECRET_NAME']

def sequence_fetch_update(mode, **kwargs):
    subprocess.call([sys.executable, "-m", "pip", "install", "--user", "psycopg2-binary"])
    import psycopg2
    credentials = get_secret(secret_name)
    host = "database-1.ch8qiq2uct5o.us-east-1.rds.amazonaws.com"
    db = "postgres"
    schema = "public"
    user = credentials['username']
    password = credentials['password']
    conn = psycopg2.connect(f"dbname={db} user={user} password={password} host={host}")
    cur = conn.cursor()

    if mode == "fetch":
        # cur.execute(f"SELECT nextval('{schema}.received_id_seq') FROM generate_series(1, {record_count})")
        cur.execute(f"SELECT nextval('{schema}.received_id_seq')")
        sequence_ids = [row[0] for row in cur.fetchall()]
        cur.execute(f"SELECT setval('{schema}.received_id_seq', {sequence_ids[0]},FALSE)")
        conn.close()
        return sequence_ids[0]
    else:
        last_seq_num = kwargs.get("last_seq")
        # cur.execute(f"SELECT MAX(received_id) FROM public.mtf_claim")
        # max_received_id = cur.fetchone()[0]
        cur.execute(f"SELECT setval('{schema}.received_id_seq', {last_seq_num},FALSE)")
        conn.commit()
        conn.close()



def fetch_s3_objects():
    # Initialize S3 client
    s3 = boto3.client('s3',region_name='us-east-1')

    # List all files in the folder
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)

    # Extract file paths
    file_paths = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"] != folder_prefix]

    # Process each file
    lists = []
    for file_path in file_paths:
        # print(f"Processing file: {file_path}")
        lists.append(file_path)
    return lists

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

def insert_dynamicDF_postgres(data_df):
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
    # Convert the DataFrame to a Glue DynamicFrame
    dynamic_frame = DynamicFrame.fromDF(data_df, glueContext, "dynamic_frame")

    # Write the data into the PostgreSQL database
    glueContext.write_dynamic_frame.from_options(
        dynamic_frame,
        connection_type="postgresql",
        connection_options=postgres_options
    )
    return "success"

# data source mapping for data_src_cd column
fds_mapping = {
    "DDPS" : 0,
    "VMS" : 1,
    "DME" : 2,
    "PARTC" : 3,
    "MTF" : 4,
    "PAPER" : 5
}

# Define the schema explicitly
columns_df1 = {
    "data_src_cd" : "string",
    "REC_TYPE": "string",
    "PDE_RCVD_DT": "string",
    "PDE_RCVD_TM": "string",
    "PKG_AUDT_KEY_ID": "int",
    "ADJSTMT_DLTN_CD": "string",
    "DOS": "string",
    "PRES_SRVC_REF_NO": "string",
    "FILL_NUM": "string",
    "SRVC_PROV_ID_QLFR": "string",
    "SRVC_PROV_ID": "string",
    "ALT_SRVC_PROV_ID_QLFR": "string",
    "ALT_SRVC_PROV_ID": "string",
    "PHRMCY_SRVC_TYPE_CD": "string",
    "PRSCRBR_QLFR": "string",
    "PRSCRBR_ID": "string",
    "PROD_SRVC_ID": "int",
    "QTY_DSPNSD": "string",
    "DAYS_SUPLY": "string",
    "IND_340B": "string",
    "SUB_CNTRCT": "string",
    "CLM_CNTL_NUM": "string",
    "CARDHLDR_ID": "string",
    "received_id" : "int",
    "received_dt" : "string",
    "internal_claim_num" : "string",
    "status_code" : "string"
}

# Get list of files in the S3 bucket
file_list = fetch_s3_objects()
curr_val = None
last_value = None
seq_count = 0
# iterate each files
for file_path in file_list:
    # Read the text file into an RDD (line by line) from S3
    print(f"Processing file: {file_path}")

    schema_names = spark.read.parquet(f"s3://{bucket_name}/{file_path}").limit(0).schema
    # print(schema_names)
    new_columns = [
                StructField("received_id", IntegerType(), True),
                StructField("received_dt", StringType(), True),
                StructField("internal_claim_num", StringType(), True)
            ]    
    updated_schema = StructType(schema_names.fields + new_columns)

    rdd = spark.read.parquet(f"s3://{bucket_name}/{file_path}").rdd
    seq_count = rdd.count()
    # print(rdd.collect())
    # Iterate each row as tuple and add additinal 3 columns
    if curr_val is None:
        curr_val = sequence_fetch_update("fetch")
        
    default_value = fds_mapping[file_path.split("/")[1].split("_")[0]]
    icn_julian_dt = datetime.datetime.now().strftime("%Y%j")
    # icn_sequence = str(curr_val).zfill(8)
    # internal_claim_num = f"{default_value}0-{icn_julian_dt}-"+icn_sequence[0:2]+"-"+icn_sequence[2:5]+"-"+icn_sequence[5:8]
    rdd = rdd.zipWithIndex().map(lambda x: (*x[0], curr_val + x[1], 
                                                datetime.datetime.now().strftime("%Y-%m-%d"),
                                                f"{default_value}0-{icn_julian_dt}-" +
                                                str(curr_val + x[1]).zfill(8)[0:2] + "-" +
                                                str(curr_val + x[1]).zfill(8)[2:5] + "-" +
                                                str(curr_val + x[1]).zfill(8)[5:8]))
    

# Step 1: Extract the 'value' column from each Row
    values = [row for row in rdd.toLocalIterator()]

# from pyspark.sql import Row
    df = spark.createDataFrame(values, updated_schema)
    # print(df.select(df.columns[:36]).show(5, truncate=False))

# Identify duplicate rows based on 'name' column
    dup_records = False
    duplicates = df.groupBy('CLM_CNTL_NUM').count().filter('count > 1')

    if duplicates.count() != 0:
        dup_records = True
    
    # Define a window specification based on the "id" column (or columns you want to check for duplicates)
        window_spec = Window.partitionBy("CLM_CNTL_NUM").orderBy("PDE_RCVD_DT")

    # Add a row number column to identify duplicate rows
        df_with_row_number = df.withColumn("row_number", row_number().over(window_spec))

    # Filter out duplicates (keep only the first occurrence of each group)
        df_no_duplicates = df_with_row_number.filter(col("row_number") == 1).drop("row_number")

    # Optionally, you can extract the duplicates (those that have row_number > 1)
        duplicate_rows = df_with_row_number.filter(col("row_number") > 1).drop("row_number")
        df_with_row_number = df_with_row_number.filter("1=0")
    # Extract rows that have duplicates (based on 'claim_control_no' column)
    # duplicate_rows = df.join(duplicates, on='claim_control_no', how='inner')
    # df_no_duplicates = df.dropDuplicates(['claim_control_no'])
    else:
        df_no_duplicates = df
        df = df.filter("1 = 0")

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

    # extract new 3 columns
    # last_3_cols = df_no_special_chars[-3:]

# List the tables and insert the data
    table_list = ["mtf_claim","error_count"]
    split_idx = 22
    schema = "public"
    table = None
    for tbl_name in table_list:
        columns = df.columns
        if tbl_name == "mtf_claim":
            default_value = file_path.split("/")[1].split("_")[0]
            table = tbl_name
            print(f"Processing claim count table")

            # Add new column of data source id and status code in the dataframe
            df_no_special_chars = df_no_special_chars.withColumn("data_src_cd", lit(fds_mapping[default_value]))
            df_no_special_chars = df_no_special_chars.withColumn("status_code", lit("INJ"))

            # Split the data frame by first 22 columns + last 5 columns
            temp_df = df_no_special_chars.select(*columns[:split_idx],*columns[-5:])

            # convert date columns to proper date format
            temp_df = temp_df.withColumn("PDE_RCVD_DT", to_date(col("PDE_RCVD_DT").cast("string"), "yyyyMMdd"))
            temp_df = temp_df.withColumn("DOS", to_date(col("DOS").cast("string"), "yyyyMMdd"))

            # format QTY_DSPNSD column properly
            temp_df = temp_df.withColumn("QTY_DSPNSD",lpad(format_string("%07.3f", col("QTY_DSPNSD") / 1000.0),11,"0"))

                        
            # Change the columns order
            # temp_df = temp_df.withColumn("data_src_cd", lit(fds_mapping[default_value]))
            new_column_order = ["data_src_cd"] + [col.lower() for col in temp_df.columns if col != "data_src_cd"]
            temp_df = temp_df.select(new_column_order)
            

            # Apply type conversion using withColumn()
            for column, dtype in columns_df1.items():
                temp_df = temp_df.withColumn(column, col(column).cast(dtype))
                    
            insert_dynamicDF_postgres(temp_df)
        else:
            print(f"Processing error count table")
            table = tbl_name
            # Split the data frame by error columns
            temp_df = df_no_special_chars.select(columns[split_idx:]).drop("data_src_cd").drop("status_code")
            new_column_order = [col.lower() for col in temp_df.columns]
            temp_df = temp_df.select(new_column_order)
            # last_value = temp_df.agg(last("received_id")).collect()[0][0]
            response = insert_dynamicDF_postgres(temp_df)
            if response == "success":
                curr_val = curr_val + seq_count
    sequence_fetch_update("update",last_seq = curr_val)


#move parquet file after processed depends on its consistency
    s3_client = boto3.client('s3')
    try:    
        final_dir = "completed"
        err_dir = "error"
        dup_dir = "duplicates"
        input_dir = "input"
        source_key = file_path.split("/")[1]

        # print(f"Copying file from {source_bucket}/{source_key} to {dest_bucket}/{source_key}")
        # Copy the file from input to completed
        s3_client.copy_object(
            CopySource={'Bucket': bucket_name, 'Key': f"{input_dir}/{source_key}"},
            Bucket=bucket_name,
            Key=f"{final_dir}/{source_key}"
        )
    
    # Once the copy is successful, delete the original file
    # print(f"Deleting original file {source_bucket}/{source_key}")
        s3_client.delete_object(Bucket=bucket_name, Key=f"{input_dir}/{source_key}")
    
    # print(f"File moved from {source_bucket}/{source_key} to {destination_bucket}/{destination_key} successfully.")
        dt_today = datetime.datetime.today().strftime("%Y%m%d")
    # Save duplicate rows to a CSV file
        if dup_records == True:
            duplicate_rows.coalesce(1).write.mode("overwrite").csv(f"s3://{bucket_name}/{dup_dir}/dup_{dt_today}.csv", header=True)
    # # Save rows with special characters to a CSV file
        if df_with_special_chars.count() != 0:
            df_with_special_chars.coalesce(1).write.mode("overwrite").csv(f"s3://{bucket_name}/{err_dir}/special_char_{dt_today}.csv", header=True)
    
    # df_no_special_chars = df_no_special_chars.withColumn("PKG_AUDT_KEY_ID", col("PKG_AUDT_KEY_ID").cast("bigint"))
        df_no_special_chars = df_no_special_chars.withColumn("PKG_AUDT_KEY_ID", col("PKG_AUDT_KEY_ID").cast("long").cast("string"))
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