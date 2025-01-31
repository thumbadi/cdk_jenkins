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
from pyspark.sql.functions import col
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
credentials = get_secret("rds!db-1b56c8d7-8b7e-46fa-a7c7-d30322ab4187")

# PostgreSQL connection options (from secret)
postgres_options = {
    "url": f"jdbc:postgresql://database-1.cwnacmkaedhp.us-east-1.rds.amazonaws.com:5432/postgres",
    "dbtable": "public.demo_loadtest",
    "user": credentials['username'],
    "password": credentials['password'],
    "driver": "org.postgresql.Driver"
}

def split_by_schema(lines, schema):
    """
    Splits the line based on the schema mapping and fills float values accordingly.
    
    :param line: The line to split (string).
    :param schema: A list of dictionaries defining the schema with column length and type.
                   Each schema dict contains:
                   - 'column': Column name (for reference)
                   - 'type': Column type (e.g., 'float', 'int', 'str')
                   - 'length': Total length of the column in the final line
                   - 'decimal_places' (optional for float types): Decimal places for float column type.
    :return: List of split column values.
    """

    values = []
    for line in lines:
        start_index = 0
        tmp = []
        # print("Testing print line by line reading **************" + str(line))
        for column in schema:
            column_name = column['column']
            column_type = column['type']
            column_length = column['length']
            decimal_places = column.get('decimal_places', 0)
    
            # Get the substring for the current column
            value = line[start_index:start_index + column_length].strip()
    
            if column_type == 'float':
                # For float columns, we need to split the value by the column length
                if value:
                    # If there's no decimal, split the value into integer and decimal portions
                    if '.' not in value:
                        integer_part = value[:column_length - decimal_places]
                        decimal_part = value[column_length - decimal_places:]
                        value = f"{integer_part}.{decimal_part}".strip()
                    else:
                        # Truncate the value if it exceeds the required decimal length
                        integer_part, decimal_part = value.split('.')
                        decimal_part = decimal_part[:decimal_places]  # Limit decimal part
                        value = f"{integer_part}.{decimal_part}"
                else:
                    # If the value is empty, represent it as 0 with the given decimal places
                    value = '0' * (column_length - decimal_places) + '.' + '0' * decimal_places
    
            elif column_type == 'int':
                # For integer columns, pad with leading zeroes if needed
                value = int(value.zfill(column_length).strip())
    
            elif column_type == 'str':
                # For string columns, pad with spaces to match the column length
                value = str(value.ljust(column_length).strip())
                
            elif column_type == 'abn':
                # For string columns, pad with spaces to match the column length
                value = str(value.ljust(column_length).strip())
            elif column_type == 'date':
                # For string columns, pad with spaces to match the column length
                dt_str = str(value.ljust(column_length).strip())
                value = datetime.datetime.strptime(dt_str,"%Y%m%d").strftime("%Y-%m-%d")
            tmp.append(value)            
    
            # Move the start index to the next column's position
            start_index += column_length
        values.append(tuple(tmp))
    return values


# Define schema for the split operation
schema = [
    {'column': 'record_id', 'type': 'str', 'length': 3},
    {'column': 'sequence_no', 'type': 'int', 'length': 7}, 
    {'column': 'mtf_responsecode', 'type': 'str', 'length': 2},
    {'column': 'adj_deletioncode', 'type': 'str', 'length': 1},
    {'column': 'date_of_service', 'type': 'date', 'length': 8},
    {'column': 'pres_serviceref_no', 'type': 'int', 'length': 12},
    {'column': 'fill_number', 'type': 'int', 'length': 2},
    {'column': 'service_providerId_qual', 'type': 'str', 'length': 2},
    {'column': 'service_provider_id', 'type': 'str', 'length': 15},
    {'column': 'alter_service_provid_qual', 'type': 'str', 'length': 2},
    {'column': 'alter_service_provid_id', 'type': 'str', 'length': 15},
    {'column': 'prescriberId_qual', 'type': 'str', 'length': 2},
    {'column': 'prescriber_id', 'type': 'str', 'length': 35},
    {'column': 'filler', 'type': 'abn', 'length': 40},
    {'column': 'product_service_id', 'type': 'str', 'length': 40},
    {'column': 'qty_dispensed', 'type': 'float', 'length': 10, 'decimal_places': 3},
    {'column': 'days_supply', 'type': 'int', 'length': 3},
    {'column': 'indicator', 'type': 'str', 'length': 1},
    {'column': 'filler', 'type': 'abn', 'length': 40},
    {'column': 'orgi_submit_cont', 'type': 'str', 'length': 5},
    {'column': 'claim_control_no', 'type': 'str', 'length': 40},
    {'column': 'card_holder_id', 'type': 'str', 'length': 20}
]

# Read the text file into an RDD (line by line) from S3
rdd = spark.read.text(input_path).rdd

# Step 1: Extract the 'value' column from each Row
values = [row['value'] for row in rdd.collect()]

# Apply the custom transformation function on the entire RDD
transformed_data = split_by_schema(values, schema)

# Define the schema explicitly
columns = StructType([
    StructField("record_id", StringType(), True),
    StructField("sequence_no", IntegerType(), True),
    StructField("mtf_responsecode", StringType(), True),
    StructField("adj_deletioncode", StringType(), True),
    StructField("date_of_service", StringType(), True),
    StructField("pres_serviceref_no", IntegerType(), True),
    StructField("fill_number", IntegerType(), True),
    StructField("service_providerId_qual", StringType(), True),
    StructField("service_provider_id", StringType(), True),
    StructField("alter_service_provid_qual", StringType(), True),
    StructField("alter_service_provid_id", StringType(), True),
    StructField("prescriberId_qual", StringType(), True),
    StructField("prescriber_id", StringType(), True),
    StructField("filler_1", StringType(), True),
    StructField("product_service_id", StringType(), True),
    StructField("qty_dispensed", StringType(), True),
    StructField("days_supply", IntegerType(), True),
    StructField("indicator", StringType(), True),
    StructField("filler_2", StringType(), True),
    StructField("orgi_submit_cont", StringType(), True),
    StructField("claim_control_no", StringType(), True),
    StructField("card_holder_id", StringType(), True)
])
# header = ["record_id","sequence_no","mtf_responsecode","adj_deletioncode","date_of_service","pres_serviceref_no","fill_number","service_providerId_qual","service_provider_id","alter_service_provid_qual","alter_service_provid_id","prescriberId_qual","prescriber_id","filler_1","product_service_id","qty_dispensed","days_supply","340b_indicator","filler_2","orgi_submit_cont","claim_control_no","card_holder_id"]

# from pyspark.sql import Row
df = spark.createDataFrame(transformed_data, columns)

# Identify duplicate rows based on 'name' column
dup_records = False
duplicates = df.groupBy('claim_control_no').count().filter('count > 1')
if duplicates.isEmpty() == False:
    dup_records = True
    
    # Extract rows that have duplicates (based on 'claim_control_no' column)
    duplicate_rows = df.join(duplicates, on='claim_control_no', how='inner')
    df_no_duplicates = df.dropDuplicates(['claim_control_no'])

# empty the dataframe after uploaded to postgres
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
    
# Convert the DataFrame to a Glue DynamicFrame
# if dup_records == True:
#     dynamic_frame = DynamicFrame.fromDF(df_no_duplicates, glueContext, "dynamic_frame")
# elif df_with_special_chars.isEmpty() == False:
#     dynamic_frame = DynamicFrame.fromDF(df_no_special_chars, glueContext, "dynamic_frame")
# else:
#     dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
dynamic_frame = DynamicFrame.fromDF(df_no_special_chars, glueContext, "dynamic_frame")


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
        duplicate_rows.coalesce(1).write.csv(f's3://{bucket}/{dup_dir}/dup_{dt_today}.csv', header=True)
    
    # # Save rows with special characters to a CSV file
    if df_with_special_chars.isEmpty() == False:
        df_with_special_chars.coalesce(1).write.csv(f's3://{bucket}/{err_dir}/special_char_{dt_today}.csv', header=True)
     
except NoCredentialsError:
    print("Credentials not available.")
except Exception as e:
    print(f"Error occurred: {str(e)}")
    

job.commit()
