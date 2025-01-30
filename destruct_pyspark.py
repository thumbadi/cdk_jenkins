import sys
import boto3
import json
# from awsglue.transforms import *
from pyspark.sql import SparkSession
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import udf
from pyspark.sql import Row
from pyspark.sql.types import StringType
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType,DateType
import datetime
# sc = SparkContext()
# glueContext = GlueContext(sc)
# spark = glueContext.spark_session
spark = SparkSession.builder.appName("SchemaSplitExample").getOrCreate()


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
        print("Testing print line by line reading **************" + str(line))
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
                        # value = float(value)
                    else:
                        # Truncate the value if it exceeds the required decimal length
                        integer_part, decimal_part = value.split('.')
                        decimal_part = decimal_part[:decimal_places]  # Limit decimal part
                        value = f"{integer_part}.{decimal_part}".strip()
                # else:
                #     # If the value is empty, represent it as 0 with the given decimal places
                #     value = '0' * (column_length - decimal_places) + '.' + '0' * decimal_places
    
            elif column_type == 'int':
                # For integer columns, pad with leading zeroes if needed
                value = int(value.zfill(column_length).strip())
    
            elif column_type == 'str':
                # For string columns, pad with spaces to match the column length
                value = value.ljust(column_length).strip()
            elif column_type == 'abn':
                # For string columns, pad with spaces to match the column length
                value = value.ljust(column_length).strip()
            elif column_type == 'date':
                # For string columns, pad with spaces to match the column length
                dt_str = str(value.ljust(column_length).strip())
                value = datetime.datetime.strptime(dt_str,"%Y%m%d").strftime("%Y-%m-%d")
            tmp.append(value)
    
            # Move the start index to the next column's position
            start_index += column_length
        values.append(tuple(tmp))
    return values

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
    {'column': '340b_indicator', 'type': 'str', 'length': 1},
    {'column': 'filler', 'type': 'abn', 'length': 40},
    {'column': 'orgi_submit_cont', 'type': 'str', 'length': 5},
    {'column': 'claim_control_no', 'type': 'str', 'length': 40},
    {'column': 'card_holder_id', 'type': 'str', 'length': 20}
]

# Read the text file into an RDD (line by line) from S3
rdd = spark.read.text("c:/flatfile.txt").rdd

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
    StructField("date_of_service", IntegerType(), True),
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
    StructField("340b_indicator", StringType(), True),
    StructField("filler_2", StringType(), True),
    StructField("orgi_submit_cont", StringType(), True),
    StructField("claim_control_no", StringType(), True),
    StructField("card_holder_id", StringType(), True)
])
header = ["record_id","sequence_no","mtf_responsecode","adj_deletioncode","date_of_service","pres_serviceref_no","fill_number","service_providerId_qual","service_provider_id","alter_service_provid_qual","alter_service_provid_id","prescriberId_qual","prescriber_id","filler_1","product_service_id","qty_dispensed","days_supply","340b_indicator","filler_2","orgi_sumbit_cont","claim_control_no","card_holder_id"]

import pandas as pd
df = pd.DataFrame(transformed_data, columns)
# from pyspark.sql import Row
# df = spark.createDataFrame(transformed_data, columns)
# Identify duplicate rows based on 'name' column
duplicates = df.groupBy('claim_control_no').count().filter('count > 1')
if duplicates.isEmpty == False:
    dup_records =True
# Extract rows that have duplicates (based on 'name' column)
    duplicate_rows = df.join(duplicates, on='claim_control_no', how='inner')
    df_no_duplicates = df.dropDuplicates(['claim_control_no'])

# Regular expression to check for special characters
special_char_pattern = '[^a-zA-Z0-9]'

# Initialize condition for filtering
condition = None

# Loop through all columns and check for special characters
for column in df.columns:
    # Create condition for each column with special characters
    column_condition = col(column).rlike(special_char_pattern)
    
    # Combine the conditions using OR (|)
    if condition is None:
        condition = column_condition
    else:
        condition = condition | column_condition
# Filter out rows with special characters in any column (invert the condition using ~)  
df_no_special_chars = df.filter(~condition)
    
# Convert the DataFrame to a Glue DynamicFrame
if dup_records.empty == False:
    dynamic_frame = DynamicFrame.fromDF(duplicate_rows, GlueContext, "dynamic_frame")
elif condition != None:
    dynamic_frame = DynamicFrame.fromDF(df_no_special_chars, GlueContext, "dynamic_frame")
else:
    dynamic_frame = DynamicFrame.fromDF(df, GlueContext, "dynamic_frame")

# df.write.csv("C:\output.csv")
# print(df)
# Convert the DataFrame to a Glue DynamicFrame
# dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")