import boto3
from datetime import datetime
import json
import sys
import time
import re
from awsglue.context import GlueContextCONNECTION_NAME
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.transforms import Map, ResolveChoice
from awsglue.utils import getResolvedOptions
from collections import namedtuple
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, concat, lit, substring, udf, when, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, DateType, TimestampType, BooleanType, LongType, DecimalType

# Define a custom class to store the results
class FileNameParseResult:
    def __init__(self, data_src_cd: StringType, sender_system_datetime: StringType):
        self.data_src_cd = data_src_cd
        self.sender_system_datetime = sender_system_datetime
    def __str__(self):
        return f"FileNameParseResult(data_src_cd={self.data_src_cd}, sender_system_datetime={self.sender_system_datetime})"

def getDataSourceCode(data_src_ref_df: DynamicFrame, src_key: StringType) -> StringType:
    # Filter the DynamicFrame to get the row where file_src_key matches src_key
    filtered_df = Filter.apply(frame=data_src_ref_df, f=lambda x: x["file_src_key"] == src_key)

    # default to "0"
    data_src_cd = "0"

    # if filtered_df is not empty
    if filtered_df.count() != 0:
        # Select the data_src_cd field
        selected_fields_df = SelectFields.apply(frame=filtered_df, paths=["data_src_cd"])

        # Resolve any potential choice conflicts
        resolved_df = ResolveChoice.apply(frame=selected_fields_df, choice="make_struct")

        # Extract the data_src_cd value safely
        try:
            data_src_cd = resolved_df.toDF().first()["data_src_cd"]
        except Exception:
            pass  # Keep the default value "0" if extraction fails

    return data_src_cd

def parse_file(s3_input_path: StringType, data_src_ref_df: DynamicFrame) -> FileNameParseResult:
    # Regular expression to parse the S3 path and extract the file name
    path_pattern = re.compile(r"s3://([^/]+)/([^/]+)/([^/]+)$")
    path_match = path_pattern.match(s3_input_path)
    if path_match:
        bucket_name = path_match.group(1)
        prefix_name = path_match.group(2)
        file_name = path_match.group(3)

        # Further parse the file name
        file_pattern = re.compile(r"(\w+)_(\w+)_((\d{8})\.(\d{6}))\.parquet")
        file_match = file_pattern.match(file_name)
        if file_match:
            sourcesystem = file_match.group(1)
            targetenv = file_match.group(2)
            fatetime = file_match.group(3)
            date_part = file_match.group(4)
            time_part = file_match.group(5)
        else:
            raise ValueError("File name format is incorrect for " + file_name)
    else:
        raise ValueError("S3 path format is incorrect " + s3_input_path)

    print(f"Bucket Name: {bucket_name}")
    print(f"Prefix Name: {prefix_name}")
    print(f"File Name: {file_name}")
    print(f"Source System: {sourcesystem}")
    print(f"Target Environment: {targetenv}")
    print(f"Fate Time: {fatetime} (Date: {date_part}, Time: {time_part})")

    data_src_cd = getDataSourceCode(data_src_ref_df, sourcesystem)

    sender_system_datetime = date_part + " " + time_part
    return FileNameParseResult(data_src_cd, sender_system_datetime)

def parse_claim_data_s3_parquet(glue_context: GlueContext, s3_input_path: StringType) -> DynamicFrame:
    dynamic_frame = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [s3_input_path]},
        format="parquet"
    )
    return dynamic_frame

def get_file_list_from_s3(s3_client: boto3.client, s3_bucket: StringType) -> list:
    # source_prefix = "testCreateParquetIn/out/SAMEER_DDPS_TEST_PARQUET_AS_CSV.csv-20250225-190016/"
    # testCreateParquetIn/
    # out/
    # SAMEER_DDPS_TEST_PARQUET_AS_CSV.csv-20250225-190016/
    source_prefix = "MTF/"
    # source_prefix = "input/"

    # List all objects under the source prefix
    response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=source_prefix)

    file_list = []

    for obj in response.get('Contents', []):
        source_key = obj['Key']

        if source_key != source_prefix:
            file_list.append(source_key)

    return file_list

def move_s3_file(s3_client: boto3.client, s3_bucket: StringType, s3_file: StringType):
    # Should we pass them in?
    source_prefix = "input/"
    target_prefix = "completed/"

    source_file = s3_file.replace(f"s3://{s3_bucket}/", "")
    target_file = source_file.replace(source_prefix, target_prefix)

    print(f"Copying file from s3://{s3_bucket}/{source_file} to s3://{s3_bucket}/{target_file}")

    # Create copy source object
    copy_source = {'Bucket': s3_bucket, 'Key': source_file}
    s3_client.copy_object(CopySource=copy_source, Bucket=s3_bucket, Key=target_file)

    print(f"Copy complete, deleting source file s3://{s3_bucket}/{source_file}")
    # Delete the original object
    s3_client.delete_object(Bucket=s3_bucket, Key=source_file)

# Try Database Connection
def fetch_mtf_data(glue_context, connection_options):
    """
    Fetch data from the `mtf_claim` table in PostgreSQL as a Spark DataFrame.
    """
    try:
        # Read the table into a DataFrame
        postgre_sql_node = glue_context.create_dynamic_frame.from_options(
            connection_type = "postgresql",
            connection_options=connection_options,
            transformation_ctx = "PostgreSQL_node"
        )
        return postgre_sql_node
    except Exception as e:
        raise Exception(f"Error fetching data connection: {str(e)}")

def transform_to_mtf_claim(filenm_parse_result: FileNameParseResult, dtl_df: DynamicFrame) -> DynamicFrame:
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

    # Covert Sender_System_Datetime
    file_time = filenm_parse_result.sender_system_datetime
    file_header_ts = None

    if re.match(r'^\d{8} \d{6}$', file_time):
        file_header_ts = datetime.strptime(file_time, "%Y%m%d %H%M%S")

    # Define a transformation map for the Dynamic Frame
    def transform_map(record):
        npi_num = None
        ncpdp_id = None

        if record.get('SRVC_PROV_ID_QLFR') == "01" or record.get('SRVC_PROV_ID_QLFR') == "1":
            npi_num = int(record.get('SRVC_PROV_ID').strip())
        elif record.get('SRVC_PROV_ID_QLFR') != "01" and record.get('SRVC_PROV_ID_QLFR') == "07" and record.get('ALT_SRVC_PROV_ID_QLFR') == "01":
            npi_num = int(record.get('ALT_SRVC_PROV_ID').strip())
        elif record.get('SRVC_PROV_ID_QLFR') != "1" and record.get('SRVC_PROV_ID_QLFR') == "7" and record.get('ALT_SRVC_PROV_ID_QLFR') == "1":
            npi_num = int(record.get('ALT_SRVC_PROV_ID').strip())

        if record.get('SRVC_PROV_ID_QLFR') == "07" or record.get('SRVC_PROV_ID_QLFR') == "7":
            ncpdp_id = int(record.get('SRVC_PROV_ID').strip())
        elif record.get('SRVC_PROV_ID_QLFR') != "07" and record.get('SRVC_PROV_ID_QLFR') == "01" and record.get('ALT_SRVC_PROV_ID_QLFR') == "07":
            ncpdp_id = int(record.get('ALT_SRVC_PROV_ID').strip())
        elif record.get('SRVC_PROV_ID_QLFR') != "7" and record.get('SRVC_PROV_ID_QLFR') == "1" and record.get('ALT_SRVC_PROV_ID_QLFR') == "7":
            ncpdp_id = int(record.get('ALT_SRVC_PROV_ID').strip())

        new_record = {
            'received_dt': current_time,
            'received_id': None,
            'internal_claim_num': None,
            # 'xref_internal_claim_num': str(None), # Do not need to pass in
            'file_header_ts': file_header_ts,
            'mtf_curr_claim_stus_ref_cd': str('ING'),
            'medicare_src_of_coverage': str('PartD'),
            'src_claim_stus_ref_cd': str(record.get('REC_TYPE').strip()),
            'srvc_dt': datetime.strptime(record.get('DOS'), "%Y%m%d").date() if re.match(r'^\d{8}$', record.get('DOS')) else None,
            'rx_srvc_ref_num': str(record.get('PRES_SRVC_REF_NO').strip()),
            'fill_num': int(record.get('FILL_NUM').strip()) if record.get('FILL_NUM') is not None else int(None),
            'srvc_npi_num': npi_num,
            'ncpdp_id': ncpdp_id,
            'src_claim_type_cd': str(record.get('ADJSTMT_DLTN_CD').strip()) if record.get('ADJSTMT_DLTN_CD') == 'A' or record.get('ADJSTMT_DLTN_CD') == 'D' else 'O' if record.get('ADJSTMT_DLTN_CD') == '' or record.get('ADJSTMT_DLTN_CD') == ' ' else str(None),
            'ndc_cd': str(record.get('PROD_SRVC_ID').strip()),
            'quantity_dispensed': float(record.get('QTY_DSPNSD')) / 1000 if record.get('QTY_DSPNSD') is not None else None,
            'days_supply': int(record.get('DAYS_SUPLY').strip()),
            'indicator_340b_yn': bool(record.get('IND_340B').strip()),
            'orig_submitting_contract_num': str(record.get('SUB_CNTRCT').strip()),
            'claim_control_num': str(record.get('CLM_CNTL_NUM').strip()),
            'card_holder_id': str(record.get('CARDHLDR_ID').strip()),
            'prescriber_id': str(record.get('PRSCRBR_ID').strip()),
            'pde_error_cnt': int(record.get('ERR_REC_CNT').strip()) if record.get('ERR_REC_CNT').strip().isdigit() else None,
            'pde_error_cd_1': str(record.get('ERR_CD1').strip()),
            'pde_error_cd_2': str(record.get('ERR_CD2').strip()),
            'pde_error_cd_3': str(record.get('ERR_CD3').strip()),
            'pde_error_cd_4': str(record.get('ERR_CD4').strip()),
            'pde_error_cd_5': str(record.get('ERR_CD5').strip()),
            'pde_error_cd_6': str(record.get('ERR_CD6').strip()),
            'pde_error_cd_7': str(record.get('ERR_CD7').strip()),
            'pde_error_cd_8': str(record.get('ERR_CD8').strip()),
            'pde_error_cd_9': str(record.get('ERR_CD9').strip()),
            'pde_error_cd_10': str(record.get('ERR_CD10').strip()),
            'insert_ts': current_time,
            'insert_user_id': int(1),
            'update_ts': current_time,
            'update_user_id': int(1),
            'data_src_cd': filenm_parse_result.data_src_cd,
            'pde_rcvd_dt': record.get('PDE_RCVD_DT'),
            'pde_rcvd_tm': record.get('PDE_RCVD_TM'),
            'pkg_audt_key_id': int(record.get('PKG_AUDT_KEY_ID')),
            'phrmcy_srvc_type_cd': str(record.get('PHRMCY_SRVC_TYPE_CD').strip()),
            # TODO: For Error condition remove record if not 01 for "PRSCRBR_QLFR"
            # "PRSCRBR_QLFR": "01",
        }
        return new_record

    print(f"Count of DyF coming in: {dtl_df.count()}")

    print("Before mapping... ")
    dtl_df.show(20)
    dtl_df.printSchema()
    print(f"Count of DyF before map in: {dtl_df.count()}")

    # Apply mapping to the Dynamic Frame
    map_mtf_claim_df = dtl_df.map(f = transform_map)

    print("after mapping... ")
    map_mtf_claim_df.show(20)
    map_mtf_claim_df.printSchema()
    print(f"Count of DyF after map in: {map_mtf_claim_df.count()}")

    # With the logic present, some values have a choice that we need to resolve before we can save to DB
    mtf_claim_df = map_mtf_claim_df.resolveChoice(specs=[
        ("internal_claim_num", "cast:string"),
        ("srvc_npi_num", "cast:long"),
        ("received_id", "cast:long"),
        ("received_dt", "cast:date"),
        ("insert_ts", "cast:timestamp"),
        ("update_ts", "cast:timestamp"),
        ("file_header_ts", "cast:timestamp"),
        ("srvc_dt", "cast:date"),
        ("ncpdp_id", "cast:long")
    ])

    print("after spec is defined... ")
    mtf_claim_df.show(10)
    mtf_claim_df.printSchema()
    print(f"Count of DyF after spec in: {mtf_claim_df.count()}")

    return mtf_claim_df

def send_to_sqs_batch(data : DynamicFrame, sqs_queue_url: StringType):
    """
    Send a batch of messages to an SQS queue using a DynamicFrame.
    
    Args:
    - data: The DynamicFrame containing data to be sent.
    """
    # Initialize boto3 SQS client
    sqs_client = boto3.client('sqs')
    entries = []
    
    for row in data.toDF().select("received_id", "received_dt", "internal_claim_num").collect():
        # Convert the row to a dictionary and then to a JSON string
        json_data = json.dumps({
            "received_id": row["received_id"],
            "received_dt": str(row["received_dt"]),
            "internal_claim_num": row["internal_claim_num"]
        })
        # Add the JSON string to the entries list
        entries.append({
            'Id': str(f"msg-{len(entries)+1}"),  # Unique ID for each message in the batch
            'MessageBody': json_data  # Message body
        })
        
        # Send messages in batches of 10 (SQS limit)
        if len(entries) == 10:
            try:
                response = sqs_client.send_message_batch(
                    QueueUrl=sqs_queue_url,
                    Entries=entries
                )
                print(f"Sent {len(entries)} messages in a batch. Batch Message IDs: {[msg['MessageId'] for msg in response['Successful']]}")
            except Exception as e:
                print(f"Error sending batch to SQS: {e}")
            finally:
                # Clear the entries after sending
                entries = []
    
    # Send any remaining messages in the final batch
    if entries:
        try:
            response = sqs_client.send_message_batch(
                QueueUrl=sqs_queue_url,
                Entries=entries
            )
            print(f"Sent {len(entries)} messages in a batch. Batch Message IDs: {[msg['MessageId'] for msg in response['Successful']]}")
        except Exception as e:
            print(f"Error sending batch to SQS: {e}")

def save_mtf_claim(mtf_claim_df: DynamicFrame, glue_context, connection_options):
    glue_context.write_dynamic_frame.from_options(
        frame=mtf_claim_df,
        connection_type='postgresql',
        connection_options=connection_options
    )
    print("Write to DB Complete")

def main():
    # Glue Spark Session
    args_list = ['JOB_NAME', 'S3_BUCKET', 'CONNECTION_NAME', 'SQS_QUEUE_URL']
    args = getResolvedOptions(sys.argv, args_list)
    sc = SparkContext().getOrCreate()
    glue_context = GlueContext(sc)
    glue_spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(args['JOB_NAME'], args)
    s3_client = boto3.client('s3')

    # Defined Job variables
    s3_bucket = args['S3_BUCKET']
    connection_name = args['CONNECTION_NAME']
    sqs_queue_url = args['SQS_QUEUE_URL']

    # DB Config
    table_name_claim = "claim.mtf_claim"
    table_name_data_source = "claim.data_source_ref"
    table_name_ingested_vw = "claim.ingested_mtf_claim_vw"

    # hhs-cms-mdrng-mtfdm-dev-ddps
    # testCreateParquetIn/
    # out/
    # SAMEER_DDPS_TEST_PARQUET_AS_CSV.csv-20250225-190016/
    # file = part-00000-04568f2e-6fc3-4e11-bac5-ee2b2a8e34ef-c000.snappy.parquet

    try:
        # Used with Data Connection
        connection_options_data_source = {
            "useConnectionProperties": "true",
            "dbtable": table_name_data_source,
            "connectionName": connection_name,
        }

        connection_options_ingested_vw = {
            "useConnectionProperties": "true",
            "dbtable": table_name_ingested_vw,
            "connectionName": connection_name,
        }

        connection_options_claim = {
            "useConnectionProperties": "true",
            "dbtable": table_name_claim,
            "connectionName": connection_name,
        }

        data_src_ref_df = fetch_mtf_data(glue_context, connection_options_data_source)
        # Print the result
        print("data from db: ")
        data_src_ref_df.show()

        files_to_process = get_file_list_from_s3(s3_client, s3_bucket)

        # Not sure if we really want to do this.
        for file in files_to_process:
            s3_input_path = f"s3://{s3_bucket}/{file}"

            # Parse dropped file name and pass in data_src_ref_df for translation of source data
            # filenm_parse_result = parse_file(s3_input_path, data_src_ref_df)

            # ***file name result *** FileNameParseResult(data_src_cd=0, sender_system_datetime=20250207 121035)
            filenm_parse_result = FileNameParseResult("0", "20250207 121035")
            print("***file name result ***")
            print(filenm_parse_result)

            dtl_df = parse_claim_data_s3_parquet(glue_context, s3_input_path)

            # Transform data from parquet to DB compatible
            mtf_claim_df = transform_to_mtf_claim(filenm_parse_result, dtl_df)

            # Save transformed data
            # save_mtf_claim(mtf_claim_df, glue_context, connection_options_claim)

            # Move files after they have been saved
            # move_s3_file(s3_client, s3_bucket, s3_input_path)

        # Place SQS batch sending message here.
        # df_ingested = fetch_mtf_data(glue_context, connection_options_ingested_vw)
        # print(f"This should be ingested {df_ingested.show(1)}")

        # send_to_sqs_batch(df_ingested, sqs_queue_url)

        # Fetch data from mtf_claim table
        # db_df = fetch_mtf_data(glue_context, connection_options_claim)

        # Display 10 rows of data for debugging
        # print("fetching 10 records after wrote to DB")
        # db_df.show(10)

    except Exception as e:
        print(f"ETL Job Failed: {str(e)}")
        raise e
    finally:
        # Stop the Spark session
        glue_spark.stop()

if __name__ == "__main__":
    main()
