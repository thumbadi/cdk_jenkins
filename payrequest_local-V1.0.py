import psycopg2, os
from io import StringIO
import pyarrow as pa
import boto3
# import pandas as pd
import json
import base64
import re, datetime, pytz
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import boto3
from botocore.exceptions import NoCredentialsError
from pyspark.sql.functions import col,lit
from pyspark.sql.types import DateType, IntegerType, LongType

JAR_PATH = "c:/csv/postgresql-42.7.5.jar"
spark = SparkSession.builder \
    .appName("ReadFromRedshift") \
    .config("spark.driver.extraClassPath", JAR_PATH) \
    .config("spark.executor.extraClassPath", JAR_PATH) \
    .getOrCreate()

AWS_ACCESS_KEY = ""
AWS_SECRET_KEY = ""
AWS_REGION = "us-east-1"

s3 = boto3.client("s3",aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY)

mtf_db="mtf"
schema = "claim"
envir = "DEV"
port=5432

def get_secret(secret_name):
    """Fetch secret from AWS Secrets Manager."""
    region_name = "us-east-1"  # Modify if needed
    client = boto3.client('secretsmanager', region_name=AWS_REGION,aws_access_key_id=AWS_ACCESS_KEY,
                       aws_secret_access_key=AWS_SECRET_KEY)

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

def postgres_query(jdbc_url,db,schema,table,**kwargs):    
    seq = kwargs.get("action")
    if seq == "read":
        query = f"""(SELECT 
                        A.*,
                        B.hold_dt,
                        B.release_dt
                    FROM claim.process_id_table A
                    LEFT JOIN claim.bank_feedback_dtl B 
                        ON A.payeeid::int8 = B.payee_id::int8)"""
        # query = f"""(SELECT
#     CURRENT_DATE AS "TransactionDate",
#     d.payee_id AS "PayeeId",
#     c.internal_claim_num AS "TransactionId",
#     x.mfr_ref_id AS "ManufacturerId",-- <<== replaced drug_id with mfr_ref_id
#     c.drug_id AS "DrugId",  
#     d.org_id AS "DispenserId",
#     p.disb_amt AS "DisbursementAmount",
#     pcr.pymt_category_desc AS "PaymentCategory",
#     c.received_dt,
#     c.received_id
# FROM claim.mtf_claim c
# JOIN claim.mtf_claim_manufacturer m
#     ON c.received_dt = m.received_dt
#     AND c.received_id = m.received_id
# JOIN claim.mtf_claim_de_tpse d
#     ON c.received_dt = d.received_dt
#     AND c.received_id = d.received_id
# JOIN claim.mtf_claim_pymt_request p
#     ON c.received_dt = p.claim_received_dt
#     AND c.received_id = p.claim_received_id
# JOIN claim.pymt_category_ref pcr
#     ON p.pymt_category_cd = pcr.pymt_category_cd
# INNER JOIN (
#     SELECT m.claim_loctn_cd
#     FROM claim.mtf_claim_msg_loctn_map m
#     WHERE m.claim_msg_cd = '151'
# ) l ON c.mtf_claim_curr_loctn_cd = l.claim_loctn_cd
# LEFT JOIN (
#     SELECT 
#         a.mfr_id, 
#         b.mfr_ref_id, 
#         a.drug_id
#     FROM shared.drug_dtl a
#     LEFT JOIN shared.mfr_dtl b ON a.mfr_id = b.mfr_id
# ) x 
# ON m.mfr_id::text = x.mfr_id::text)"""

        df = spark.read.format("jdbc") \
                .option("url", f"jdbc:postgresql://{host}:{5432}/{db}") \
                .option("dbtable", query) \
                .option("user", credentials['username']) \
                .option("password", credentials['password']) \
                .option("driver", "org.postgresql.Driver") \
                .load()
        return df
        
    elif seq == "insert":
        df = kwargs.get("data")
        df.write \
            .format("jdbc") \
            .option("url", f"jdbc:postgresql://{host}:{5432}/{db}") \
            .option("dbtable", f"{schema}.{table}") \
            .option("user", credentials['username']) \
            .option("password", credentials['password']) \
            .mode("append") \
            .save()
    elif seq == "update":
        id_list = kwargs.get("data")
    
        conn = psycopg2.connect(
        dbname=mtf_db,
        user=credentials['username'],
        password=credentials['password'],
        host=host,
        port=port
        )
        cursor = conn.cursor()
        
        query = """
            UPDATE claim.mtf_claim \
            SET mtf_claim_curr_loctn_cd = %s \
            WHERE internal_claim_num = (%s)
        """       
             
        if id_list:  # Prevent empty execution
            cursor.executemany(query,[tuple(item) for item in id_list])

        
        conn.commit()
        cursor.close()
        conn.close()
    

host = "database-1.ch8qiq2uct5o.us-east-1.rds.amazonaws.com"
secret_name = "rds!db-dcb3ad0e-5246-450e-9f85-44450fccbddb"
credentials = get_secret(secret_name)

schema_data =  [
    {
    "read" : {
        "table_name" : "mtf_claim",
        "schema" : "claim"
    }},
    {"insert" : {
        "table_name1" : "mtf_claim_process_loctn",
        "schema1" : "claim",
        "table_name2" : "mtf_claim_process_msg",
        "schema2" : "claim"
    }}
    ]

payfile_schema = pa.schema([
    pa.field('TransactionDate', pa.date64(), nullable=False),
    pa.field('PayeeId', pa.string(), nullable=False),
    pa.field('TransactionId', pa.string(), nullable=False),
    pa.field('ManufacturerId', pa.string(), nullable=False),
    pa.field('DrugId', pa.string(), nullable=False),
    pa.field('DispenserId', pa.string(), nullable=False),
    pa.field('DisbursementAmount', pa.decimal128(14,2), nullable=False),
    pa.field('PaymentCategory', pa.string(), nullable=False)
])

env="dev"
stage_error = False
s3_bucket = "pyspark-demo"
utc_now = datetime.datetime.utcnow()
eastern = pytz.timezone('America/New_York')
utc_minus_4 = pytz.utc.localize(utc_now).astimezone(eastern)

code_mapping = [
    {"expired": {
        "claim_msg_cd" : "201",
        "claim_loctn_cd" : "050"
    }},
    {"not_expired":{
        "claim_msg_cd" : "155",
        "claim_loctn_cd" : "301"
    }}
    ]
for entry in schema_data:
    if stage_error == False:
        for key in entry.keys():  
            bucket_name = s3_bucket
            if key == "read":
                schema = entry[key]["schema"]
                table = entry[key]["table_name"] 
                view_df = postgres_query(host,mtf_db,schema,table,action="read")
                if view_df.isEmpty() == False:
                    df_with_empty_flag = view_df.withColumn("empty_flag",F.when(
                        ((F.col("release_dt").isNull()) & (F.col("hold_dt").isNull())),
                        F.lit("Yes")).otherwise(F.lit("No"))
                        )
                    df_filtered_empty = df_with_empty_flag.filter(F.col("empty_flag") == "Yes")
                    if not (df_filtered_empty.isEmpty()):
                        ts = utc_minus_4.strftime("%Y%m%d_%H%M%S")
                        file_name=f"mtfdm_{env}_PaymentRequestData_{ts}.parquet"
                        file_path = f"payrequest/ready/{file_name}"
                        df = df_filtered_empty.toPandas()
                        columns_to_remove = ["received_dt","received_id","empty_flag",'hold_dt', 'release_dt']
                        df_parquet = df.drop(columns=columns_to_remove)
                        df_parquet.to_parquet(f"c:/tmp/{file_name}",schema=payfile_schema)
                        insert_ts = utc_minus_4.strftime("%Y-%m-%d %H:%M:%S")
                        df['insert_ts'] = insert_ts
                        meta_file_size = os.path.getsize(f"c:/tmp/{file_name}")
                        s3_client = boto3.client('s3')
                        s3_client.upload_file(f"c:/tmp/{file_name}", bucket_name, file_path)
                        df_final = df
                        job_id = get_GlueJob_id()
                        meta_info.append([job_id,"008",file_name,meta_file_size,df.shape[0],"COMPLETED",-1,insert_ts])
                        logger.info('Parquet file has been created.')
                        df['loctn_cd'] = '050'
                        icn_list = [tuple(map(str, x)) for x in df[['loctn_cd', 'transactionid']].values.tolist()]
                        postgres_query(host,mtf_db,"claim","mtf_claim",action="update",data=icn_list)
                        continue
                    else:
                        stage_error = True
            elif key == "insert" and stage_error == False:
                schema = entry[key]["schema1"]
                table = entry[key]["table_name1"] 
                today = F.current_date()
                df_with_flag = view_df.withColumn("expired_flag",F.when(
                        ((F.col("release_dt").isNull()) & (F.col("hold_dt") <= today))
                        | ((F.col("release_dt").isNotNull()) & (F.col("release_dt") <= today)),
                        F.lit("Yes")).otherwise(F.lit("No"))
                        )
                for item in code_mapping:
                    for flag in item.keys():
                        if flag == "expired":
                            df_filtered = df_with_flag.filter(F.col("expired_flag") == "Yes")
                        else:
                            df_filtered = df_with_flag.filter(F.col("expired_flag") == "No")
                        if not(df_filtered.isEmpty()):
                            claim_msg_cd = item.get(flag)["claim_msg_cd"]
                            claim_loctn_cd = item.get(flag)["claim_loctn_cd"]
                            df_final = (df_filtered
                                .withColumn("claim_msg_cd", F.lit(claim_msg_cd))
                                .withColumn("claim_loctn_cd", F.lit(claim_loctn_cd))
                                .withColumn("insert_user_id", F.lit(-1))
                            )
                            df_final = (df_final.withColumn("received_dt", F.col("received_dt").cast(DateType())))
                            df_final = df_final.withColumnRenamed("TransactionId", "internal_claim_num")
                            icn_list = [tuple(map(str, row)) for row in df_final.select("claim_loctn_cd", "internal_claim_num").collect()]
                            postgres_query(host,mtf_db,"claim","mtf_claim",action="update",data=icn_list)

                            df_final = df_final.select(
                                    "received_dt",
                                    "received_id",
                                    "internal_claim_num",
                                    "claim_msg_cd",
                                    "claim_loctn_cd",
                                    "insert_user_id"
                                )
                            postgres_query(host,mtf_db,schema,table,action="insert", data=df_final)
                            table = entry[key]["table_name2"]
                            schema = entry[key]["schema2"] 
                            df_msg = df_final.select(
                                    "received_dt",
                                    "received_id",
                                    "internal_claim_num",
                                    "claim_msg_cd",
                                    "insert_user_id"
                                )
                            postgres_query(host,mtf_db,schema,table,action="insert", data=df_msg)
                        else:
                            print(f"No Records are found in {flag}")
            # elif key == "insert2":
            #     df_msg = df_final.select(
            #         "received_dt",
            #         "received_id",
            #         "internal_claim_num",
            #         "claim_msg_cd",
            #         "insert_user_id"
            #     )
            #     postgres_query(host,mtf_db,schema,table,action="insert", data=df_msg)
                # target_col = ["received_dt", "received_id", "internal_claim_num", "claim_msg_cd", "claim_loctn_cd", "insert_user_id"]
            # elif key == "update":
            #     postgres_query(host,mtf_db,schema,table,action="update", data=df_msg)