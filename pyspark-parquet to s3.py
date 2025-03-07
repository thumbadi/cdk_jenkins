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
from awsglue.transforms import *
from awsglue.job import Job
subprocess.call([sys.executable, "-m", "pip", "install", "--user", "psycopg2-binary"])
import psycopg2

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def postgres_query(db,schema,table,**kwargs):

    host = "database-1.ch8qiq2uct5o.us-east-1.rds.amazonaws.com"

    query = f"""(select c.internal_claim_num, c.xref_internal_claim_num, 
        c.received_dt, c.src_claim_type_cd, null mra_error_code,
        c.medicare_src_of_coverage, c.srvc_dt, c.rx_srvc_ref_num, 
        null fill_num, '01' service_provider_id_qualifier, c.srvc_npi_num, 
        null prescriber_id_qualifier,
        c.prescriber_id, c.ndc_cd, c.quantity_dispensed, 
        c.days_supply, c.indicator_340b_yn, c.orig_submitting_contract_num, c.mtf_curr_claim_stus_ref_cd,
        b.wac_amt, b.mfp_amt, b.sdra_amt, a.pymt_pref,
        null pre_mfp_refund_paid_product, null pre_mfp_refund_paid_amt, 
        null pre_mfp_refund_paid_date, null pre_mfp_refund_paid_qty, 
        null pre_mfp_refund_paid_method,
        null mra_pm_switch, null mfp_refund_amt, null npi_mfp_refund, 
        null qty_of_selected_drug, null amt_mfp_refund_by_mtfpm, 
        null mfp_refund_adj, null mfp_refund_trans,
        d.mfr_id, d.mfr_name
        from claim.mtf_claim c join claim.mtf_claim_de_tpse a on a.received_dt = c.received_dt and a.received_id = c.received_id
        join  claim.mtf_claim_pricing b on b.received_dt = c.received_dt and b.received_id = c.received_id
        join  claim.mtf_claim_manufacturer d on d.received_dt = c.received_dt and d.received_id = c.received_id
        where c.mtf_curr_claim_stus_ref_cd = 'MRN')
        """
    df = spark.read.format("jdbc") \
        .option("url", f"jdbc:postgresql://{host}:{5432}/{db}") \
        .option("dbtable", query) \
        .option("user", credentials['username']) \
        .option("password", credentials['password']) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    return df

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
    
schema_data =  {
            "table_name" : "mtf_claim",
            "schema" : "claim",
            "database" : "mtf"
        }

secret_name = "rds!db-dcb3ad0e-5246-450e-9f85-44450fccbddb"
credentials = get_secret(secret_name)

database = schema_data["database"]
schema = schema_data["schema"]
table = schema_data["table_name"]

mfg_result = postgres_query(database,schema,table)

# if mfg_result.isEmpty() == False:
mfr_list = [row for row  in mfg_result.select("mfr_id", "mfr_name").distinct().collect()]

if len(mfr_list) == 0:
    print("No records found in the result")
else:
    for row in mfr_list:
        id_value = row["mfr_id"]
        mfg_name_value = row["mfr_name"]
        df_filtered = mfg_result.filter((col("mfr_id") == id_value))
        ts = datetime.datetime.today().strftime("%m%d%Y.%H%S")
        file_path = f"{mfg_name_value}-{id_value}/mrn/outbound/{mfg_name_value}.{ts}.parquet"
        df = df_filtered.toPandas()
        df.to_parquet(f"/tmp/{mfg_name_value}.{ts}.parquet")
        bucket_name = "hhs-cms-mdrng-mtfdm-dev-mfr"

        s3_client = boto3.client('s3')
        s3_client.upload_file(f"/tmp/{mfg_name_value}.{ts}.parquet", bucket_name, file_path)
job.commit()