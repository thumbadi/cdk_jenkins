import sys
import logging
from awsglue.utils import getResolvedOptions
import boto3
import json
import psycopg2
from psycopg2.extras import execute_values
import snowflake.connector
from datetime import datetime
from datetime import timedelta
from urllib.parse import urlparse
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import base64
from datetime import datetime
import pytz

def get_secret(secret_name):
    logger.info('getting secret')
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
        logger.error(f"Error retrieving secret: {e}")
        raise e
def get_snowflake_secret():
    logger.info('getting secret')
   
    # Create an SSM client
    ssm_client = boto3.client('ssm')
    try:
       
         # Retrieve a parameter
        parameter_name = '/mtfdm/idr/snowflake'
        response = ssm_client.get_parameter(
            Name=parameter_name,
            WithDecryption=True  # Set to True if the parameter is encrypted
        )
        parameter_value = response['Parameter']['Value']
        json_data = json.loads(parameter_value)
       

        return json_data

        
    except Exception as e:
        logger.error(f"Error retrieving secret: {e}")
        raise e
    
# Create a Glue client
logger = logging.getLogger()
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)
glue_client = boto3.client('glue')
args = getResolvedOptions(sys.argv, ['IDR_DBNAME', 'IDR_DB_TYPE'])
ETZ = pytz.timezone('US/Eastern') 
idr_conn = None
mtf_conn = None

try:
 
    mtf_connection = glue_client.get_connection(Name='MTFDMDataConnector')
    mtf_connection_options = mtf_connection['Connection']['ConnectionProperties']
    jdbc_url = mtf_connection['Connection']['ConnectionProperties']['JDBC_CONNECTION_URL']

    parsed_url = urlparse(jdbc_url.replace("jdbc:", ""))


    # We need 3 SQLs per table
    # 1. to Select from IDR db
    # 2. to delete existing data in mtf shared schema
    # 3. to insert data in mtf shared schema
    mdfp_ndc_select_sql = "Select distinct(ndc_cd) FROM shared.mfp_dtl"
    mddb_del_sql= "delete from shared.mddb_dtl"
    mddb_insert_sql = "INSERT INTO shared.mddb_dtl (ndc_cd,ndc_price_dt,ndc_unit_amt,insert_user_id,insert_ts) VALUES %s"


    #SECRET_ID
    

    mtf_secret=get_secret(mtf_connection_options['SECRET_ID'])

    #get IDR db type (snowflake or PostgreSQL)
    idr_db_type=args['IDR_DB_TYPE']
    if idr_db_type.lower() == "snowflake":
        #snokeflake connection details from SSM
        snowflake_secret=get_snowflake_secret()
        decoded_bytes = base64.b64decode(snowflake_secret['base64_private_key'])
        private_key = serialization.load_pem_private_key(
            decoded_bytes.decode('utf-8').encode('utf-8'),
            password=None,  # If your key is encrypted, provide the password here
            backend=default_backend()
        )

        idr_conn = snowflake.connector.connect(
            user=snowflake_secret['user'],
            private_key=private_key,
            account=snowflake_secret['account'],
            warehouse=snowflake_secret['warehouse'],
            database=snowflake_secret['database'],
            schema=snowflake_secret['schema'],
            role=snowflake_secret['role'],
            autocommit=False
        )
    else:
        # Connect to Aurora PostgreSQL database
        
        idr_connection = glue_client.get_connection(Name='IDRDataConnector')
        idr_connection_options = idr_connection['Connection']['ConnectionProperties']
        idr_secret=get_secret(idr_connection_options['SECRET_ID'])
        idr_conn = psycopg2.connect(
            dbname=args['IDR_DBNAME'],
            user=idr_secret['username'],
            password=idr_secret['password'],
            host=idr_secret['host'],
            port=idr_secret['port']
        )


     # Connect to Aurora PostgreSQL database
    mtf_conn = psycopg2.connect(
        dbname=parsed_url.path.lstrip('/'),
        user=mtf_secret['username'],
        password=mtf_secret['password'],
        host=mtf_secret['host'],
        port=mtf_secret['port']
    )
    mtf_conn.autocommit = False
    idr_cursor = idr_conn.cursor()

    mtf_cursor = mtf_conn.cursor()
    #get MFP eligible NDC from MTF DB first
    mtf_cursor.execute(mdfp_ndc_select_sql)
    mfp_ndc = mtf_cursor.fetchall()
    if len(mfp_ndc) == 0:
      logger.error("No MFP eligible NDC found in MTF")
      raise ValueError("No MFP eligible NDC found in MTF")
    # Extract values from the result_set
    ndcs = [row[0] for row in mfp_ndc]  
    current_time = datetime.now(ETZ).strftime("%Y-%m-%d %H:%M:%S.%f")
    #loop over the ndc list of prepare IDR select SQL
    # Prepare the SQL query with placeholders
    mddb_select_sql = "Select ndc_cd,ndc_price_dt,ndc_unit_amt,'-1' as insert_user_id,  '"+current_time+ "' as insert_ts "\
    " FROM v2_mdcr_ndc_mddb_price WHERE ndc_rec_cd = 'Q01' and  ndc_cd IN ({placeholders})".format(
        placeholders=", ".join("%s" for _ in ndcs)
    )
    #call load_data_in_table for each table in scope
    #load_data_in_table(idr_cursor,mtf_cursor,mddb_select_sql,mddb_del_sql,mddb_insert_sql)
    idr_cursor.execute(mddb_select_sql,ndcs)
    rows = idr_cursor.fetchall()
    logger.info(len(rows))
    if len(rows) == 0:
      logger.error("IDR returned 0 rows")
      raise ValueError("IDR returned 0 rows")
    mtf_cursor.execute(mddb_del_sql)
    logger.info('done delete')
    execute_values(mtf_cursor, mddb_insert_sql, rows)
    logger.info('done insert')
    # Commit the changes to the database
    mtf_conn.commit()
    logger.info('done commit')
   
    
except Exception as e:
    logger.error(f"Error: {e}")
    if idr_conn is not None:
        idr_conn.rollback()
    if mtf_conn is not None:
        mtf_conn.rollback()
    raise e
finally:
    # Close the database connections
    if idr_conn is not None:
        idr_conn.close()
    if mtf_conn is not None:
        mtf_conn.close()
      
    logger.info("Connections closed.")
   
