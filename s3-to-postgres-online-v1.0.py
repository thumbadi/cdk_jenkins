import sys
import boto3
import subprocess
subprocess.call([sys.executable, "-m", "pip", "install", "psycopg2-binary"])
import psycopg2
import pandas as pd
from io import StringIO
import json


# --- S3 Config ---
bucket = 'pyspark-demo12'
input_dir = "input"
completed_dir = "completed"
error_dir = "error"

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

def move_s3_files(source_key,target_key):
    # Copy the file from input to completed
    s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': source_key}, Key=target_key)
    s3.delete_object(Bucket=bucket, Key=source_key)


# --- PostgreSQL Config ---

table_name = 'claim.mfp_dtl'
db = 'mtf'
host = "database-1.ch8qiq2uct5o.us-east-1.rds.amazonaws.com"
secret_name = "rds!db-dcb3ad0e-5246-450e-9f85-44450fccbddb"
credentials = get_secret(secret_name)

pg_config = {
    'host': host,
    'port': 5432,
    'dbname': db,
    'user': credentials['username'],
    'password': credentials['password']
}

# --- Read file from S3 ---
s3 = boto3.client('s3')
response = s3.list_objects_v2(Bucket=bucket, Prefix=input_dir)
psv_files = [item['Key'] for item in response.get('Contents', []) if item['Key'].endswith('.psv')]


for key in psv_files:
    print(f"Processing file: {key}")    
    obj = s3.get_object(Bucket=bucket, Key=key)
    psv_data = obj['Body'].read().decode('utf-8')

    # --- Load into DataFrame ---
    df = pd.read_csv(StringIO(psv_data), sep='|')
    new_columns = [
        "ipay_year",
        "drug_id",
        "drug_name",
        "active_ingredient",
        "ndc_9",
        "ndc_cd",
        "xref_ndc_11",    
        "mfp_eff_dt",
        "mfp_end_dt",
        "mfp_per_30",
        "mfp_per_unit",
        "mfp_per_pkg",
        "asof_dt",
        "type_of_update",
        "remarks"
        # "insert_user_id",
        # "insert_ts",
        # "update_user_id",
        # "update_ts"    

    ]
    df.columns = new_columns
    df['ndc_cd'] = df['ndc_cd'].astype(str).str.zfill(11)
    df['mfp_eff_dt']= pd.to_datetime(df['mfp_eff_dt'].astype(str), format='%Y%m%d')
    if df['mfp_eff_dt'].isna().any():
        df['mfp_eff_dt'] = df['mfp_eff_dt'].fillna(pd.Timestamp('2262-04-11'))
    df['mfp_end_dt']= pd.to_datetime(df['mfp_end_dt'].astype(str), format='%Y%m%d')
    if df['mfp_end_dt'].isna().any():
        df['mfp_end_dt'] = df['mfp_end_dt'].fillna(pd.Timestamp('2262-04-11'))
    df['asof_dt']= pd.to_datetime(df['asof_dt'].astype(str), format='%Y%m%d')
    if df['asof_dt'].isna().any():
        df['asof_dt'] = df['asof_dt'].fillna(pd.Timestamp('2262-04-11'))

    # --- Connect to Postgres ---
    conn = psycopg2.connect(**pg_config)
    cur = conn.cursor()

    # --- Truncate table ---
    cur.execute(f'TRUNCATE TABLE {table_name};')
    conn.commit()

    # Use COPY for fast bulk insert
    buffer = StringIO()
    df.to_csv(buffer, sep='|', header=False, index=False)


    try:
        buffer.seek(0)  # Always rewind before reading
        cur.copy_expert("""
            COPY claim.mfp_dtl (
                ipay_year, drug_id, drug_name, active_ingredient, ndc_9, ndc_cd, 
                xref_ndc_11, mfp_eff_dt, mfp_end_dt, mfp_per_30, mfp_per_unit, 
                mfp_per_pkg, asof_dt, type_of_update, remarks
            )
            FROM STDIN WITH (FORMAT csv, DELIMITER '|', NULL '')
        """, buffer)
        conn.commit()
        fname = key.split("/")[1]
        move_s3_files(key,f"{completed_dir}/{fname}")
    except Exception as e:
        print("COPY failed:", e)
        conn.rollback()
        fname = key.split("/")[1]
        move_s3_files(key,f"{error_dir}/{fname}")

    # --- Cleanup ---
    cur.close()
    conn.close()

    print("PSV data inserted successfully.")
