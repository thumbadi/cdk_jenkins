import psycopg2
import boto3
import json
import base64

AWS_ACCESS_KEY = "AKIA5V6I7K7SDHB6P3EC"
AWS_SECRET_KEY = "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"
AWS_REGION = "us-east-1"

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
# Retrieve credentials from Secrets Manager
credentials = get_secret("rds!db-1b56c8d7-8b7e-46fa-a7c7-d30322ab4187")


# PostgreSQL connection parameters
DB_HOST = "database-1.cwnacmkaedhp.us-east-1.rds.amazonaws.com"  # e.g., "your-db-name.c9akciq32.rds.amazonaws.com"
DB_PORT = 5432
DB_NAME = "postgres"
DB_USER = credentials['username']
DB_PASSWORD = credentials['password']

# AWS SQS details
SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/940482451428/postgres-sqs-api"

# AWS credentials (access key and secret key)
# AWS_ACCESS_KEY = "your-access-key"
# AWS_SECRET_KEY = "your-secret-key"
# AWS_REGION = "your-region"  # e.g., "us-east-1"

# Step 1: Connect to PostgreSQL
def get_postgres_data():
    try:
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = connection.cursor()

        # Retrieve data from a specific table, change the query as per your needs
        cursor.execute("select * from public.demo_loadtest;")  # Example query, adjust as needed
        data = cursor.fetchall()

        columns = [
    "record_id", "sequence_no", "mtf_responsecode", "adj_deletioncode",
    "date_of_service", "pres_serviceref_no", "fill_number", "service_providerId_qual",
    "service_provider_id", "alter_service_provid_qual", "alter_service_provid_id",
    "prescriberId_qual", "prescriber_id", "filler_1", "product_service_id", 
    "qty_dispensed", "days_supply", "indicator", "filler_2", "orgi_submit_cont", 
    "claim_control_no", "card_holder_id"
]

        # Step 2: Process the data (e.g., convert to JSON or any desired format)
        rows = []
        # for row in data:
        #     rows.append(
        #         {
        #     'record_id': row[0],
        #     'sequence_no': row[1],
        #     'mtf_responsecode': row[2],
        #     'adj_deletioncode': row[3],
        #     'date_of_service': row[4],
        #     'pres_serviceref_no': row[5],
        #     'fill_number': row[6],
        #     'service_providerId_qual': row[7],
        #     'service_provider_id': row[8],
        #     'alter_service_provid_qual': row[9],
        #     'alter_service_provid_id': row[10],
        #     'prescriberId_qual': row[11],
        #     'prescriber_id': row[12],
        #     'filler_1': row[13],
        #     'product_service_id': row[14],
        #     'qty_dispensed': row[15],
        #     'days_supply': row[16],
        #     'indicator': row[17],
        #     'filler_2': row[18],
        #     'orgi_submit_cont': row[19],
        #     'claim_control_no': row[20],
        #     'card_holder_id': row[21]
        #     })
        for row in data:
            # Dynamically create a dictionary where the column names are mapped to the respective row values
            record = {}
            for i, column_name in enumerate(columns):
                record[column_name] = row[i]
            rows.append(record)
        return rows
    except Exception as e:
        print(f"Error fetching data from PostgreSQL: {e}")
        return []
    finally:
        if connection:
            cursor.close()
            connection.close()

# Step 3: Send data to AWS SQS in batches
def send_to_sqs_batch(data):
    # Initialize boto3 SQS client with explicit AWS credentials
    sqs = boto3.client('sqs',
                       aws_access_key_id=AWS_ACCESS_KEY,
                       aws_secret_access_key=AWS_SECRET_KEY,
                       region_name=AWS_REGION)

    # Send messages in batches of 10
    batch_size = 10
    for i in range(0, len(data), batch_size):
        batch = data[i:i+batch_size]  # Get the next batch of messages

        # Create the list of SQS message entries
        entries = []
        for idx, record in enumerate(batch):
            entries.append({
                'Id': str(i + idx),  # Unique ID for each message in the batch
                'MessageBody': json.dumps(record)  # Message body
            })

        try:
            # Send the batch of messages to the SQS queue
            response = sqs.send_message_batch(
                QueueUrl=SQS_QUEUE_URL,
                Entries=entries
            )

            print(f"Sent {len(entries)} messages in a batch. Batch Message IDs: {[msg['MessageId'] for msg in response['Successful']]}")
        except Exception as e:
            print(f"Error sending batch to SQS: {e}")

# Main function to run the script
def main():
    # Retrieve data from PostgreSQL
    postgres_data = get_postgres_data()

    if postgres_data:
        # Send the retrieved data to SQS in batches
        send_to_sqs_batch(postgres_data)
    else:
        print("No data to send to SQS.")

if __name__ == "__main__":
    main()
