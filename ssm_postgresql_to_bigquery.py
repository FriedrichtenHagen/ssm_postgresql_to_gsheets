import boto3
import psycopg2
import pandas as pd
import os
from dotenv import load_dotenv
import time

load_dotenv()

# AWS SSM parameters
region = os.environ.get("REGION")
instance_id = os.environ.get("INSTANCE_ID")
ssm_document_name = os.environ.get("SSM_DOCUMENT_NAME")
host = os.environ.get("HOST")
# AWS credentials
aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")

ssm_parameters = {
    "host": [host],
    "portNumber": ["5432"],
    "localPortNumber": ["5432"],
}

# PostgreSQL credentials
db_type = "PostgreSQL"
db_host = "localhost"
db_port = "5432"
db_user = os.environ.get("DB_USER")
db_password = os.environ.get("DB_PASSWORD")
db_name = os.environ.get("DB_NAME")

# Establish an SSM session
ssm = boto3.client("ssm", region_name=region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
ssm_start_response = ssm.start_session(
    Target=instance_id, DocumentName=ssm_document_name, Parameters=ssm_parameters
)
# Print a message when the session is successfully made
if ssm_start_response.get("SessionId"):
    print("SSM session successfully established.")
    print(f"SSM session details: {ssm_start_response}")
    # Introduce a delay
    time.sleep(5)  # Adjust as needed
    try:
        # PostgreSQL connection
        conn = psycopg2.connect(
            dbname=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port="5432",
        )
        query = "SELECT * FROM account_transactions LIMIT 15"
        data = pd.read_sql_query(query, conn)
        conn.close()
        print(data)
    except Exception as e:
        print(f"Error: {e}")

    # Terminate the SSM session
    ssm.terminate_session(SessionId=ssm_start_response["SessionId"])
    print("SSM session terminated.")
