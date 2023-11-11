import boto3
import psycopg2
import pandas as pd
import gspread
import os
from dotenv import load_dotenv

load_dotenv()

# AWS SSM parameters
region = os.environ.get("REGION")
instance_id = os.environ.get("INSTANCE_ID")
ssm_document_name = os.environ.get("SSM_DOCUMENT_NAME")
host = os.environ.get("HOST")

ssm_parameters = {
    "host": [host],
    "portNumber": ["5432"],
    "localPortNumber": ["5432"],
}

# Establish an SSM session
ssm = boto3.client("ssm", region_name=region)
ssm_start_response = ssm.start_session(
    Target=instance_id, DocumentName=ssm_document_name, Parameters=ssm_parameters
)
# Print a message when the session is successfully made
if ssm_start_response.get("SessionId"):
    print("SSM session successfully established.")

    

    # End the SSM session
    ssm.end_session(SessionId=ssm_start_response["SessionId"])
