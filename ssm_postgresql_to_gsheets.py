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

# PostgreSQL credentials
db_type = "PostgreSQL"
db_host = "localhost"
db_port = "5432"

db_user = os.environ.get("DB_USER")
db_password = os.environ.get("DB_PASSWORD")
db_name = os.environ.get("DB_NAME")


# Establish an SSM session
ssm = boto3.client("ssm", region_name=region)
ssm_start_response = ssm.start_session(
    Target=instance_id, DocumentName=ssm_document_name, Parameters=ssm_parameters
)
# Print a message when the session is successfully made
if ssm_start_response.get("SessionId"):
    print("SSM session successfully established.")

    print([
        db_name,
        db_user,
        db_password,
        db_host,
        ssm_parameters["localPortNumber"][0],
    ])
    # PostgreSQL connection
    conn = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=ssm_parameters["localPortNumber"][0],
    )

    # Download data from the PostgreSQL table as a CSV
    query = "SELECT * FROM account_transactions"
    data = pd.read_sql_query(query, conn)

    # Close the PostgreSQL connection
    conn.close()

    # Google Sheets credentials (replace with your own)
    gspread_credentials_file = os.environ.get("GOOGLE_SHEETS_KEY_FILE")
    spreadsheet_name = os.environ.get("GOOGLE_SHEETS_FILE_NAME")

    # Upload data to Google Sheets
    gc = gspread.service_account(filename=gspread_credentials_file)
    spreadsheet = gc.open(spreadsheet_name)
    worksheet = spreadsheet.get_worksheet(0)  # Adjust the worksheet index as needed
    worksheet.update([data.columns.values.tolist()] + data.values.tolist())

    # End the SSM session
    ssm.end_session(SessionId=ssm_start_response["SessionId"])
