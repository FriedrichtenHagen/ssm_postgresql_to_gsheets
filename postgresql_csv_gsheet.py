import boto3
import psycopg2
import pandas as pd
import gspread
import os
from dotenv import load_dotenv

load_dotenv()

# PostgreSQL credentials
db_type = "PostgreSQL"
db_host = "localhost"
db_port = "5432"
db_user = os.environ.get("DB_USER")
db_password = os.environ.get("DB_PASSWORD")
db_name = os.environ.get("DB_NAME")

# Google Sheets credentials (replace with your own)
gspread_credentials_file = os.environ.get("GOOGLE_SHEETS_KEY_FILE")
spreadsheet_name = os.environ.get("GOOGLE_SHEETS_FILE_NAME")

# PostgreSQL connection
conn = psycopg2.connect(
    dbname=db_name,
    user=db_user,
    password=db_password,
    host=db_host,
    port="5432",
)
query = "SELECT * FROM account_transactions"
data = pd.read_sql_query(query, conn)
conn.close()

# Convert timestamp columns to string before saving to CSV
data['created_at'] = data['created_at'].astype(str)
data['updated_at'] = data['updated_at'].astype(str)
data['completed_at'] = data['completed_at'].astype(str)
data['amount_minor_units'] = (data['amount_minor_units']).fillna(0).astype(int) / 100

# Save data to a CSV file
csv_filename = "data.csv"
data.to_csv(csv_filename, index=False)

# Upload the CSV to Google Sheets
gc = gspread.service_account(filename=gspread_credentials_file)
spreadsheet = gc.open(spreadsheet_name)
worksheet = spreadsheet.get_worksheet(0)  # Adjust the worksheet index as needed

# Import data from CSV to Google Sheets
with open(csv_filename, "r") as file:
    csv_content = file.read()
csv_data = [line.split(",") for line in csv_content.split("\n")]
range_name = "A1"  # Start importing data from cell A1
worksheet.update(range_name, csv_data)

# Optional: Remove the CSV file
os.remove(csv_filename)

print("Data imported to Google Sheets")