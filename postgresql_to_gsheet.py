import boto3
import psycopg2
import pandas as pd
import gspread
import os
from dotenv import load_dotenv

load_dotenv()

# PostgreSQL credentials
# db_type = "PostgreSQL"
# db_host = "localhost"
# db_port = "5432"
# db_user = "friedrich_at_advertace"
# db_password = "x5fh87bqPjM6O6c"
# db_name = "production"

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

# Convert timestamp columns to string before updating Google Sheets
data['created_at'] = data['created_at'].astype(str)
data['updated_at'] = data['updated_at'].astype(str)
data['completed_at'] = data['completed_at'].astype(str)
data['amount_minor_units'] = (data['amount_minor_units'] ).fillna(0).astype(int) / 100

conn.close()

# Upload data to Google Sheets
gc = gspread.service_account(filename=gspread_credentials_file)
spreadsheet = gc.open(spreadsheet_name)
worksheet = spreadsheet.get_worksheet(0)  # Adjust the worksheet index as needed

# Convert the DataFrame to a list of lists for updating Google Sheets
data_as_list = [data.columns.values.tolist()] + data.values.tolist()

# Define the range where you want to update the data (e.g., "A1" for starting from cell A1)
range_name = "A1"



batch_size = 1000  # Define your batch size
for i in range(0, len(data), batch_size):
    batch_data = data[i:i+batch_size]
    worksheet.update([batch_data.columns.values.tolist()] + batch_data.values.tolist())
    print(i)