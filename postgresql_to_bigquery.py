import os
import psycopg2
from dotenv import load_dotenv
import pandas as pd
from google.cloud import bigquery

load_dotenv()

# BigQuery credentials (replace with your own)
project_id = os.environ.get("BIGQUERY_PROJECT_ID")
dataset_id = os.environ.get("BIGQUERY_DATASET_ID")
table_id = os.environ.get("BIGQUERY_TABLE_ID")

# PostgreSQL credentials
db_type = "PostgreSQL"
db_host = "localhost"
db_port = "5432"
db_user = os.environ.get("DB_USER")
db_password = os.environ.get("DB_PASSWORD")
db_name = os.environ.get("DB_NAME")

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

# Initialize BigQuery client
client = bigquery.Client(project=project_id)

# Push data to BigQuery table
job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Replace with the desired write disposition
)
table_ref = client.dataset(dataset_id).table(table_id)
job = client.load_table_from_dataframe(data, table_ref, job_config=job_config)
job.result()  # Wait for the job to complete

print(f"Data pushed to BigQuery table: {project_id}.{dataset_id}.{table_id}")


# Replace the placeholders for BigQuery project, dataset, and table IDs with your actual values. 
# This script will load the data from PostgreSQL, transform it, and push it to the specified BigQuery table using the Google Cloud BigQuery API.