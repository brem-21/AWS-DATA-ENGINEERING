from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime,timedelta
import pandas as pd
from airflow.hooks.base_hook import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from airflow.providers.mysql.hooks.mysql import MySqlHook
import mysql.connector
import boto3
import os
import pyarrow as pa
import pyarrow.parquet as pq

AWS_ACCESS_KEY = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = Variable.get("AWS_BUCKET_NAME")
AWS_REGION = Variable.get("AWS_REGION")
DB_HOST = Variable.get("DB_HOST")
DB_USER = Variable.get("DB_USER")
DB_PASSWORD = Variable.get("DB_PASS")
DB_NAME = Variable.get("DB_NAME")
DB_PORT = Variable.get("DB_PORT")
s3_folder = "rds-data/"

DATABASE_CONFIG = {
    'user': DB_USER,
    'password': DB_PASSWORD,
    'host': DB_HOST,
    'database': DB_NAME,
    'port': DB_PORT
}

MY_SQL_CONN_ID = 'SQL_CONN'

default_args = {
    'owner': 'brempong',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1, 
    'retries_delay': timedelta(minutes=1)
}

def get_tables():
    """Fetch all table names from the MySQL database."""
    conn = mysql.connector.connect(**DATABASE_CONFIG)
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    tables = [table[0] for table in cursor.fetchall()]
    cursor.close()
    conn.close()
    return tables


def check_table_data():
    """Check if each table has data."""
    conn = mysql.connector.connect(**DATABASE_CONFIG)
    cursor = conn.cursor()
    
    tables = get_tables()
    if not tables:
        print("No tables found in the database.")
        return

    for table in tables:
        query = f"SELECT COUNT(*) FROM `{table}`"
        cursor.execute(query)
        result = cursor.fetchone() 

        if result and result[0] > 0:
            print(f"Table '{table}' has {result[0]} records.")
        else:
            print(f"Table '{table}' is EMPTY!")

    cursor.close()
    conn.close()

def create_indexes():
    """Recreate indexes on specific tables for optimized querying."""
    conn = mysql.connector.connect(**DATABASE_CONFIG)
    cursor = conn.cursor()

    index_queries = [
        ("apartments_attributes", "idx_apartment_attr_id", "CREATE INDEX idx_apartment_attr_id ON apartments_attributes(id);"),
        ("apartments", "idx_apartments_id", "CREATE INDEX idx_apartments_id ON apartments(id);"),
        ("bookings", "idx_bookings_id", "CREATE INDEX idx_bookings_id ON bookings(booking_id);"),
        ("user_viewing", "idx_user_viewing_user_id", "CREATE INDEX idx_user_viewing_user_id ON user_viewing(user_id);"),
        ("bookings", "idx_bookings_user_id", "CREATE INDEX idx_bookings_user_id ON bookings(user_id);"),
        ("bookings", "idx_bookings_apartment_id", "CREATE INDEX idx_bookings_apartment_id ON bookings(apartment_id);"),
        ("user_viewing", "idx_user_viewing_apartment_id", "CREATE INDEX idx_user_viewing_apartment_id ON user_viewing(apartment_id);"),
        ("apartments_attributes", "idx_apartment_attr_cityname", "CREATE INDEX idx_apartment_attr_cityname ON apartments_attributes(cityname);"),
        ("apartments", "idx_apartments_price", "CREATE INDEX idx_apartments_price ON apartments(price);"),
        ("bookings", "idx_bookings_booking_date", "CREATE INDEX idx_bookings_booking_date ON bookings(booking_date);"),
        ("user_viewing", "idx_user_viewing_viewed_at", "CREATE INDEX idx_user_viewing_viewed_at ON user_viewing(viewed_at);"),
        ("apartments_attributes", "idx_apartment_attr_category", "CREATE FULLTEXT INDEX idx_apartment_attr_category ON apartments_attributes(category);"),
        ("apartments_attributes", "idx_apartment_attr_amenities", "CREATE FULLTEXT INDEX idx_apartment_attr_amenities ON apartments_attributes(amenities);")
    ]

    for table, index_name, create_sql in index_queries:
        try:
            # Check if the index exists
            check_sql = f"""
                SELECT COUNT(*) FROM information_schema.STATISTICS
                WHERE table_schema = DATABASE() AND table_name = '{table}' AND index_name = '{index_name}';
            """
            cursor.execute(check_sql)
            exists = cursor.fetchone()[0]

            if exists:
                drop_sql = f"DROP INDEX {index_name} ON {table};"
                cursor.execute(drop_sql)
                print(f"Dropped existing index: {index_name} on {table}")

            cursor.execute(create_sql)
            print(f"Created index: {index_name} on {table}")

        except mysql.connector.Error as e:
            print(f"Error with index {index_name} on {table}: {e}")

    conn.commit()
    cursor.close()
    conn.close()


def fetch_data():
    """Fetch data in batches and save as Parquet files locally."""
    conn = mysql.connector.connect(**DATABASE_CONFIG)
    cursor = conn.cursor(dictionary=True)
    tables = get_tables()

    for table in tables:
        offset = 0
        batch_size = 100000
        while True:
            query = f"SELECT * FROM {table} LIMIT {batch_size} OFFSET {offset}"
            cursor.execute(query)
            rows = cursor.fetchall()

            if not rows:
                break  # Exit loop when no more data

            # Convert to Parquet
            df = pd.DataFrame(rows)
            table_path = f"/tmp/{table}.parquet"

            # Convert DataFrame to Arrow Table
            table_arrow = pa.Table.from_pandas(df)
            pq.write_table(table_arrow, table_path)

            print(f"✅ Saved {table} batch {offset//batch_size + 1} locally.")

            # Move to next batch
            offset += batch_size

    cursor.close()
    conn.close()

# Upload local Parquet files to S3
def upload_to_s3():
    """Upload local Parquet files to S3 and delete them after upload."""
    s3_client = boto3.client("s3")
    tables = get_tables()

    for table in tables:
        table_path = f"/tmp/{table}.parquet"

        if os.path.exists(table_path):
            s3_key = f"{s3_folder}{table}.parquet"
            s3_client.upload_file(table_path, AWS_BUCKET_NAME, s3_key)
            print(f"✅ Uploaded {table} to S3.")

            # Cleanup local file
            os.remove(table_path)

def upload_to_s3():
    s3_hook = S3Hook(aws_conn_id="aws_default")
    tables = get_tables()

    for table in tables:
        table_path = f"/tmp/{table}.parquet"
        s3_key = f"{s3_folder}{table}.parquet"

        if os.path.exists(table_path):
            s3_hook.load_file(
                filename=table_path,
                key=s3_key,
                bucket_name=AWS_BUCKET_NAME,
                replace=True
            )
            print(f"✅ {table} uploaded to S3.")

def verify_and_cleanup():
    s3_hook = S3Hook(aws_conn_id="aws_default")
    tables = get_tables()

    for table in tables:
        s3_key = f"{s3_folder}{table}.parquet"
        table_path = f"/tmp/{table}.parquet"

        if s3_hook.check_for_key(s3_key, AWS_BUCKET_NAME):
            print(f"✅ {table} verified in S3. Deleting local file.")
            os.remove(table_path)
        else:
            print(f"⚠️ {table} NOT found in S3. Keeping local copy.")

with DAG(
    'Getting-data-from-RDS-and-uploading-to-S3',
    default_args=default_args,
    description='A pipeline to get data from RDS and upload to S3',
    schedule_interval='@daily',
    catchup=False   
) as dag:
    start_dag = DummyOperator(task_id='start_dag')

    get_table_names = PythonOperator(
        task_id='get_table_names',
        python_callable=get_tables
    )
    check_table_data = PythonOperator(
        task_id='check_table_data',
        python_callable=check_table_data
    )

    create_indexes = PythonOperator(
        task_id='create_indexes',
        python_callable=create_indexes
    )

    fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data
    )

    upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3
    )
    verify_and_cleanup = PythonOperator(
        task_id='verify_and_cleanup',
        python_callable=verify_and_cleanup
    )

    start_dag >> get_table_names >> check_table_data >> create_indexes >> fetch_data >> upload_to_s3 >> verify_and_cleanup