from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
import requests
import gzip
import os
from sqlalchemy import create_engine

# Constants
BASE_URL = "https://dumps.wikimedia.org/other/pageviews/2024/10/"
FILENAME = "pageviews-2024-10-10-16.gz"  # Adjust for desired hour/date
COMPANIES = ["Amazon", "Apple", "Facebook", "Google", "Microsoft"]
DATABASE_URI = 'postgresql+psycopg2://airflow:airflow@postgres/airflow'

def download_file(url, dest):
    response = requests.get(url)
    with open(dest, 'wb') as f:
        f.write(response.content)
    return dest  # Return the path to the downloaded file for XCom

def extract_and_save_pageviews(filepath):
    sql_commands = []
    with gzip.open(filepath, 'rt', encoding='utf-8') as f:
        for line in f:
            page, views = line.split(' ')
            if any(company in page for company in COMPANIES):
                sql_commands.append(f"INSERT INTO pageviews (page, views) VALUES ('{page}', {views});")
    return sql_commands

def load_to_postgres(sql_commands):
    engine = create_engine(DATABASE_URI)
    with engine.connect() as conn:
        for command in sql_commands:
            conn.execute(command)

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG('wikipedia_pageviews', default_args=default_args, schedule_interval='@daily') as dag:

    download_task = PythonOperator(
        task_id='download_pageviews',
        python_callable=download_file,
        op_kwargs={'url': BASE_URL + FILENAME, 'dest': '/tmp/' + FILENAME},
    )

    extract_task = BashOperator(
        task_id='extract_pageviews',
        bash_command='gzip -d /tmp/{{ task_instance.xcom_pull(task_ids="download_pageviews") }}',  # Pulling the correct file path
    )

    process_task = PythonOperator(
        task_id='process_pageviews',
        python_callable=extract_and_save_pageviews,
        op_kwargs={'filepath': '/tmp/' + FILENAME.replace('.gz', '')},  # Updated to use the decompressed filename
        do_xcom_push=True,
    )

    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        op_kwargs={'sql_commands': '{{ task_instance.xcom_pull(task_ids="process_pageviews") }}'},
    )

    download_task >> extract_task >> process_task >> load_task


# import os
# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.operators.bash_operator import BashOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
# import requests
# import gzip
# import csv
# import logging

# default_args= {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2024, 10, 17),
#     'retries': 2,
#     'retry_delay': timedelta(minutes=5),

# }

# dag = DAG(
#     'wikipedia_page_views',
#     default_args=default_args,
#     description='Fetch Wikipedia page views.',
#     schedule_interval=timedelta(days=1),
# )

# # Download page views
# # def download_page_views():    
# #     url = "https://dumps.wikimedia.org/other/pageviews/2024/2024-10/projectviews-20241010-160000.gz"
# #     response = requests.get(url)
# #     with open('/tmp/pageviews.gz', 'wb') as f:
# #         f.write(response.content)

# def download_pageviews(**context):
#     execution_date = context['execution_date']
#     url = f"https://dumps.wikimedia.org/other/pageviews/{execution_date.year}/{execution_date.year}-{execution_date.month:02}/pageviews-{execution_date.year}{execution_date.month:02}{execution_date.day:02}-{execution_date.hour:02}0000.gz"
    
#     logging.info(f"Attempting to download file from URL: {url}")
    
#     try:
#         response = requests.get(url)
#         response.raise_for_status()  # Raises an HTTPError for bad responses
        
#         with open('/tmp/pageviews.gz', 'wb') as f:
#             f.write(response.content)
        
#         logging.info(f"File downloaded successfully. Size: {len(response.content)} bytes")
        
#         # Verify if the file is in gzip format
#         try:
#             with gzip.open('/tmp/pageviews.gz', 'rb') as f:
#                 f.read(1)
#             logging.info("File verified to be in gzip format")
#         except gzip.BadGzipFile:
#             logging.error("Downloaded file is not in valid gzip format")
#             raise
        
#     except requests.RequestException as e:
#         logging.error(f"Error downloading file: {e}")
#         raise
#     except Exception as e:
#         logging.error(f"Unexpected error: {e}")
#         raise

# # Process page views
# def process_page_views():
#     companies = ['Amazon', 'Apple', 'Facebook', 'Google', 'Microsoft']
#     results = []
#     with gzip.open('/tmp/pageviews.gz', 'rt') as f:
#         reader = csv.reader(f, delimiter=' ')
#         for row in reader:
#             if row[0] == 'en' and row[1] in companies:
#                 results.append((row[1], int(row[2]), int(row[3])))

#     with open('/tmp/pageviews.sql', 'w') as f:
#         for company, views, bytes_sent in results:
#             f.write(f"INSERT INTO pageviews (company, views, bytes_sent) VALUES ('{company}', {views}, {bytes_sent});\n")

# download = PythonOperator(
#     task_id='download_page_views',
#     python_callable=download_pageviews,
#     dag=dag,
# )

# extract = BashOperator(
#     task_id='extract_page_views',
#     bash_command='''
#     if [ -f /tmp/pageviews.gz ]; then
#         file_type=$(file -b /tmp/pageviews.gz)
#         echo "File type: $file_type"
#         if [[ $file_type == *"gzip compressed data"* ]]; then
#             gunzip -f /tmp/pageviews.gz
#             echo "File extracted successfully"
#         else
#             echo "Error: File is not in gzip format"
#             exit 1
#         fi
#     else
#         echo "Error: /tmp/pageviews.gz does not exist"
#         exit 1
#     fi
#     ''',
#     dag=dag,
# )

# process = PythonOperator(
#     task_id='process_page_views',
#     python_callable=process_page_views,
#     dag=dag,
# )

# create_table = PostgresOperator(
#     task_id='create_table',
#     postgres_conn_id='postgres_default',
#     sql='sql/create_table.sql',
#     dag=dag,
# )

# load_data = PostgresOperator(
#     task_id='load_data',
#     postgres_conn_id='postgres_default',
#     sql='/tmp/pageviews.sql',
#     dag=dag,
# )

# download >> extract >> process >> create_table >> load_data