from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
import requests
import gzip
import os
from sqlalchemy import create_engine

BASE_URL = "https://dumps.wikimedia.org/other/pageviews/2024/10/"
FILENAME = "pageviews-2024-10-10-16.gz"
COMPANIES = ["Amazon", "Apple", "Facebook", "Google", "Microsoft"]
DATABASE_URI = 'postgresql+psycopg2://airflow:airflow@postgres/airflow'

def download_file(url, dest):
    response = requests.get(url)
    with open(dest, 'wb') as f:
        f.write(response.content)
    return dest

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
        bash_command='gzip -d /tmp/{{ task_instance.xcom_pull(task_ids="download_pageviews") }}', 
    )

    process_task = PythonOperator(
        task_id='process_pageviews',
        python_callable=extract_and_save_pageviews,
        op_kwargs={'filepath': '/tmp/' + FILENAME.replace('.gz', '')},
        do_xcom_push=True,
    )

    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        op_kwargs={'sql_commands': '{{ task_instance.xcom_pull(task_ids="process_pageviews") }}'},
    )

    download_task >> extract_task >> process_task >> load_task
