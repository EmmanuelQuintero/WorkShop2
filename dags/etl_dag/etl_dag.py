from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys

sys.path.append(os.path.abspath("/opt/airflow/dags/etl_dag/"))

from etl import extract_csv, transform_csv, extract_db, transform_db, merge, load, store

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),  # Update the start date to today or an appropriate date
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'ETL_Dag',
    default_args=default_args,
    description='My Workshop Dag with ETL process!',
    schedule_interval='@daily',  # Set the schedule interval as per your requirements) as dag:
) as dag:    
    extract_csv = PythonOperator(
        task_id='extraction_csv',
        python_callable=extract_csv
    )

    transform_csv = PythonOperator(
        task_id='transformation_csv',
        python_callable=transform_csv
    )


    extract_db = PythonOperator(
        task_id='extraction_db',
        python_callable=extract_db
    )

    transform_db = PythonOperator(
        task_id='transformation_db',
        python_callable=transform_db
    )

    merge = PythonOperator(
        task_id='merge',
        python_callable=merge
    )

    load = PythonOperator(
        task_id='load',
        python_callable=load
    )

    store = PythonOperator(
        task_id='store',
        python_callable=store
    )

    extract_csv >> transform_csv >> merge

    extract_db >> transform_db >> merge

    merge >> load >> store