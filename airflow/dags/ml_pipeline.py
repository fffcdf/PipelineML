from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import sys
import os
AIRFLOW_DIR=os.path.dirname(os.path.dirname(__file__))
sys.path.append(os.path.join(AIRFLOW_DIR,'ml_code'))

from getdata_pipeline import get_data_and_load_data
from add_cities_to_db import fill_db
from preprocessing_pipeline import preprocess
from train_pipeline import train


dag0=DAG(
    dag_id='add_raw_table_to_db_once',
    start_date=datetime(2025,7,17,12,30),
    schedule_interval='@once',
    catchup=True,
    default_args={'owner':'airflow'})
add_raw_table=PostgresOperator(
        task_id='add_raw_table',
        postgres_conn_id='postgres_raw_data',
        dag=dag0,
        sql="""CREATE TABLE raw (
    temp_c FLOAT,
    feelslike_c FLOAT,
    humidity INT,
    pressure_mb FLOAT,
    wind_kph FLOAT,
    gust_kph FLOAT,
    cloud INT,
    precip_mm FLOAT,
    uv FLOAT,
    is_day INT,
    condition_code INT,
    hour INT,
    month INT,
    weekday INT,
    lat FLOAT,
    lon FLOAT
);"""
    )

dag1=DAG(
    dag_id='add_clean_table_to_db_once',
    start_date=datetime(2025,7,17,12,30),
    schedule_interval='@once',
    catchup=True,
    default_args={'owner':'airflow'})
add_clean_table=PostgresOperator(
        task_id='add_clean_table',
        postgres_conn_id='postgres_clean_data',
        dag=dag1,
        sql="""CREATE TABLE clean (
    temp_c FLOAT,
    feelslike_c FLOAT,
    humidity INT,
    pressure_mb FLOAT,
    wind_kph FLOAT,
    gust_kph FLOAT,
    cloud INT,
    precip_mm FLOAT,
    uv FLOAT,
    is_day INT,
    target TEXT,
    hour INT,
    month INT,
    weekday INT,
    lat FLOAT,
    lon FLOAT
);"""
    )

dag2=DAG(
    dag_id='add_cities_to_db_once',
    start_date=datetime(2025,7,17,12,30),
    schedule_interval='@once',
    catchup=True,
    default_args={'owner':'airflow'})
add_cities_task=PythonOperator(
    task_id='add_cities_to_db',
    python_callable=fill_db,
    dag=dag2
)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout':timedelta(minutes=60)
}
dag3=DAG(
    dag_id='get_data_dag',
    default_args=default_args,
    schedule_interval='*/30 * * * *',
    start_date=datetime(2025, 7, 17, 13),
    catchup=False
)
get_weather_task =PythonOperator(
    task_id='get_weather_data',
    python_callable=get_data_and_load_data,
    dag=dag3
    )


default_args1 = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout':timedelta(minutes=30)
}
dag4=DAG(
    dag_id='preprocess_dag',
    default_args=default_args1,
    schedule_interval='*/60 * * * *',
    start_date=datetime(2025,7,17,14,10),
    catchup=False
)
preprocess_task=PythonOperator(
    task_id='preprocess_task',
    python_callable=preprocess,
    dag=dag4
)


default_args2 = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout':timedelta(minutes=30)
}
dag5=DAG(
    dag_id='train_dag',
    default_args=default_args2,
    schedule_interval='@daily',
    start_date=datetime(2025,7,17,12,30),
    catchup=False
)
train_task=PythonOperator(
    task_id='train_task',
    python_callable=train,
    dag=dag5
)
