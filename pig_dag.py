import airflow
import pandas as pd
import psycopg2
from datetime import timedelta
from airflow import DAG
from airflow.operators.postgres_plugin_operator import PostgresToS3
from pig_tables import TABLES

default_args = {
    'owner': 'airflow',    
    'start_date': airflow.utils.dates.days_ago(0),
    'depends_on_past': False
}

with DAG(dag_id='passing_pig', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:
            send_s3 = PostgresToS3(task_id='send_s3',
                                sql='/templates/pig_versions.sql',
                                file_name='versions.parquet',
                                postgres_conn_id='postgres_conn_id',
                                aws_conn_id='aws_petlove',
                                s3_bucket='petlove-nessie-dev',
                                s3_key='ingestion_tier/raw_data_zone/pig/pig_versions/'             
            )       