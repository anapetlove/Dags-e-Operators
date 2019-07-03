import airflow
import pandas as pd
import psycopg2
from datetime import timedelta
from airflow import DAG
from airflow.operators.postgres_plugin_operator import PostgresToS3
from airflow.contrib.operators.aws_athena_operator import AWSAthenaOperator
from airflow.operators.bash_operator import BashOperator
from templates.pig_templates import TEMPLATES
import airflow.macros

default_args = {
    'owner': 'airflow',    
    'start_date': airflow.utils.dates.days_ago(0),
    'depends_on_past': False
}

partition = "pyear={{macros.datetime.strptime(ds_nodash, '%Y%m%d').strftime('%Y')}}/pmonth={{macros.datetime.strptime(ds_nodash, '%Y%m%d').strftime('%m')}}/pday={{macros.datetime.strptime(ds_nodash, '%Y%m%d').strftime('%d')}}/prefdate={{ds_nodash}}/"

with DAG(dag_id='passing_pig', default_args=default_args, schedule_interval=timedelta(days=1)) as dag:
   for template in TEMPLATES:
        send_s3_raw = PostgresToS3(task_id='send_s3_raw_'+template,
                                sql='/templates/pig_'+template+'.sql',
                                file_name=template+'_{{ds_nodash}}_{{ts_nodash}}.parquet',
                                postgres_conn_id='postgres_conn_id',
                                aws_conn_id='aws_petlove',
                                s3_bucket='petlove-nessie-dev',
                                s3_key='ingestion_tier/raw_data_zone/pig/pig_'+template+'/'+partition
        )

        repair_raw = AWSAthenaOperator(task_id='repair_raw_'+template,
                                query='MSCK REPAIR TABLE pig_'+template,
                                database='raw',
                                output_location='s3://aws-athena-query-results-381158256258-us-east-2/',
                                aws_conn_id='aws_petlove'
        )

        send_s3_trusted = BashOperator(task_id='send_s3_trusted_'+template,
                                bash_command= "aws s3 cp {{task_instance.xcom_pull(task_ids='send_s3_raw_"+template+"', key='s3_bucket_full')}} s3://petlove-nessie-dev/hub_tier/trusted_data_zone/pig/"+template+'/'+partition+template+"_{{ds_nodash}}.parquet"                               
                               
        )
        repair_trusted_Athena = AWSAthenaOperator(task_id='repair_trusted_'+template,
                        query='MSCK REPAIR TABLE pig_'+template,
                        database='trusted',
                        output_location='s3://aws-athena-query-results-381158256258-us-east-2/',
                        aws_conn_id='aws_petlove'
        )
        send_s3_raw >> repair_raw >> send_s3_trusted >> repair_trusted_Athena




        

        




 