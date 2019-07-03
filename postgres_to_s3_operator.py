import airflow
import pandas as pd
import logging
import boto3
from sqlalchemy import create_engine
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.executors.base_executor import BaseExecutor
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.hooks.base_hook import BaseHook
from airflow.utils.decorators import apply_defaults
from boto3.s3.transfer import S3Transfer

class PostgresToS3(BaseOperator):    
    template_fields = ('postgres_conn_id', 'aws_conn_id', 's3_bucket', 's3_key', 'sql', 'file_name')
    template_ext = ('.sql',)
    @apply_defaults
    def __init__(self,
                postgres_conn_id, 
                aws_conn_id,
                s3_bucket,
                s3_key,
                sql,
                file_name,
                *args, **kwargs):
            super(PostgresToS3, self).__init__(*args, **kwargs)
            self.postgres_conn_id = postgres_conn_id
            self.aws_conn_id = aws_conn_id
            self.s3_bucket = s3_bucket
            self.s3_key = s3_key
            self.sql = sql
            self.file_name = file_name


    def execute(self, context):
        #Postgres connection
        connection = BaseHook.get_connection(self.postgres_conn_id)
        password = connection.password
        host = connection.host
        user = connection.login
        db = connection.schema
        
        sql_conn = 'postgresql://'+str(user)+':'+str(password)+'@'+str(host)+'/'+str(db)
        engine = create_engine(sql_conn)
        df = pd.read_sql_query(self.sql, con=engine)        
        df.to_parquet('/tmp/'+self.file_name, index=False)
        path_file = '/tmp/'+self.file_name
        
        #s3 connection to send file into bucket
        s3 = S3Hook(aws_conn_id=self.aws_conn_id)
        credentials = s3.get_credentials()
        access_key = credentials.access_key
        secret_key = credentials.secret_key
        bucket_name = self.s3_bucket
        key = self.s3_key
        bucket_full_path = f's3://{bucket_name}/{key+self.file_name}'
        file_folder = f's3://{bucket_name}/{key}/'
       
        client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key) 
        transfer = S3Transfer(client)
        transfer.upload_file(path_file, bucket_name, key+self.file_name)

        #send keys to another operator
        task_instance = context['task_instance']
        task_instance.xcom_push(key="s3_bucket_full", value=bucket_full_path)
        task_instance.xcom_push(key="s3_file_folder", value=file_folder)        


        logging.info(f'upload_file to '+bucket_full_path + '\n')
        logging.info(f'savin file in'+file_folder)
        
        
    
class PostgresPlugin(AirflowPlugin):
    name = 'postgres_plugin_operator'
    operators = [PostgresToS3]
    hooks = []
    executors = []
    macros = []




