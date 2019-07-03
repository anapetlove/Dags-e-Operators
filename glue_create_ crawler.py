import boto3
from dags.templates.pig_templates import TEMPLATES

client = boto3.client(service_name='glue', region_name='us-east-2')


for table in TEMPLATES:
          
    response = client.create_crawler(
        Name='raw.pig_'+table,
        Role='NessieDatalakeGlue',
        DatabaseName='raw',
        Targets={
            'S3Targets':[
                {
                    'Path': 's3://petlove-nessie-dev/ingestion_tier/raw_data_zone/pig/pig_'+table
                }
        ]
        },
    )
    response = client.start_crawler(
        Name='raw.pig_'+table,
    )




