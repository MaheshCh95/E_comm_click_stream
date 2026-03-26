import airflow
from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta
from airflow.operators.dummy import DummyOperator

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}
project_id = 'e-commerceclickstream'
raw_dataset = 'e-comm-stag-23'
refine_dataset = 'e-comm-hist-23'
auth_view_dataset = 'e-comm-auth-23'
with DAG(
    'e_comm_24_03_2026',
    default_args=default_args,
    description='gcsbucket_dag',
    schedule_interval='*/5 * * * *',
)as dag:

    create_bucket = GCSCreateBucketOperator ( task_id = '23',bucket_name = 'e-comm-24-03-2026')
    
    create_raw_ds= BigQueryInsertJobOperator(task_id = '25',configuration={'query':{'query':f'
    create schema if not exist {project_id }.{raw_dataset}','uselegacy'= False,}},)
    
    create_refine_ds= BigQueryInsertJobOperator(task_id = '26',configuration={'query':{'query':f'
    create schema if not exist {project_id }.{refine_dataset}','uselegacy'= False,}},)
    
    create_auth_view_ds= BigQueryInsertJobOperator(task_id = '27',configuration={'query':{'query':f'
    create schema if not exist {project_id }.{auth_view_dataset}','uselegacy'= False,}},)



