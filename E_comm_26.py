import airflow
from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta
from airflow.operators.dummy import DummyOperator

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 2,
    'retry_delay': timedelta(minutes=3)
}
project_id = 'e-commerceclickstream'
raw_dataset = 'e-comm-stag-26'
refine_dataset = 'e-comm-hist-26'
auth_view_dataset = 'e-comm-auth-26'
with DAG(
    'e_comm_26_03_2026',
    default_args=default_args,
    description='gcsbucket_dag',
    schedule_interval='*/3 * * * *',
)as dag:

    create_bucket = GCSCreateBucketOperator ( task_id = '26',bucket_name = 'e-comm-26-03-2026')
    
    create_raw_ds= BigQueryInsertJobOperator(task_id = '26',configuration={'query':{'query':f'
    create schema if not exist {project_id }.{raw_dataset}','uselegacy'= False,}},)
    
    create_refine_ds= BigQueryInsertJobOperator(task_id = '26',configuration={'query':{'query':f'
    create schema if not exist {project_id }.{refine_dataset}','uselegacy'= False,}},)
    
    create_auth_view_ds= BigQueryInsertJobOperator(task_id = '26',configuration={'query':{'query':f'
    create schema if not exist {project_id }.{auth_view_dataset}','uselegacy'= False,}},)



