from airflow import DAG
from datetime import datetime , timedelta
from airflow.operators.python_operator import PythonOperator
from extraction import run_extraction
from loading import run_loading

my_args = {
    'owner' : 'airflow',
    'depends_on_past' : False ,
    'start_date' : datetime(2025 ,1 ,18),
    'email':'admin@gmail.com',
    'email_on_failure': False ,
    'email_on_retry': False ,
    'retries' : 1 ,
    'retries_delay' : timedelta(minutes=2)
    
}

dags = DAG(
    'kike_stores',
    default_args = my_args ,
    description = 'kike stores data migration to azure blob storage',

)

Extraction = PythonOperator(
    task_id = 'extraction_phase' ,
    python_callable = run_extraction ,
    dag = dags
)

Loading = PythonOperator(
    task_id = 'loading_phase' ,
    python_callable = run_loading ,
    dag = dags
)

Extraction >> Loading
