from airflow import DAG
from airflow.utils.dates import days_ago

default_arguments = {"owner": "Peter", 'start_date': days_ago(1)}

with DAG(
    dag_id = 'core_concepts',
    schedule_interval = '@daily', 
    catchup=False,
    default_args = default_arguments
) as dag: 
