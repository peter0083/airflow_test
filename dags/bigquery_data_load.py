from airflow import DAG
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

from airflow.utils.dates import days_ago

default_arguments = {'owner': 'Peter', 'start_date': days_ago(1)}

with DAG(
    'bigquery_data_load',
    schedule_interval='@hourly',
    catchup=False,
        default_args=default_arguments) as dag:

    load_data = GoogleCloudStorageToBigQueryOperator(
        task_id='load_data',
        bucket='pl-logistics-landing-bucket-dev',
        source_objects=['*'],
        source_format='CSV',
        skip_leading_rows=1,
        field_delimiter=',',
        destination_project_dataset_table='pl-airflow-310103.vehicle_analytics.history',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND',
        bigquery_conn_id='google_cloud_default',
        google_cloud_storage_conn_id='google_cloud_default'
    )

    query = '''
    SELECT * except (rank)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY vehicle_id ORDER BY DATETIME(date, TIME(hour, minute, 0)) DESC
            ) as rank
        FROM `pl-airflow-310103.vehicle_analytics.history`) as latest
    WHERE rank = 1;
    '''

    create_table = BigQueryOperator(
        task_id='create_table',
        sql=query,
        destination_dataset_table='pl-airflow-310103.vehicle_analytics.latest',
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        use_legacy_sql=False,
        location='us-west1',
        bigquery_conn_id='google_cloud_default'
    )

load_data >> create_table
