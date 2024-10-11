from airflow import DAG
from airflow.contrib.operators.file_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime,timedelta

# Custom Python logic for derriving data value
yesterday = datetime.combine(datetime.today() - timedelta(1), datetime.min.time())

# Default arguments
default_args = {
    'start_date': yesterday,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(dag_id='Local_to_GCS',
         catchup=False,
         schedule_interval=timedelta(days=1),
         default_args=default_args
         ) as dag:


    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src='/Users/Glide Cloud/datafiles/data.csv',
        dst='gs://gcssource1211',
        bucket='gcssource1211',
        gcp_conn_id='google_connect',
        dag=dag
    )

    trigger_next_dag = TriggerDagRunOperator(
        task_id='trigger_transform_data_dag',
        trigger_dag_id='GCS_to_BQ_and_AGG',  # The ID of the second DAG
        wait_for_completion=True,  # Wait for the triggered DAG to finish
    )

    # Define the task dependencies
    upload_file >> trigger_next_dag


