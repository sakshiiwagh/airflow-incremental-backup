from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define a mock function to simulate a backup step
def compress_files(**kwargs):
    print("Compressing files...")

def upload_to_cloud(**kwargs):
    print("Uploading files to cloud storage...")

def verify_backup(**kwargs):
    print("Verifying backup integrity...")

# Subworkflow definition
def create_backup_subdag(parent_dag_name, child_dag_name, default_args):
    subdag = DAG(
        dag_id=f"{parent_dag_name}.{child_dag_name}",
        default_args=default_args,
        schedule_interval="@daily",
        start_date=datetime(2023, 12, 1),
    )

    with subdag:
        start = EmptyOperator(task_id="start_backup")
        compress = PythonOperator(
            task_id="compress_files",
            python_callable=compress_files,
        )
        upload = PythonOperator(
            task_id="upload_to_cloud",
            python_callable=upload_to_cloud,
        )
        verify = PythonOperator(
            task_id="verify_backup",
            python_callable=verify_backup,
        )
        end = EmptyOperator(task_id="end_backup")

        # Define task dependencies
        start >> compress >> upload >> verify >> end

    return subdag
