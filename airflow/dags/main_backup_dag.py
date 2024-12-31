from airflow import DAG
from airflow.operators.subdag import SubDagOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from backup_subworkflow_dag import create_backup_subdag

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

# Main DAG definition
with DAG(
    dag_id="main_backup_dag",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2023, 12, 1),
) as dag:
    start_main = EmptyOperator(task_id="start_main_backup")

    # Subworkflow for `directory_1` backup
    backup_directory_1 = SubDagOperator(
        task_id="backup_directory_1",
        subdag=create_backup_subdag(
            "main_backup_dag",
            "backup_directory_1",
            default_args,
        ),
    )

    # Subworkflow for `directory_2` backup
    backup_directory_2 = SubDagOperator(
        task_id="backup_directory_2",
        subdag=create_backup_subdag(
            "main_backup_dag",
            "backup_directory_2",
            default_args,
        ),
    )

    end_main = EmptyOperator(task_id="end_main_backup")

    # Define task dependencies
    start_main >> [backup_directory_1, backup_directory_2] >> end_main
