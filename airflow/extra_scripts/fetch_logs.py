from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import DagRun, TaskInstance
from airflow.utils.dates import days_ago
import time

# Function to fetch the logs for each dynamically generated TaskInstance (mapped tasks)
def fetch_logs_for_mapped_tasks():
    # Get the latest dag_run for the final_backup1 DAG
    # Get all the DagRuns for 'final_backup1' and sort by execution_date
    dag_runs = DagRun.find(dag_id='final_backup1')
    
    # Sort the DagRuns by execution_date in descending order (latest first)
    latest_run = sorted(dag_runs, key=lambda run: run.execution_date, reverse=True)[0]  # Get the latest run
    
    print(f"Latest DAG Run details for final_backup1: {latest_run}")
    print(f"Execution Date: {latest_run.execution_date}")
    print(f"State: {latest_run.state}")
    print(f"Start Date: {latest_run.start_date}")
    print(f"End Date: {latest_run.end_date}")

    # Dynamically generate task instances for 'copy_files_task' using the expand method
    task_instance_list = []
    
    # Assuming the copy_files_task uses mapped arguments with a list of files
    # For example, you might be expanding a task with a list of files to copy
    task_parameters = ['file1', 'file2', 'file3']  # Example parameters, replace with actual ones
    
    # Iterate over the task parameters and create TaskInstances
    for param in task_parameters:
        task_instance = TaskInstance(task=copy_files_task, execution_date=latest_run.execution_date, dag_id='final_backup1', 
                                      task_id=f'copy_files_task_{param}')
        task_instance_list.append(task_instance)

    # Iterate over each task_instance and fetch logs
    for task_instance in task_instance_list:
        task_instance.refresh()  # Ensure the TaskInstance is up to date
        print(f"Fetching logs for Task: {task_instance.task_id}")
        try:
            # Fetching logs directly from Airflow's logging system
            logs = task_instance.log_url
            print(f"Log URL for {task_instance.task_id}: {logs}")
        except Exception as e:
            print(f"Error fetching logs for {task_instance.task_id}: {e}")

# Define the DAG
with DAG(
    'your_dag_id',  # DAG ID
    default_args={
        'owner': 'airflow',
        'retries': 1,
    },
    description='Fetch logs for each mapped task instance of copy_files_task',
    schedule_interval='*/5 * * * *',  # every 5 minutes
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Define the task to fetch logs for dynamically expanded copy_files_task instances
    fetch_logs_task = PythonOperator(
        task_id='fetch_logs_task',
        python_callable=fetch_logs_for_mapped_tasks,
    )

    fetch_logs_task  # This will run every 5 minutes as per the schedule
