import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowSkipException
from airflow.providers.http.operators.http import SimpleHttpOperator
from requests.auth import HTTPBasicAuth
from airflow.utils.dates import days_ago

# Function to get the latest DAG run status (either 'running' or 'success')
def get_latest_dagrun_status():
    """Fetch the latest DAG run and return its status."""
    airflow_url = "http://localhost:8080/api/v1/dags/final_backup1/dagRuns"
    auth = HTTPBasicAuth('admin', 'NDW9vsZ2mC7UKxqW')
    
    # Sending GET request to fetch DAG runs
    response = requests.get(airflow_url, auth=auth)

    if response.status_code == 200:
        dag_runs = response.json().get('dag_runs', [])
        latest_dag_run = None

        # Iterate over DAG runs to find the latest 'running' or 'success' state
        for dag_run in dag_runs:
            if dag_run['state'] in ['running', 'success']:
                if not latest_dag_run or dag_run['execution_date'] > latest_dag_run['execution_date']:
                    latest_dag_run = dag_run

        if latest_dag_run:
            # Return execution date of the most recent "running" or "success" DAG run
            return datetime.fromisoformat(latest_dag_run['execution_date'])
        else:
            raise ValueError("No running or successful DAG found.")
    else:
        raise Exception(f"Error fetching DAG runs: {response.status_code} {response.text}")

# Safe execution date function for the ExternalTaskSensor
def safe_execution_date_fn(logical_date):
    try:
        return get_latest_dagrun_status()
    except ValueError as e:
        raise AirflowSkipException(f"Skipping because no running or successful DAG was found: {e}")

# Define the DAG and tasks
with DAG(
    dag_id='external_task_sensor_example',
    start_date=days_ago(1),
    schedule_interval=None,  # Ensure this is triggered manually or scheduled
    catchup=False
) as dag:
    
    # Start task
    start = EmptyOperator(
        task_id='start'
    )

    # Trigger final_backup1 when this DAG is triggered
    trigger_final_backup1 = TriggerDagRunOperator(
        task_id='trigger_final_backup1',
        trigger_dag_id='final_backup1',  # The DAG to trigger
        conf={"key": "value"},  # You can pass configuration if needed
    )

    # Wait for the external task to be either running or successful
    wait_for_task = ExternalTaskSensor(
    task_id='wait_for_external_task',
    external_dag_id='final_backup1',  # External DAG to monitor
    external_task_id='converge_task',   # External task to monitor
    execution_date_fn=safe_execution_date_fn,  # Custom function to fetch the execution date
    poke_interval=30,  # Time interval between retries (in seconds)
    timeout=3600,      # Timeout for waiting (in seconds)
    retries=3,         # Number of retries
    retry_delay=timedelta(minutes=5),  # Delay between retries
    mode='reschedule'  # This will free the thread and reschedule the task for next poke
)

    # Complete task after the sensor task succeeds
    complete = EmptyOperator(
        task_id='complete'
    )

    # Define task sequence
    start >> trigger_final_backup1 >> wait_for_task >> complete
