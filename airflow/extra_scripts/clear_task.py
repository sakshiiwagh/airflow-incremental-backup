import requests
import json
from datetime import datetime

# Airflow API URL and authentication details
AIRFLOW_URL = "http://localhost:8080/api/v1/dags"
DAG_ID = "final_backup1"
AUTH = ('admin', 'NDW9vsZ2mC7UKxqW')  # Replace with your Airflow credentials

# Step 1: Get the latest DAG run
def get_latest_dag_run(dag_id):
    url = f"{AIRFLOW_URL}/{dag_id}/dagRuns"
    params = {"limit": 1, "order_by": "start_date desc"}  # Fetch the latest run
    response = requests.get(url, auth=AUTH, params=params)
    if response.status_code == 200:
        dag_runs = response.json()['dag_runs']
        if dag_runs:
            return dag_runs[0]['dag_run_id'], dag_runs[0]['execution_date']
    else:
        print("Error fetching DAG runs:", response.status_code, response.text)
    return None, None

# Step 2: Get failed task instances for the latest DAG run
def get_failed_tasks(dag_id, dag_run_id):
    url = f"{AIRFLOW_URL}/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
    response = requests.get(url, auth=AUTH)
    if response.status_code == 200:
        task_instances = response.json()['task_instances']
        failed_tasks = [task['task_id'] for task in task_instances if task['state'] == 'failed']
        return failed_tasks
    else:
        print("Error fetching task instances:", response.status_code, response.text)
    return []

# Step 3: Clear task instances (use the new API endpoint)
def clear_task_instances(dag_id, task_ids, start_date=None, end_date=None):
    url = f"{AIRFLOW_URL}/{dag_id}/clearTaskInstances"
    payload = {
        "task_ids": task_ids,
        "start_date": start_date,
        "end_date": end_date,
        "only_failed": True,  # Only clear failed tasks
        "reset_dag_runs": True  # Reset DAG runs to running state
    }
    response = requests.post(url, auth=AUTH, json=payload)
    if response.status_code == 200:
        print(f"Successfully cleared task instances: {task_ids}")
    else:
        print("Error clearing task instances:", response.status_code, response.text)

# Step 4: Trigger the DAG again
def trigger_dag(dag_id):
    url = f"{AIRFLOW_URL}/{dag_id}/dagRuns"
    payload = {
        "conf": {}  # Any necessary config can be added here
    }
    response = requests.post(url, auth=AUTH, json=payload)
    if response.status_code == 200:
        print(f"Successfully triggered DAG: {dag_id}")
    else:
        print("Error triggering DAG:", response.status_code, response.text)

# Main process
def main():
    # Step 1: Get the latest DAG run
    dag_run_id, execution_date = get_latest_dag_run(DAG_ID)
    if dag_run_id:
        print(f"Latest DAG Run ID: {dag_run_id}, Execution Date: {execution_date}")
        
        # Step 2: Get failed tasks
        failed_tasks = get_failed_tasks(DAG_ID, dag_run_id)
        if failed_tasks:
            print(f"Failed Tasks: {failed_tasks}")
            
            # Step 3: Clear failed tasks using the new API endpoint
            clear_task_instances(DAG_ID, failed_tasks, start_date=execution_date, end_date=execution_date)
            
            # Step 4: Trigger the DAG again
            trigger_dag(DAG_ID)
        else:
            print("No failed tasks found.")
    else:
        print("No DAG runs found for this DAG.")

if __name__ == "__main__":
    main()
