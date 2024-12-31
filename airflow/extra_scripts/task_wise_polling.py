import os
import requests
import time
from airflow.models import TaskInstance
from airflow.exceptions import AirflowException
from datetime import timedelta
import logging

def fetch_task_logs(base_url, dag_id, dag_run_id, task_id, username, password):
    """
    Fetches the logs for a given task instance and displays status messages like "queued", "running".
    """
    logs_endpoint = f"{base_url}{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs"
    response = requests.get(logs_endpoint, auth=(username, password))

    if response.status_code == 200:
        logs = response.text
        logging.info(f"Logs for Task {task_id}:\n{logs}")
        # Check for specific log messages like "queued", "running"
        if "queued" in logs:
            print(f"Task {task_id} is currently in the queued state.")
        if "running" in logs:
            print(f"Task {task_id} is running.")
    elif response.status_code == 404:
        print(f"Logs for Task {task_id} not found yet or not available.")
    else:
        print(f"Failed to fetch logs for Task {task_id} in DAG run {dag_run_id}: {response.status_code} - {response.text}")

def fetch_latest_task_instances_until_complete(base_url, dag_id, username, password, timeout=600, poll_interval=5):
    """
    Fetches the task instances for the latest DAG run and waits until all task instances are in the 'success' or 'failed' state.
    It also fetches logs for each task, looking for specific messages like 'queued', 'running'.
    """
    start_time = time.time()

    while time.time() - start_time < timeout:
        print("Polling for the latest DAG run and task instances...")

        # Fetch the list of DAG runs and sort to get the latest one
        dag_run_endpoint = f"{base_url}{dag_id}/dagRuns"
        response = requests.get(dag_run_endpoint, auth=(username, password))

        if response.status_code == 200:
            dag_runs = response.json().get("dag_runs", [])
            if not dag_runs:
                print("No active DAG runs found.")
                break

            # Get the latest DAG run based on execution date
            latest_dag_run = max(dag_runs, key=lambda run: run["execution_date"])
            dag_run_id = latest_dag_run["dag_run_id"]
            dag_run_state = latest_dag_run["state"]

            # Check if the DAG run is in a terminal state (success or failed)
            if dag_run_state in ["success", "failed"]:
                print(f"The latest DAG run {dag_run_id} is in a terminal state: {dag_run_state}. Stopping polling.")
                return

            print(f"Checking task instances for the latest DAG run ID: {dag_run_id}")

            # Fetch task instances for the latest DAG run
            task_instances_endpoint = f"{base_url}{dag_id}/dagRuns/{dag_run_id}/taskInstances"
            task_response = requests.get(task_instances_endpoint, auth=(username, password))

            if task_response.status_code == 200:
                task_instances = task_response.json().get("task_instances", [])
                if task_instances:
                    # Check if any tasks are still running or queued
                    all_complete = True
                    for task in task_instances:
                        task_id = task['task_id']
                        state = task['state']
                        print(f"Task ID: {task_id} - State: {state}")

                        # Fetch and display logs for each task
                        fetch_task_logs(base_url, dag_id, dag_run_id, task_id, username, password)

                        # Check task state
                        if state not in ["success", "failed"]:
                            all_complete = False

                    # If all tasks are in 'success' or 'failed' state, stop polling
                    if all_complete:
                        print(f"All task instances for the latest DAG run {dag_run_id} have completed.")
                        return
                else:
                    print(f"No task instances found for DAG run {dag_run_id}.")
            else:
                print(f"Failed to fetch task instances: {task_response.status_code} - {task_response.text}")
        else:
            print(f"Failed to fetch DAG runs: {response.status_code} - {response.text}")

        # Wait for the next poll cycle
        print(f"\nPolling again in {poll_interval} seconds...\n")
        time.sleep(poll_interval)

    print("Polling finished due to timeout.")

if __name__ == "__main__":
    # Predefined Airflow details
    base_url = "http://localhost:8080/api/v1/dags/"
    username = "admin"
    password = "khKmk6kutyT6N32Z"

    # Get user input for DAG ID
    dag_id = input("Enter the DAG ID: ").strip()

    # Fetch task instances for the latest DAG run and wait until they succeed or fail, also fetch logs
    fetch_latest_task_instances_until_complete(base_url, dag_id, username, password, timeout=600, poll_interval=5)
