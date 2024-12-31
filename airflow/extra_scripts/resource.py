import os
import psutil
import requests
from prettytable import PrettyTable
from datetime import datetime

def parse_datetime_with_timezone(datetime_str):
    """
    Parse datetime string with a potential invalid timezone format and correct it.
    :param datetime_str: The datetime string from the Airflow API
    :return: A datetime object
    """
    if datetime_str.endswith("+00:0"):
        # Correct the timezone format by adding an extra zero to make it +00:00
        datetime_str = datetime_str[:-2] + "00:00"
    # Handle the case where there might be a 'Z' at the end, common in ISO formats
    if datetime_str.endswith("Z"):
        datetime_str = datetime_str[:-1]  # Remove 'Z' if it exists

    return datetime.fromisoformat(datetime_str)  # Now it should be in the correct format

def get_successful_dag_resource_usage(dag_id):
    """
    Checks resource usage for all tasks in the latest successful DAG run using Airflow's API with Basic Auth.

    :param dag_id: The ID of the DAG to monitor
    :return: A dictionary of task resource utilization and total queue time
    """
    # Hardcoded credentials
    username = "admin"  # Your username
    password = "NDW9vsZ2mC7UKxqW"  # Your password
    auth = (username, password)

    # Hardcoded base URL
    base_url = "http://localhost:8080/api/v1/dags/"

    # Validate if the DAG exists
    dag_url = f"{base_url}{dag_id}"
    try:
        response = requests.get(dag_url, auth=auth, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
        return

    print(f"Checking resource utilization for DAG: {dag_id}\n")

    resource_usage = {}
    task_running_times = []

    # Fetch successful DAG runs for the given DAG
    dag_runs_url = f"{base_url}{dag_id}/dagRuns?limit=10&order_by=-execution_date"
    try:
        response = requests.get(dag_runs_url, auth=auth, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching DAG runs: {e}")
        return

    dag_runs = response.json().get("dag_runs", [])
    if not dag_runs:
        print("No DAG runs found.")
        return

    # Find the latest successful DAG run
    latest_successful_run = next((run for run in dag_runs if run["state"] == "success"), None)
    if not latest_successful_run:
        print("No successful DAG runs found.")
        return

    dag_run_id = latest_successful_run["dag_run_id"]
    dag_start_time = parse_datetime_with_timezone(latest_successful_run["start_date"])
    dag_end_time = parse_datetime_with_timezone(latest_successful_run["end_date"])
    dag_execution_time = (dag_end_time - dag_start_time).total_seconds()

    task_instances_url = f"{base_url}{dag_id}/dagRuns/{dag_run_id}/taskInstances"
    try:
        response = requests.get(task_instances_url, auth=auth, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching task instances: {e}")
        return

    task_instances = response.json().get("task_instances", [])

    for task_instance in task_instances:
        task_id = task_instance["task_id"]
        state = task_instance["state"]

        # Check resource usage if the task state was successful
        if state == "success":
            try:
                # Get task timing information
                start_time = parse_datetime_with_timezone(task_instance["start_date"])
                end_time = parse_datetime_with_timezone(task_instance["end_date"])

                task_running_time = (end_time - start_time).total_seconds()
                task_running_times.append(task_running_time)  # Store the task running time

                # Placeholder for RAM usage (replace PID handling logic as necessary)
                pid = os.getpid()  # Temporary placeholder for testing
                if pid and psutil.pid_exists(pid):
                    process = psutil.Process(pid)
                    memory_usage_in_ram = process.memory_info().rss / (1024 * 1024)  # RAM in MB
                else:
                    memory_usage_in_ram = 0.0  # Set to 0 if no process found

                resource_usage[task_id] = {
                    "memory_usage_mb": memory_usage_in_ram,
                    "task_running_time": task_running_time,
                }
            except Exception as e:
                print(f"\tUnable to fetch resource usage for task '{task_id}': {e}")

    return resource_usage, dag_execution_time

if __name__ == "__main__":
    # Input the DAG ID
    dag_id = input("Enter the DAG ID to check resource usage: ")

    usage, dag_execution_time = get_successful_dag_resource_usage(dag_id)

    if usage:
        print("\nResource Utilization Summary:")

        # Prepare table output
        table = PrettyTable()
        table.field_names = ["Task", "RAM Usage (MB)", "Running Time (s)"]
        run_time = 0
        for task_id, stats in usage.items():
            table.add_row([ 
                task_id,
                f"{stats['memory_usage_mb']:.2f}",
                f"{stats['task_running_time']:.2f}",
            ])
            run_time += stats['task_running_time']

        print(table)
        queue_time = dag_execution_time-run_time
        print(f"Task Running Time: {run_time:.2f} seconds")
        print(f"Total Queue Time: {queue_time:.2f} seconds")
        print(f"DAG Execution Time: {dag_execution_time:.2f} seconds")

