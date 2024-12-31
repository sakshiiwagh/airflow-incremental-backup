import time
import requests

# Airflow Web Server URL
BASE_URL = "http://localhost:8080/api/v1/dags/"

# Airflow username and password
AUTH = ("admin", "khKmk6kutyT6N32Z")  # Replace with your credentials
POLL_INTERVAL = 5  # Interval in seconds for polling logs


def get_running_dag_run(dag_id):
    """Fetch the latest running DAG run."""
    url = f"{BASE_URL}{dag_id}/dagRuns"
    response = requests.get(url, auth=AUTH, params={"state": "running"})
    if response.status_code == 200:
        runs = response.json().get("dag_runs", [])
        if runs:
            return runs[0]["dag_run_id"]  # Get the latest running instance
    else:
        print(f"Failed to fetch DAG runs: {response.status_code} - {response.reason}")
    return None


def get_running_task_instances(dag_id, dag_run_id):
    """Fetch running task instances for a given DAG run."""
    url = f"{BASE_URL}{dag_id}/dagRuns/{dag_run_id}/taskInstances"
    response = requests.get(url, auth=AUTH)
    if response.status_code == 200:
        task_instances = response.json().get("task_instances", [])
        return [task for task in task_instances if task["state"] == "running"]
    else:
        print(f"Failed to fetch task instances: {response.status_code} - {response.reason}")
    return []


def fetch_task_logs(dag_id, dag_run_id, task_id, try_number):
    """Fetch logs for a specific task instance."""
    url = f"{BASE_URL}{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}"
    response = requests.get(url, auth=AUTH, params={"full_content": "true"})
    if response.status_code == 200:
        print(f"Logs for task '{task_id}':\n{response.text}")
    else:
        print(f"Failed to fetch logs for task '{task_id}': {response.status_code} - {response.reason}")


def poll_logs(dag_id):
    """Poll logs for running tasks of the latest DAG run."""
    while True:
        print("Checking for running DAG instances...")
        dag_run_id = get_running_dag_run(dag_id)
        if not dag_run_id:
            print(f"No running instances found for DAG: {dag_id}")
            time.sleep(POLL_INTERVAL)
            continue

        print(f"Found running DAG instance: {dag_run_id}")
        running_tasks = get_running_task_instances(dag_id, dag_run_id)

        if not running_tasks:
            print(f"No running tasks found for DAG run: {dag_run_id}")
            time.sleep(POLL_INTERVAL)
            continue

        for task in running_tasks:
            task_id = task["task_id"]
            try_number = task["try_number"]
            fetch_task_logs(dag_id, dag_run_id, task_id, try_number)

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    # Take DAG ID input from the user
    dag_id = input("Enter the DAG ID to fetch logs for: ").strip()
    if dag_id:
        poll_logs(dag_id)
    else:
        print("DAG ID is required. Exiting...")
