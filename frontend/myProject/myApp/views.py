import requests
from requests.auth import HTTPBasicAuth
import uuid
import time
from django.shortcuts import render
import logging
# Airflow Web Server URL
base_url = "http://localhost:8080/api/v1/dags/"

# Airflow username and password (Update with your credentials)
username = "admin"  # Replace with your Airflow username
password = "NDW9vsZ2mC7UKxqW"  # Replace with your Airflow password

def trigger_dag(dag_id):
    """
    Trigger the specified DAG in Airflow using the API.
    """
    unique_dag_run_id = f"manual_trigger_{int(time.time())}_{uuid.uuid4()}"
    payload = {
        "dag_run_id": unique_dag_run_id,
        "conf": {"param1": "value1"}  # You can customize this if necessary
    }

    response = requests.post(
        f"{base_url}{dag_id}/dagRuns",
        json=payload,
        auth=HTTPBasicAuth(username, password)
    )

    if response.status_code == 200:
        return f"DAG {dag_id} triggered successfully."
    else:
        return f"Failed to trigger DAG. Status code: {response.status_code}. {response.text}"

def pause_dag(dag_id):
    """
    Pause the specified DAG using the Airflow API.
    """
    response = requests.patch(
        f"{base_url}{dag_id}",
        json={"is_paused": True},
        auth=HTTPBasicAuth(username, password)
    )

    if response.status_code == 200:
        return f"DAG {dag_id} paused successfully."
    else:
        return f"Failed to pause DAG. Status code: {response.status_code}. {response.text}"

def unpause_dag(dag_id):
    """
    Unpause the specified DAG using the Airflow API.
    """
    response = requests.patch(
        f"{base_url}{dag_id}",
        json={"is_paused": False},
        auth=HTTPBasicAuth(username, password)
    )

    if response.status_code == 200:
        return f"DAG {dag_id} unpaused successfully."
    else:
        return f"Failed to unpause DAG. Status code: {response.status_code}. {response.text}"

def manage_dag(request):
    if request.method == 'POST':
        dag_id = request.POST.get('dag_id')
        action = request.POST.get('action')

        if action == 'trigger':
            message = trigger_dag(dag_id)
        elif action == 'pause':
            message = pause_dag(dag_id)
        elif action == 'unpause':
            message = unpause_dag(dag_id)
        else:
            message = "Invalid action. Please enter 'trigger', 'pause', or 'unpause'."

        return render(request, 'myApp/manage_dag.html', {'message': message})

    return render(request, 'myApp/manage_dag.html', {'message': None})

def get_dag_runs(dag_id):
    """
    Fetch the DAG runs for a specific DAG ID from Airflow.
    """
    response = requests.get(
        f"{base_url}{dag_id}/dagRuns",
        auth=HTTPBasicAuth(username, password)
    )
    if response.status_code == 200:
        return response.json()  # Return the JSON response
    else:
        return {
            "error": f"Failed to fetch DAG runs. Status code: {response.status_code}. {response.text}"
        }

def list_dag_runs(request):
    """
    Handle the view to fetch and display DAG runs for a specific DAG ID.
    """
    if request.method == 'POST':
        dag_id = request.POST.get('dag_id')  # Get the DAG ID from the form input
        result = get_dag_runs(dag_id)

        if "error" in result:
            message = result["error"]
            runs = None
        else:
            message = f"DAG Runs for {dag_id}:"
            runs = result.get("dag_runs", [])

        return render(request, 'myApp/list_dag_runs.html', {'message': message, 'runs': runs})

    return render(request, 'myApp/list_dag_runs.html', {'message': None, 'runs': None})

def get_running_dag_run(dag_id):
    """
    Get the currently running DAG run for the given DAG ID.
    """
    response = requests.get(
        f"{base_url}{dag_id}/dagRuns",
        auth=HTTPBasicAuth(username, password)  # Basic Auth
    )

    if response.status_code == 200:
        dag_runs = response.json()['dag_runs']
        for dag_run in dag_runs:
            if dag_run['state'] == 'running':
                return dag_run
        return None
    else:
        return None

def fail_running_dag(dag_id):
    """
    Fail the currently running DAG by changing its state to 'failed'.
    """
    dag_run = get_running_dag_run(dag_id)
    
    if dag_run:
        dag_run_id = dag_run['dag_run_id']
        state_url = f"{base_url}{dag_id}/dagRuns/{dag_run_id}"
        payload = {"state": "failed"}  # Set state to 'failed'
        
        response = requests.patch(
            state_url,
            json=payload,
            auth=HTTPBasicAuth(username, password)  # Basic Auth
        )
        
        if response.status_code == 200:
            return f"DAG run {dag_run_id} in DAG {dag_id} successfully marked as failed."
        else:
            return f"Failed to mark DAG run as failed. Status code: {response.status_code}. {response.text}"
    else:
        return f"No running DAG found for {dag_id}."

def manage_fail_running_dag(request):
    """
    Handle the action to fail a running DAG.
    """
    message = None
    if request.method == 'POST':
        dag_id = request.POST.get('dag_id')
        message = fail_running_dag(dag_id)

    return render(request, 'myApp/fail_running_dag.html', {'message': message})

def fetch_task_logs(base_url, dag_id, dag_run_id, task_id, username, password):
    """
    Fetches the logs for a given task instance and displays status messages like "queued", "running".
    """
    logs_endpoint = f"{base_url}{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs"
    response = requests.get(logs_endpoint, auth=HTTPBasicAuth(username, password))

    if response.status_code == 200:
        logs = response.text
        logging.info(f"Logs for Task {task_id}:\n{logs}")
        # Check for specific log messages like "queued", "running"
        if "queued" in logs:
            return f"Task {task_id} is currently in the queued state."
        if "running" in logs:
            return f"Task {task_id} is running."
    elif response.status_code == 404:
        return f"Logs for Task {task_id} not found yet or not available."
    else:
        return f"Failed to fetch logs for Task {task_id} in DAG run {dag_run_id}: {response.status_code} - {response.text}"

def get_task_states(dag_id, dag_run_id):
    """
    Fetch task states for the latest DAG run.
    """
    task_instances_endpoint = f"{base_url}{dag_id}/dagRuns/{dag_run_id}/taskInstances"
    task_response = requests.get(task_instances_endpoint, auth=HTTPBasicAuth(username, password))
    
    task_states = []

    if task_response.status_code == 200:
        task_instances = task_response.json().get("task_instances", [])
        if task_instances:
            for task in task_instances:
                task_id = task['task_id']
                state = task['state']
                log_status = fetch_task_logs(base_url, dag_id, dag_run_id, task_id, username, password)
                task_states.append({'task_id': task_id, 'state': state, 'log': log_status})
    else:
        task_states.append({'error': 'Failed to fetch task instances'})
    
    return task_states

def monitor_dag(request):
    """
    Monitor task instances for a specific DAG and check their statuses.
    """
    if request.method == 'POST':
        dag_id = request.POST.get('dag_id')  # Get the DAG ID from the form input
        result = get_dag_runs(dag_id)

        if "error" in result:
            message = result["error"]
            tasks = None
        else:
            latest_dag_run = max(result['dag_runs'], key=lambda run: run["execution_date"])
            dag_run_id = latest_dag_run['dag_run_id']
            message = f"Monitoring tasks for DAG run {dag_run_id}:"
            tasks = get_task_states(dag_id, dag_run_id)

        return render(request, 'myApp/monitor_dag.html', {'message': message, 'tasks': tasks})

    return render(request, 'myApp/monitor_dag.html', {'message': None, 'tasks': None})

def home(request):
    return render(request, 'myApp/home.html')