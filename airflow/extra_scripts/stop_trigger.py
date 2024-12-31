import requests
from requests.auth import HTTPBasicAuth

# Airflow Web Server URL
base_url = "http://localhost:8080/api/v1/dags/"

# Airflow username and password
username = "admin"  # Replace with your Airflow username
password = "NDW9vsZ2mC7UKxqW"  # Replace with your Airflow password

def update_dag_run_state(dag_id, dag_run_id, new_state="failed"):
    """
    Update the state of a DAG run to the desired state (e.g., 'failed').
    """
    state_url = f"{base_url}{dag_id}/dagRuns/{dag_run_id}"
    payload = {
        "state": new_state  # State to update to (e.g., 'failed')
    }
    
    print(f"Requesting to update DAG run {dag_run_id} state in DAG {dag_id} to {new_state}. URL: {state_url}")
    
    response = requests.patch(
        state_url,
        json=payload,
        auth=HTTPBasicAuth(username, password)  # Basic Auth
    )

    # Log the full response for debugging
    print("Response status code:", response.status_code)
    print("Response content:", response.text)

    if response.status_code == 200:
        print(f"DAG run {dag_run_id} in DAG {dag_id} successfully marked as {new_state}.")
    else:
        print(f"Failed to update DAG run state. Status code: {response.status_code}")
        print("Response:", response.text)

def stop_running_dag(dag_id):
    """
    Stop the running DAG by updating its state to 'failed'.
    """
    # Fetching all DAG runs
    response = requests.get(
        f"{base_url}{dag_id}/dagRuns",
        auth=HTTPBasicAuth(username, password)  # Basic Auth
    )

    if response.status_code == 200:
        dag_runs = response.json()['dag_runs']
        running_dag_found = False  # Flag to track if we found a running DAG run
        
        for dag_run in dag_runs:
            if dag_run['state'] == 'running':
                running_dag_found = True
                dag_run_id = dag_run['dag_run_id']
                print(f"Found running DAG run {dag_run_id}. Marking the DAG run as failed.")
                
                # Mark the DAG run state as 'failed'
                update_dag_run_state(dag_id, dag_run_id, new_state="failed")
        
        if not running_dag_found:
            print(f"No running DAG found for {dag_id}.")
    else:
        print(f"Failed to fetch DAG runs. Status code: {response.status_code}")

if __name__ == "__main__":
    # Take DAG ID input from the user
    dag_id = input("Enter the DAG ID (e.g., final_backup): ").strip()
    
    stop_running_dag(dag_id)
