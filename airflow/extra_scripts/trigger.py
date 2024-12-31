import requests
from requests.auth import HTTPBasicAuth
import uuid
import time

# Airflow Web Server URL
base_url = "http://localhost:8080/api/v1/dags/"
dag_id = ""

# Airflow username and password
username = "admin"  # Replace with your Airflow username
password = "NDW9vsZ2mC7UKxqW"  # Replace with your Airflow password

def trigger_dag(dag_id):
    """
    Trigger the specified DAG in Airflow using the API.
    """
    # Generate a unique dag_run_id using timestamp and UUID
    unique_dag_run_id = f"manual_trigger_{int(time.time())}_{uuid.uuid4()}"

    payload = {
        "dag_run_id": unique_dag_run_id,  # Unique DAG run ID
        # Optional: Pass DAG parameters (you can add them here if needed)
        "conf": {"param1": "value1"}
    }

    # Make POST request to trigger the DAG
    response = requests.post(
        f"{base_url}{dag_id}/dagRuns",
        json=payload,
        auth=HTTPBasicAuth(username, password)  # Basic Auth
    )

    # Check the response status
    if response.status_code == 200:
        print(f"DAG {dag_id} triggered successfully.")
        print("Response:", response.json())
    else:
        print(f"Failed to trigger DAG. Status code: {response.status_code}")
        print("Response:", response.text)

def pause_dag(dag_id):
    """
    Pause the specified DAG using the Airflow API.
    """
    response = requests.patch(
        f"{base_url}{dag_id}",
        json={"is_paused": True},
        auth=HTTPBasicAuth(username, password)  # Basic Auth
    )

    # Check the response status
    if response.status_code == 200:
        print(f"DAG {dag_id} paused successfully.")
    else:
        print(f"Failed to pause DAG. Status code: {response.status_code}")
        print("Response:", response.text)

def unpause_dag(dag_id):
    """
    Unpause the specified DAG using the Airflow API.
    """
    response = requests.patch(
        f"{base_url}{dag_id}",
        json={"is_paused": False},
        auth=HTTPBasicAuth(username, password)  # Basic Auth
    )

    # Check the response status
    if response.status_code == 200:
        print(f"DAG {dag_id} unpaused successfully.")
    else:
        print(f"Failed to unpause DAG. Status code: {response.status_code}")
        print("Response:", response.text)

def user_input_handler():
    """
    Ask user for DAG ID and what action to perform.
    """
    global dag_id
    dag_id = input("Enter the DAG ID (e.g., final_backup): ").strip()

    # Ask for action: trigger, pause, or unpause
    action = input("Enter action (trigger/pause/unpause): ").strip().lower()

    if action == "trigger":
        trigger_dag(dag_id)
    elif action == "pause":
        pause_dag(dag_id)
    elif action == "unpause":
        unpause_dag(dag_id)
    else:
        print("Invalid action. Please enter 'trigger', 'pause', or 'unpause'.")

if __name__ == "__main__":
    # Start the user input handler
    user_input_handler()
