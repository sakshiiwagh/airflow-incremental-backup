import requests
from requests.auth import HTTPBasicAuth

# Define the base URL and your credentials
base_url = "http://localhost:8080/api/v1/dags/"
username = "admin"
password = "khKmk6kutyT6N32Z"

# Replace with your DAG and task details
DAG_ID = "final_backup1"  # Your DAG ID
TASK_ID = "copy_files_task"  # Task ID you're querying
DAG_RUN_ID = "scheduled__2024-12-15T00:00:00+00:00"  # The DAG run ID to fetch

# Construct the URL for fetching mapped task instances
url = f"{base_url}{DAG_ID}/dagRuns/{DAG_RUN_ID}/taskInstances/{TASK_ID}/listMapped"

# Make the GET request to the Airflow API
response = requests.get(url, auth=HTTPBasicAuth(username, password))

if response.status_code == 200:
    # If the request is successful, print the task instances
    task_instances = response.json()
    print("Mapped Task Instances:", task_instances)
else:
    # If the request fails, print the error message
    print(f"Failed to fetch task instances: {response.status_code}")
