import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# Hardcoded credentials
username = "admin"  # Your username
password = "NDW9vsZ2mC7UKxqW"  # Your password
auth = HTTPBasicAuth(username, password)

# Hardcoded base URL
base_url = "http://localhost:8080/api/v1/dags/"

def fetch_dag_run_details(dag_id, execution_date):
    """
    Fetch the details of a specific DAG run.
    """
    url = f"{base_url}{dag_id}/dagRuns/{execution_date}"
    response = requests.get(url, auth=auth)

    if response.status_code == 200:
        run_details = response.json()
        print(f"Details for DAG run '{execution_date}':")
        print(run_details)
        return run_details
    else:
        print(f"Error: Received status code {response.status_code} with message: {response.json()}")
        return None

def fetch_task_instances(dag_id, execution_date):
    """
    Fetch task instance data for a specific DAG run.
    """
    url = f"{base_url}{dag_id}/dagRuns/{execution_date}/taskInstances"
    response = requests.get(url, auth=auth)

    if response.status_code == 200:
        tasks = response.json().get('task_instances', [])
        print(f"Fetched {len(tasks)} task instances for DAG '{dag_id}' and run '{execution_date}'.")
        return tasks
    else:
        print(f"Error: Received status code {response.status_code} with message: {response.json()}")
        return []

def plot_bar_chart(task_data, dag_execution_time):
    """
    Plot a bar chart for the given task data, showing task names and execution durations.
    """
    # Convert the task data to DataFrame
    df = pd.DataFrame(task_data)

    # Ensure start_date and end_date are in datetime format
    df['start_date'] = pd.to_datetime(df['start_date'])
    df['end_date'] = pd.to_datetime(df['end_date'])
    df['duration'] = (df['end_date'] - df['start_date']).dt.total_seconds()  # Duration in seconds

    # Filter out tasks with a duration of 0 seconds
    df = df[df['duration'] > 0]

    # Plot the bar chart
    plt.figure(figsize=(12, 8))
    bars = plt.bar(df['task_id'], df['duration'], color='skyblue', width=0.5)

    # Annotate each bar with the execution duration
    for bar, duration in zip(bars, df['duration']):
        plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.1, 
                 f"{int(duration)}s", ha='center', va='bottom', color='black', fontsize=10)

    # Add labels and title with improved styling
    plt.xlabel("Task Name", fontsize=14, fontweight='bold')
    plt.ylabel("Execution Duration (seconds)", fontsize=14, fontweight='bold')
    plt.title("Backup on 5000 Files", fontsize=16, fontweight='bold', color='darkblue')
    plt.xticks(rotation=45, ha='right', fontsize=12)

    # Add DAG execution time below the graph
    plt.figtext(0.5, -0.02, 
                f"Total DAG Execution Time: {dag_execution_time.total_seconds()} seconds", 
                ha='center', va='center', fontsize=12, fontweight='bold', color='darkgreen')

    # Adjust layout for proper display
    plt.tight_layout()

    # Display the chart
    plt.show()

# Replace 'final_backup1' with your DAG ID
dag_id = "final_backup1"
# Replace 'manual__2024-12-25T07:17:12.165064+00:00' with your DAG run's execution date
execution_date = "manual__2024-12-25T07:17:12.165064+00:00"

# Main logic to fetch and plot data
dag_run_details = fetch_dag_run_details(dag_id, execution_date)
if dag_run_details:
    dag_start_time = pd.to_datetime(dag_run_details['start_date'])
    dag_end_time = pd.to_datetime(dag_run_details['end_date'])
    dag_execution_time = dag_end_time - dag_start_time

    print(f"DAG Execution Time: {dag_execution_time.total_seconds()} seconds")

    task_data = fetch_task_instances(dag_id, execution_date)
    if task_data:
        plot_bar_chart(task_data, dag_execution_time)
    else:
        print("No task instances found for the specified DAG run.")
else:
    print("Failed to fetch DAG run details.")
