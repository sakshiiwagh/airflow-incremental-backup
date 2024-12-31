from airflow import DAG
from airflow.operators.python import PythonOperator
from time import sleep
from datetime import datetime

# Define a Python function to simulate delay
def simulate_delay():
    sleep(10)  # Delay for 10 seconds
    print("Task completed after 10 seconds delay")

# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 25),
    'retries': 1,
}

# Define the DAG
with DAG('simulate_task_change', 
         default_args=default_args,
         schedule_interval=None,  # set to None for manual trigger
         catchup=False) as dag:

    # Create 6 tasks, each with a 10-second delay
    task_1 = PythonOperator(
        task_id='task_1',
        python_callable=simulate_delay
    )

    task_2 = PythonOperator(
        task_id='task_2',
        python_callable=simulate_delay
    )

    task_3 = PythonOperator(
        task_id='task_3',
        python_callable=simulate_delay
    )

    task_4 = PythonOperator(
        task_id='task_4',
        python_callable=simulate_delay
    )

    task_5 = PythonOperator(
        task_id='task_5',
        python_callable=simulate_delay
    )

    task_6 = PythonOperator(
        task_id='task_6',
        python_callable=simulate_delay
    )

    # Define the task sequence (all tasks can run in parallel)
    task_5 >> task_4 >> task_3 >> task_6 >> task_2
    #Changing the sequence of tasks