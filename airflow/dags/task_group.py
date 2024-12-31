from airflow import DAG
from airflow.decorators import task
from datetime import datetime

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': datetime(2024, 12, 29),
}

# Define the DAG
with DAG('task_group',
         default_args=default_args,
         schedule_interval=None,  # No automatic schedule
         catchup=False) as dag:

    # List of words to process
    words = ["Airflow", "Python", "DAG", "Task", "Execution"]

    # Define the task functions with @task decorator
    @task
    def print_length(word):
        print(f"Length of the word '{word}': {len(word)}")

    @task
    def print_uppercase(word):
        print(f"Uppercase of the word '{word}': {word.upper()}")

    @task
    def reverse_word(word):
        print(f"Reversed word of '{word}': {word[::-1]}")

    @task
    def print_hello(word):
        print(f"Hello {word}")

    @task
    def start_task():
        print("Starting the DAG execution...")

    @task
    def end_task():
        print("Ending the DAG execution...")

    # Define the start task
    start = start_task()
    end = end_task()
    # Task group to organize tasks for each word, but tasks for different words will run in parallel
    for word in words:
        # Define tasks for each word
        length = print_length(word)
        uppercase = print_uppercase(word)
        reversed_word = reverse_word(word)
        hello = print_hello(word)

        # Set dependencies to execute tasks in sequence within each word
        length >> uppercase >> reversed_word >> hello

        # Ensure the start task triggers the word tasks and the end task happens after all words are processed
        start >> length  # Start task triggers the first task for each word
        hello >> end  # End task runs after the last task for each word

