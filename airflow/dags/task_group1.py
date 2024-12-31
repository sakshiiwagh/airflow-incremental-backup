from airflow import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from datetime import datetime

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'start_date': datetime(2024, 12, 29),
}

# Define the DAG
with DAG('task_group_example_dag',
         default_args=default_args,
         schedule_interval=None,  # No automatic schedule
         catchup=False) as dag:

    # Define some sample task functions with @task decorator
    @task
    def print_length(word):
        print(f"Length of the word '{word}': {len(word)}")

    @task
    def print_uppercase(word):
        print(f"Uppercase of the word '{word}': {word.upper()}")

    @task
    def reverse_word(word):
        reversed_word = word[::-1]
        print(f"Reversed word of '{word}': {reversed_word}")
        return reversed_word  # Return reversed word for further tasks

    @task
    def print_hello(word):
        print(f"Hello {word}")

    @task
    def contains_o(word):
        if 'o' in word.lower():
            print(f"The word '{word}' contains the letter 'o'.")
        else:
            print(f"The word '{word}' does not contain the letter 'o'.")

    @task
    def count_o(word):
        count = word.lower().count('o')
        print(f"The word '{word}' contains {count} 'o' characters.")

    # List of words to process
    words = ["Airflow", "Python", "TaskGroup", "DAG", "Workflow"]

    # Add task dependencies outside of TaskGroup
    @task
    def start_task():
        print("Starting the DAG execution...")

    @task
    def end_task():
        print("Ending the DAG execution...")

    start_task = start_task()
    end_task = end_task()

    # String operations TaskGroup
    with TaskGroup("string_operations_group") as string_group:
        for word in words:
            # First, reverse the word
            reversed_word = reverse_word(word)

            # Perform other tasks on the reversed word
            length = print_length(reversed_word)
            uppercase = print_uppercase(reversed_word)
            hello = print_hello(reversed_word)

            # Set task dependencies for the word-specific tasks
            reversed_word >> length >> uppercase >> hello

    # New task group for checking and counting 'o' characters
    with TaskGroup("o_character_operations_group") as o_char_group:
        for word in words:
            reversed_word = reverse_word(word)  # Reverse the word first
            contains_o_task = contains_o(reversed_word)
            count_o_task = count_o(reversed_word)

            # Set task dependencies
            contains_o_task >> count_o_task

    # Set dependencies for task groups and start/end tasks
    start_task >> [string_group, o_char_group] >> end_task
