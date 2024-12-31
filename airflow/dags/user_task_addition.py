from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import timedelta
import logging
import os
import shutil
from airflow.exceptions import AirflowException

# Enable logging
logging.basicConfig(level=logging.INFO)

def find_new_or_updated_files(src_dir, dest_dir):
    new_or_updated_files = []
    logging.info(f"Scanning directory: {src_dir} for new or updated files...")
    for root, _, files in os.walk(src_dir):
        for file in files:
            src_file = os.path.join(root, file)
            relative_path = os.path.relpath(src_file, src_dir)
            dest_file = os.path.join(dest_dir, relative_path)
            src_file_mtime = os.path.getmtime(src_file)
            dest_file_exists = os.path.exists(dest_file)
            dest_file_mtime = os.path.getmtime(dest_file) if dest_file_exists else None
            if not dest_file_exists or src_file_mtime > dest_file_mtime:
                new_or_updated_files.append(src_file)
    return new_or_updated_files

def copy_files(files_to_copy, src_dir, dest_dir):
    backed_up_files = []
    for file in files_to_copy:
        try:
            relative_path = os.path.relpath(file, src_dir)
            dest_file = os.path.join(dest_dir, relative_path)
            dest_folder = os.path.dirname(dest_file)
            if not os.path.exists(dest_folder):
                os.makedirs(dest_folder)
            shutil.copy2(file, dest_file)
            backed_up_files.append(dest_file)
        except Exception as e:
            raise AirflowException(f"Error copying file: {e}")
    return backed_up_files

# Define the tasks outside of the DAG to avoid dynamic task creation inside the DAG context
@task(task_id='find_files_task', retries=3, retry_delay=timedelta(minutes=5))
def find_files_task(src_dirs):
    files_dict = {}
    for src_dir in src_dirs:
        dest_dir = src_dir.replace("src", "backup")
        files = find_new_or_updated_files(src_dir, dest_dir)
        logging.info(f"Files to be copied from {src_dir}: {files}")
        files_dict[src_dir] = files
    return files_dict

@task(task_id='copy_files_task', retries=3, retry_delay=timedelta(minutes=5))
def copy_files_task(files_to_copy_dict):
    all_backed_up_files = []
    for src_dir, files_to_copy in files_to_copy_dict.items():
        dest_dir = src_dir.replace("src", "backup")
        backed_up_files = copy_files(files_to_copy, src_dir, dest_dir)
        logging.info(f"Backed up files from {src_dir}: {backed_up_files}")
        all_backed_up_files.extend(backed_up_files)
    return all_backed_up_files

# Task creation function to add dynamic tasks
def task_addition(task_list, src_dirs):
    tasks = []
    for task_name in task_list:
        if task_name == 'find_files_task':
            tasks.append(find_files_task(src_dirs))
        elif task_name == 'copy_files_task':
            # This is the fix: we pass the output of `find_files_task` to `copy_files_task`
            find_files_task_output = find_files_task(src_dirs)  # Get the output of find_files_task
            tasks.append(copy_files_task(find_files_task_output))  # Pass the output as input to copy_files_task
        else:
            @task(task_id=task_name, retries=3, retry_delay=timedelta(minutes=5))
            def dynamic_task():
                logging.info(f"Executing task: {task_name}")
                return task_name
            tasks.append(dynamic_task())
    return tasks

# Example task list and source directories provided by the user
task_list = ['find_files_task', 'copy_files_task']
src_dirs = ['src_dir1', 'src_dir2']  # Add all source directories here

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'user_defined_workflow',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Start task
    start_task = EmptyOperator(task_id='start')

    # Add user-defined tasks
    user_tasks = task_addition(task_list, src_dirs)

    # End task
    end_task = EmptyOperator(task_id='end')

    # Define the sequence: start -> user_tasks -> end
    start_task >> user_tasks >> end_task
