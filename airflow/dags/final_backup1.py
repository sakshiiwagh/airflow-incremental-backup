import os
from airflow.decorators import task
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.exceptions import AirflowException
from airflow.models import TaskInstance
from airflow.utils.dates import days_ago
from airflow import DAG
from datetime import timedelta
import logging
import shutil

logging.basicConfig(level=logging.INFO)

def choose_branch(*args, **kwargs):
    """Determine which branch to follow based on whether there are files to copy."""
    # Pull the XCom data from the previous task
    files_to_copy_dict = kwargs['ti'].xcom_pull(task_ids='find_files_task')

    # Log the structure of files_to_copy_dict to understand its contents
    logging.info(f"DEBUG: files_to_copy_dict: {files_to_copy_dict}")
    
    # Since find_tasks is expanded, it will return a list of dicts for each src_dir
    # where each dict has the src_dir as key and a list of files as the value
    for result in files_to_copy_dict:
        for src_dir, files in result.items():
            if files:  # Check if there are files to copy
                return 'copying_task'
    
    # If no files to copy
    return 'skip_copy'
 

def send_failure_email_task(context):
    """Send failure email with details about the task failure."""
    task_instance: TaskInstance = context['task_instance']
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    execution_date = context['execution_date']
    exception = context.get('exception', 'No exception details available.')

    # Create the failure email content
    email_content = f"""
    <p>The task <strong>{task_id}</strong> in DAG <strong>{dag_id}</strong> failed.</p>
    <p><strong>Execution Date:</strong> {execution_date}</p>
    <p><strong>Exception:</strong> {exception}</p>
    <p>Check the Airflow logs for more details.</p>
    """

    # Send the failure email
    email = EmailOperator(
        task_id='send_failure_email',
        to='sakshiwagh11@gmail.com',
        subject=f'Failure in task {task_id} of DAG {dag_id}',
        html_content=email_content,
    )
    email.execute(context)  # This triggers the email sending process

# Define the workspace path
workspace_path = ''  #Removing error
source_base = os.path.join(workspace_path, 'Source')
backup_base = os.path.join(workspace_path, 'Backup')

# Source directories
src_dirs = [
    os.path.join(source_base, 'src_dir1'),
    os.path.join(source_base, 'src_dir2'),
]

# Setup the main DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'final_backup1',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    @task(retries=3, retry_delay=timedelta(minutes=5), on_failure_callback=send_failure_email_task)
    def validate_dirs_task():
        """Validate source and backup directories, create backup directories if missing."""
        logging.info("Starting directory validation...")
        missing_dirs = []
        created_dirs = []

        for src_dir in src_dirs:
            backup_dir = src_dir.replace("Source", "Backup").replace("src", "backup")

            # Check if source directory exists
            if not os.path.exists(src_dir):
                missing_dirs.append(src_dir)
                logging.warning(f"Missing source directory: {src_dir}")

            # Check if backup directory exists, create it if missing
            if not os.path.exists(backup_dir):
                os.makedirs(backup_dir)
                created_dirs.append(backup_dir)
                logging.info(f"Created backup directory: {backup_dir}")

        if missing_dirs:
            logging.error(f"Validation failed. Missing source directories: {missing_dirs}")
            raise AirflowException(f"Missing source directories: {', '.join(missing_dirs)}")

        logging.info(f"Validation complete. Created backup directories: {created_dirs}")
        return {"created_dirs": created_dirs}

    @task(retries=3, retry_delay=timedelta(minutes=5), on_failure_callback=send_failure_email_task)
    def initialize_data():
        """Initialize workflow state."""
        logging.info("Initializing workflow state...")
        state = {'workflow_state': 'initialized', 'files_to_copy': {}}
        logging.info(f"Workflow initialized with state: {state}")
        return state

    @task(retries=3, retry_delay=timedelta(minutes=5), on_failure_callback=send_failure_email_task)
    def find_files_task(src_dir):
        """Find new or updated files that need to be backed up."""
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

                    if not dest_file_exists:
                        new_or_updated_files.append(src_file)
                        logging.debug(f"New/Updated file found: {src_file}")
                        continue

                    if src_file_mtime != dest_file_mtime:
                        continue

                    else:
                        new_or_updated_files.append(src_file)
                        logging.debug(f"Updated file found: {src_file}")

            logging.info(f"Found {len(new_or_updated_files)} new or updated files.")
            return new_or_updated_files

        dest_dir = src_dir.replace("Source", "Backup").replace("src", "backup")
        files = find_new_or_updated_files(src_dir, dest_dir)
        logging.info(f"Files to copy from {src_dir}: {files}")
        return {src_dir: files}

    @task(retries=3, retry_delay=timedelta(minutes=5), on_failure_callback=send_failure_email_task,task_id='copying_task')
    def copy_files_task(files_to_copy_dict):
        """Copy files to the backup location."""
        def copy_files(files_to_copy, src_dir, dest_dir):
            backed_up_files = []
            logging.info(f"Starting file copy operation for {len(files_to_copy)} files...")
            for file in files_to_copy:
                try:
                    relative_path = os.path.relpath(file, src_dir)
                    dest_file = os.path.join(dest_dir, relative_path)
                    dest_folder = os.path.dirname(dest_file)

                    if not os.path.exists(dest_folder):
                        os.makedirs(dest_folder)
                        logging.debug(f"Created directory for file: {dest_folder}")

                    shutil.copy2(file, dest_file)
                    backed_up_files.append(dest_file)
                    logging.info(f"Copied file: {file} to {dest_file}")
                except Exception as e:
                    logging.error(f"Error copying file {file}: {e}")
                    raise AirflowException(f"Error copying file: {e}")
            return backed_up_files

        src_dir = list(files_to_copy_dict.keys())[0]
        dest_dir = src_dir.replace("Source", "Backup").replace("src", "backup")
        files_to_copy = files_to_copy_dict[src_dir]
        copied_files = copy_files(files_to_copy, src_dir, dest_dir)
        logging.info(f"File copy operation complete. Copied files: {copied_files}")
        return copied_files

    @task(retries=3, retry_delay=timedelta(minutes=5), on_failure_callback=send_failure_email_task)
    def check_for_files_to_copy(files_to_copy_dict):
        """Check if there are any files to copy and trigger the appropriate task directly."""
        logging.info("Checking for files to copy...")

        # Iterate through the dictionary to check if any directory has files to copy
        for src_dir, files in files_to_copy_dict.items():
            if files:  # If there are files in the list (non-empty)
                logging.info(f"Files found for backup in {src_dir}. Proceeding with copy operation: {files}")
                return 'copy_tasks'
        # If no files to copy
        logging.info("No files found for backup. Skipping the copy operation.")
        return 'skip_copy'  # Return the skip_copy task if no files found

    from airflow.decorators import task
    from datetime import timedelta
    import logging
    from time import sleep

    @task(retries=3, retry_delay=timedelta(minutes=5), on_failure_callback=send_failure_email_task)
    def monitor_copying_task(task_ids=['copying_task'], max_checks=10, check_interval=60, **kwargs):
        """Monitor the status of copying tasks asynchronously."""
        logging.info("Starting monitor for copying tasks...")

        for attempt in range(max_checks):
            all_done = True

            for task_id in task_ids:
                # Pull the XCom value for the task
                status = kwargs['ti'].xcom_pull(task_ids=task_id, key='return_value')

                if status is None:
                    all_done = False
                    logging.info(f"Task {task_id} is still running on check {attempt + 1}/{max_checks}...")
                    break  # Wait and recheck all tasks

            if all_done:
                logging.info("All copying tasks completed successfully.")
                break

            sleep(check_interval)
        else:
            logging.warning("Copying tasks did not complete within the monitoring period.")

    # Define tasks for email notifications
    success_email = EmailOperator(
        task_id='send_success_email',
        to='sakshiwagh11@gmail.com',
        subject='Backup Completed Successfully',
        html_content="""
        <h2>Backup Workflow: Success Notification</h2>
        <p>The backup workflow completed successfully.</p>
        <p><strong>Execution Date:</strong> {{ execution_date }}</p>
        <p>Thank you for using the backup service!</p>
        """
    )

    # Define tasks for empty operation and completion
    skip_copy = EmptyOperator(
        task_id='skip_copy',
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=send_failure_email_task,
    )

    complete_task = EmptyOperator(
        task_id='complete_task',
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=send_failure_email_task
    )
    # Expanding the find_files_task based on source directories
    find_tasks = find_files_task.expand(src_dir=src_dirs)
        
    # Check if we need to copy any files
    branch_task = BranchPythonOperator(
        task_id='branch_task',
            python_callable=choose_branch,
            provide_context=True  # Ensures the context (like find_tasks) is passed to the callable
        )
    # Convergence using EmptyOperator
    converge_task = EmptyOperator(task_id='converge_task',trigger_rule='none_failed_min_one_success')
    # Expanding the copy_files_task based on the output of find_tasks
    copy_tasks = copy_files_task.expand(files_to_copy_dict=find_tasks)

    # Define the task dependencies
    validate_dirs = validate_dirs_task()
    init_task = initialize_data()

    init_task >> validate_dirs >> find_tasks >> branch_task
    branch_task >> [copy_tasks, skip_copy]

    monitor_task = monitor_copying_task(task_ids=['copying_task'])
    branch_task >> [copy_tasks, skip_copy]
    [copy_tasks, monitor_task] >> converge_task
    skip_copy >> converge_task
    converge_task>> success_email >> complete_task

              