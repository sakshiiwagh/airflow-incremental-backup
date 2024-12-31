import os
from airflow.decorators import task
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.exceptions import AirflowException
from airflow.models import TaskInstance
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago
from airflow import DAG
from datetime import timedelta
import shutil

# Define the workspace path
workspace_path = ''  # Update with the actual path
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
    'final_backup2',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    # Failure email function
    def send_failure_email(context):
        """Send failure notification email with error message."""
        task_instance = context.get('task_instance')
        exception = context.get('exception', 'No exception')
        error_message = f"Task {task_instance.task_id} failed due to error: {exception}"
        
        failure_email = EmailOperator(
            task_id='send_failure_email',
            to='sakshiwagh11@gmail.com',
            subject=f'Backup Workflow Failed: {task_instance.task_id}',
            html_content=f"""
            <h2>Backup Workflow: Failure Notification</h2>
            <p>The task <strong>{task_instance.task_id}</strong> failed.</p>
            <p><strong>Execution Date:</strong> {{ execution_date }}</p>
            <p><strong>Error Message:</strong> {error_message}</p>
            <p>Please check the logs for more details.</p>
            """
        )
        return failure_email.execute(context=context)

    # Define the failure callback
    failure_callback = send_failure_email

    @task(retries=3, retry_delay=timedelta(minutes=5), on_failure_callback=failure_callback)
    def validate_dirs_task():
        """Validate source and backup directories, create backup directories if missing."""
        missing_dirs = []
        created_dirs = []

        for src_dir in src_dirs:
            backup_dir = src_dir.replace("Source", "Backup").replace("src", "backup")

            # Check if source directory exists
            if not os.path.exists(src_dir):
                missing_dirs.append(src_dir)

            # Check if backup directory exists, create it if missing
            if not os.path.exists(backup_dir):
                os.makedirs(backup_dir)
                created_dirs.append(backup_dir)

        if missing_dirs:
            raise AirflowException(f"Missing source directories: {', '.join(missing_dirs)}")

        return {"created_dirs": created_dirs}

    @task(retries=3, retry_delay=timedelta(minutes=5), on_failure_callback=failure_callback)
    def initialize_data():
        """Initialize workflow state."""
        state = {'workflow_state': 'initialized', 'files_to_copy': {}}
        return state

    @task(retries=3, retry_delay=timedelta(minutes=5), on_failure_callback=failure_callback)
    def find_files_task(src_dir):
        """Find new or updated files that need to be backed up."""
        def find_new_or_updated_files(src_dir, dest_dir):
            new_or_updated_files = []
            for root, _, files in os.walk(src_dir):
                for file in files:
                    src_file = os.path.join(root, file)
                    relative_path = os.path.relpath(src_file, src_dir)
                    dest_file = os.path.join(dest_dir, relative_path)

                    src_file_mtime = os.path.getmtime(src_file)
                    dest_file_exists = os.path.exists(dest_file)
                    dest_file_mtime = os.path.getmtime(dest_file) if dest_file_exists else None

                    if not dest_file_exists or src_file_mtime != dest_file_mtime:
                        new_or_updated_files.append(src_file)

            return new_or_updated_files

        dest_dir = src_dir.replace("Source", "Backup").replace("src", "backup")
        files = find_new_or_updated_files(src_dir, dest_dir)
        return {src_dir: files}

    @task(retries=3, retry_delay=timedelta(minutes=5), on_failure_callback=failure_callback)
    def copy_files_task(files_to_copy_dict):
        """Copy files to the backup location."""
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

        src_dir = list(files_to_copy_dict.keys())[0]
        dest_dir = src_dir.replace("Source", "Backup").replace("src", "backup")
        files_to_copy = files_to_copy_dict[src_dir]
        copied_files = copy_files(files_to_copy, src_dir, dest_dir)

        # Log the number of files copied and total files found
        total_files = len(files_to_copy)
        copied_files_count = len(copied_files)
        print(f"Copied {copied_files_count} out of {total_files} files from {src_dir} to {dest_dir}.")

        return copied_files

    @task(retries=3, retry_delay=timedelta(minutes=5), on_failure_callback=failure_callback)
    def check_for_files_to_copy(files_to_copy_dict):
        """Check if there are any files to copy and trigger the appropriate task directly."""
        for src_dir, files in files_to_copy_dict.items():
            if files:
                return 'copy_tasks'
        return 'skip_copy'

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
        on_failure_callback=failure_callback
    )

    complete_task = EmptyOperator(
        task_id='complete_task',
        retries=3,
        retry_delay=timedelta(minutes=5),
        on_failure_callback=failure_callback
    )

    # Expanding the find_files_task based on source directories
    find_tasks = find_files_task.expand(src_dir=src_dirs)

    # Check if we need to copy any files
    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=check_for_files_to_copy,
        trigger_rule='none_failed_min_one_success',
        provide_context=True,
        on_failure_callback=failure_callback
    )

    # Convergence using EmptyOperator
    converge_task = EmptyOperator(task_id='converge_task', trigger_rule='none_failed_min_one_success')

    # Expanding the copy_files_task based on the output of find_tasks
    copy_tasks = copy_files_task.expand(files_to_copy_dict=find_tasks)

    # Define the task dependencies
    init_task = initialize_data()
    validate_dirs = validate_dirs_task()
    init_task >> validate_dirs >> find_tasks >> branch_task
    branch_task >> [copy_tasks, skip_copy]
    [copy_tasks, skip_copy] >> converge_task
    converge_task >> success_email >> complete_task
