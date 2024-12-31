from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the branching function
def choose_branch(**kwargs):
    """Choose which branch to follow."""
    condition = kwargs['dag_run'].conf.get('condition', 'branch_a')  # Use DAG run config for the condition
    if condition == 'branch_a':
        return 'branch_a_task'
    else:
        return 'branch_b_task'

# Define the DAG
with DAG(
    'simple_branch_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    # Dummy start task
    start = EmptyOperator(task_id='start')

    # Branching task
    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=choose_branch,
        provide_context=True,
    )

    # Branch A task
    branch_a_task = EmptyOperator(task_id='branch_a_task')

    # Branch B task
    branch_b_task = EmptyOperator(task_id='branch_b_task')

    # Task to converge the branches
    join = EmptyOperator(task_id='join', trigger_rule='none_failed_min_one_success')

    # Define the task dependencies
    start >> branching
    branching >> [branch_a_task, branch_b_task]
    [branch_a_task, branch_b_task] >> join
