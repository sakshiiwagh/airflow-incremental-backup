import logging
from airflow.plugins_manager import AirflowPlugin
from airflow.models import TaskInstance, DagRun
from airflow.utils.state import State
from sqlalchemy.event import listen

# Set up logging
logger = logging.getLogger("EventListenerPlugin")

# Task Instance Event Listener
def task_instance_event_listener(target: TaskInstance):
    if target.state == State.RUNNING:
        logger.info(f"Task {target.task_id} in DAG {target.dag_id} is RUNNING.")
    elif target.state == State.SUCCESS:
        logger.info(f"Task {target.task_id} in DAG {target.dag_id} succeeded.")
    elif target.state == State.FAILED:
        logger.info(f"Task {target.task_id} in DAG {target.dag_id} FAILED.")

# DAG Run Event Listener
def dag_run_event_listener(target: DagRun):
    if target.state == State.RUNNING:
        logger.info(f"DAG {target.dag_id} is RUNNING.")
    elif target.state == State.SUCCESS:
        logger.info(f"DAG {target.dag_id} succeeded.")
    elif target.state == State.FAILED:
        logger.info(f"DAG {target.dag_id} FAILED.")

# Register the listeners
def register_listeners():
    listen(TaskInstance, "after_update", task_instance_event_listener)
    listen(DagRun, "after_update", dag_run_event_listener)

# Define the plugin
class EventListenerPlugin(AirflowPlugin):
    name = "event_listener_plugin"

    def on_load(self, *args, **kwargs):
        register_listeners()
        logger.info("Event Listener Plugin loaded.")
