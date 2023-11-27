from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Define default_args dictionary to specify the default parameters of the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate a DAG
dag = DAG(
    'example_dag',
    default_args=default_args,
    description='A simple example DAG',
    schedule_interval=timedelta(days=1),  # Set the schedule interval
)

# Define a Python function that will be used as the first task
def print_hello():
    print("Hello")

# Task 1: Use the PythonOperator to execute the print_hello function
task_hello = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag,
)

# Define a Python function that will be used as the second task
def print_world():
    print("World")

# Task 2: Use the PythonOperator to execute the print_world function
task_world = PythonOperator(
    task_id='world_task',
    python_callable=print_world,
    dag=dag,
)

# Set the task dependencies
task_hello >> task_world

if __name__ == "__main__":
    dag.cli()
