from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
import pyodbc  # Assuming you're using a SQL Server database; adjust this based on your database type

# Define the connection ID configured in Airflow (you'll set this up in the Airflow web UI)
CONNECTION_ID = "my_database_connection"

# Define default_args dictionary
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'my_dag',
    default_args=default_args,
    description='A simple example DAG with a database task',
    schedule_interval=timedelta(days=1),
)

def execute_database_task():
    # Use DBeaver connection configured in Airflow
    db_hook = BaseHook.get_hook(CONNECTION_ID)

    # Example SQL query
    sql_query = "SELECT * FROM your_table"

    # Execute the query
    result = db_hook.get_records(sql_query)
    print("Database task result:", result)

# Task 3: Use PythonOperator to execute the execute_database_task function
task_database = PythonOperator(
    task_id='database_task',
    python_callable=execute_database_task,
    dag=dag,
)

# Set the task dependencies
task_hello >> task_world >> task_database

if __name__ == "__main__":
    dag.cli()
