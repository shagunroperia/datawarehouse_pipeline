from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def greet():
    print("Hello, Airflow!")


def bye():
    print("Bye Bye!!")


# Define the default arguments dictionary
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 16),
}

# Instantiate the DAG
dag = DAG(
    'simple_tutorial_dag',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval='@daily',
    catchup=False,
)

# Define the PythonOperator
greet_task = PythonOperator(
    task_id='greet_task',
    python_callable=greet,
    dag=dag
)

# Define the PythonOperator
bye_task = PythonOperator(
    task_id='bye_task',
    python_callable=bye,
    dag=dag
)

greet_task >> bye_task
