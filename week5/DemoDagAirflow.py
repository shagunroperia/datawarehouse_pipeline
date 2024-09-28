from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Define the default arguments dictionary
default_args = {
    'owner': 'shagun',
    'start_date': datetime(2024, 9, 15),
}

# Instantiate the DAG
dag = DAG(
    'DemoDagAirflow',
    default_args=default_args,
    description='A demo for DAG with Python, Bash',
    schedule_interval='@daily',
    catchup=False,
)

# Define Python functions for PythonOperator tasks
def hello_airflow():
    print("Hello Airflow")

def my_args(*args, **kwargs):
    print("My Args are: {}".format(args))
    print("My Kwargs are: {}".format(kwargs))

# Task 1: BashOperator to print a message
t1 = BashOperator(
    task_id='bash',
    bash_command='echo "Hello Airflow"',
    dag=dag,
)

# Task 2: PythonOperator to print "Hello Airflow" from a Python function
t2 = PythonOperator(
    task_id='python',
    python_callable=hello_airflow,
    dag=dag,
)

# Task 3: PythonOperator to pass and print arguments and keyword arguments
t3 = PythonOperator(
    task_id='python_with_arguments',
    python_callable=my_args,
    op_args=['a', 'b', 'c'],  # List of positional arguments
    op_kwargs={'a': '2'},  # Dictionary of keyword arguments
    dag=dag,
)

# # Task 4: SnowflakeOperator to execute a simple SELECT query
# t4 = SnowflakeOperator(
#     task_id='snowflake_query',
#     snowflake_conn_id='snowflakes_connection',  # Ensure this is set up in Airflow UI
#     sql="SELECT * FROM DEMO_DB.RAW_DATA.company_metadata LIMIT 10;",
#     dag=dag,
# )

# Set task dependencies
t1 >> t2 >> t3
