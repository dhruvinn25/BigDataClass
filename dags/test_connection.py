from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def say_hello():
    print("Hello from Airflow!")


with DAG(
    dag_id="hello_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    hello_task = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,
    )