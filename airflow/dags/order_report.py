from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime


def print_order_report():
    print("Generate order report")


with DAG(
    dag_id="order_report",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    task = PythonOperator(
        task_id="report_task",
        python_callable=print_order_report
    )