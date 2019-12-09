from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

with DAG(
    dag_id='exercise1',
    dagrun_timeout=timedelta(seconds=10)
) as dag:
    task1 = DummyOperator(
        task_id='task1'
    )
    task2 = DummyOperator(
        task_id='task2'
    )
    task3 = DummyOperator(
        task_id='task3'
    )
    task4 = DummyOperator(
        task_id='task4'
    )
    task5 = DummyOperator(
        task_id='task5'
    )
    task1 >> task2 >> [task3, task4] >> task5
