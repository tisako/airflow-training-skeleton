from datetime import timedelta

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(9),
}


def print_date(**context):
    print(context['execution_date'])


with DAG(
    dag_id='exercise4',
        default_args=args,
        schedule_interval=timedelta(hours=2.5)
) as dag:
    print_date = PythonOperator(
        task_id='task1',
        provide_context=True,
        python_callable=print_date
    )

    sleep = map(lambda i: BashOperator(task_id='wait_' + str(i),
                                       bash_command='sleep ' + str(i)), [1, 5, 10])

    the_end = DummyOperator(
        task_id='the_end'
    )

    print_date >> sleep >> the_end
