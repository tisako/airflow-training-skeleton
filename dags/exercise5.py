from datetime import timedelta

# noinspection PyPackageRequirements
import airflow
# noinspection PyPackageRequirements
from airflow import DAG
# noinspection PyPackageRequirements
from airflow.operators.bash_operator import BashOperator
# noinspection PyPackageRequirements
from airflow.operators.dummy_operator import DummyOperator
# noinspection PyPackageRequirements
from airflow.operators.python_operator import PythonOperator

# noinspection PyUnresolvedReferences
args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(9),
}


def print_date(**context):
    print(context['execution_date'])


def create_bash_sleep(i: int) -> BashOperator:
    return BashOperator(task_id='wait_' + str(i), bash_command='sleep ' + str(i))


with DAG(
    dag_id='exercise5',
        default_args=args,
        schedule_interval=timedelta(hours=2.5)
) as dag:
    print_date = PythonOperator(
        task_id='task1',
        provide_context=True,
        python_callable=print_date
    )

    sleep = list(map(create_bash_sleep, [1, 5, 10]))

    the_end = DummyOperator(
        task_id='the_end'
    )

    print_date >> sleep >> the_end
