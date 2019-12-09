from datetime import timedelta, datetime

# noinspection PyPackageRequirements
import airflow
# noinspection PyPackageRequirements
from airflow import DAG
# noinspection PyPackageRequirements
from airflow.operators.dummy_operator import DummyOperator
# noinspection PyPackageRequirements
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

# noinspection PyUnresolvedReferences
from airflow.utils.trigger_rule import TriggerRule

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(9),
}


nameList = ['jan', 'peter', 'klaas', 'fred', 'jan', 'klaas', 'blob']


def print_date(**context):
    print(get_week_day(context))


def get_week_day(context):
    return context['execution_date'].weekday()


def email(name: str) -> DummyOperator:
    return DummyOperator(task_id=str(name))


def branch_func(**context):
    return nameList[get_week_day(context)]


with DAG(
    dag_id='exercise6',
        default_args=args,
        schedule_interval=timedelta(hours=2.5)
) as dag:
    print_date = PythonOperator(
        task_id='task1',
        provide_context=True,
        python_callable=print_date
    )

    branching = BranchPythonOperator(
        task_id='branch_task',
        provide_context=True,
        python_callable=branch_func
    )

    sleep = list(map(email, set(nameList)))

    the_end = DummyOperator(
        task_id='the_end',
        trigger_rule=TriggerRule.NONE_FAILED
    )

    print_date >> branching >> sleep >> the_end
