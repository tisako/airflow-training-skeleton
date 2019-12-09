from datetime import timedelta

# noinspection PyPackageRequirements
import airflow
# noinspection PyPackageRequirements
from airflow import DAG
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(9),
}


with DAG(
    dag_id='exercise8',
        default_args=args,
        schedule_interval=timedelta(hours=2.5)
) as dag:
    PostgresToGoogleCloudStorageOperator(
        task_id='postges',
        postgres_conn_id='gddconnection',
        google_cloud_storage_conn_id="google_cloud_storage_default",
        sql="select * from land_registry_price_paid_uk where transfer_date == '{{ execution_date.strftime('%Y-%m-%d') }}'",
        bucket='nice_bucket',
        filename='{{execution_date}}'
    )
