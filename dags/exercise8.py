import json
from tempfile import NamedTemporaryFile

import airflow
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from hooks.rocket_hook import RocketHook

# noinspection PyUnresolvedReferences
args = {"owner": "godatadriven", "start_date": airflow.utils.dates.days_ago(10)}
dag = DAG(
    dag_id="download_rocket_launches",
    default_args=args,
    description="DAG downloading rocket launches from Launch Library.",
    schedule_interval="0 0 * * *",
)


def _download_rocket_launches(ds, tomorrow_ds, **context):
    rocket_hook = RocketHook()
    gcloud_storage_hook = GoogleCloudStorageHook()
    tmp_file_handle = NamedTemporaryFile(delete=True)
    tmp_file_handle.write(rocket_hook.get_lauches(ds, tomorrow_ds).text)
    gcloud_storage_hook.upload(bucket="nice_bucket", filename=tmp_file_handle.name, object=f"rocket_launches/ds={ds}",
                               mime_type='application/json')


def _print_stats(ds, **context):
    gcloud_storage_hook = GoogleCloudStorageHook()
    tmp_file_handle = NamedTemporaryFile(delete=True)
    gcloud_storage_hook.download(bucket="nice_bucket", object=f"rocket_launches/ds={ds}", filename=tmp_file_handle.name)
    data = json.load(tmp_file_handle)
    rockets_launched = [launch["name"] for launch in data["launches"]]
    rockets_str = ""
    if rockets_launched:
        rockets_str = f" ({' & '.join(rockets_launched)})"
        print(f"{len(rockets_launched)} rocket launch(es) on {ds}{rockets_str}.")


download_rocket_launches = PythonOperator(
    task_id="download_rocket_launches",
    python_callable=_download_rocket_launches,
    provide_context=True,
    dag=dag,
)
print_stats = PythonOperator(
    task_id="print_stats", python_callable=_print_stats, provide_context=True, dag=dag
)
download_rocket_launches >> print_stats
