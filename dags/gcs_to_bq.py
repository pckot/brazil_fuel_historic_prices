from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator
)
from airflow.operators.python import PythonOperator


BQ_DATASET_ID = os.environ.get('BQ_DATASET_ID')
COMPOSER_SA = os.environ.get('COMPOSER_SA')
COMPOSER_BUCKET = os.environ.get('COMPOSER_BUCKET')
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
GCP_REGION = os.environ.get('GCP_REGION')
PIPELINE_BUCKET = os.environ.get('PIPELINE_BUCKET')
SPARK_TEMP_BUCKET = os.environ.get('SPARK_TEMP_BUCKET')


DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 4, 21),
}


CLUSTER_NAME = 'etl-spark-cluster-{{ ds_nodash }}'
CLUSTER_CONFIG = {
    'master_config': {
        'num_instances': 1,
        'machine_type_uri': 'e2-standard-2',
        'disk_config': {'boot_disk_type': 'pd-standard', 'boot_disk_size_gb': 30},
    },
    'worker_config': {
        'num_instances': 2,
        'machine_type_uri': 'e2-standard-2',
        'disk_config': {'boot_disk_type': 'pd-standard', 'boot_disk_size_gb': 30},
    },
    'software_config': {
        'image_version': '2.1-debian11',
        'properties': {
            'spark:spark.executor.memory': '1g',
            'spark:spark.driver.memory': '2g',
            'dataproc:dataproc.conscrypt.provider.enable': 'false',
        },
    },
    'gce_cluster_config': {
        'service_account': COMPOSER_SA,
        'service_account_scopes': [
            'https://www.googleapis.com/auth/cloud-platform'
        ],
    },
}


PYSPARK_JOB = {
    'reference': {'project_id': GCP_PROJECT_ID},
    'placement': {'cluster_name': CLUSTER_NAME},
    'pyspark_job': {
        'main_python_file_uri': f'gs://{COMPOSER_BUCKET}/spark_jobs/write_to_bq.py',
        'args': [
            f'gs://{PIPELINE_BUCKET}/fuel_prices_2004_01.csv',
            BQ_DATASET_ID,
            'fuel_prices',
            SPARK_TEMP_BUCKET
        ],
        'jar_file_uris': [
            'gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.31.1.jar'
        ],
        'properties': {
            'spark.executor.memory': '1g',
            'spark.driver.memory': '2g',
            'spark.executor.cores': '1',
            'spark.dynamicAllocation.enabled': 'false',
        },
    },
}

def _print_execution_date(**kwargs):
    print(f'Execution date: {kwargs["execution_date"]}')
    return f'Execution date: {kwargs["execution_date"]}'


with DAG(
    'gcs_to_bq',
    default_args=DEFAULT_ARGS,
    description='ETL pipeline to process data from GCS to BigQuery',
    schedule_interval=None,
    catchup=False,
    tags=['test'],
) as dag:

    print_execution_date = PythonOperator(
        task_id='print_execution_date',
        python_callable=_print_execution_date,
        provide_context=True,
    )

    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        project_id=GCP_PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=GCP_REGION,
        cluster_name=CLUSTER_NAME,
    )

    submit_pyspark_job = DataprocSubmitJobOperator(
        task_id='submit_pyspark_job',
        job=PYSPARK_JOB,
        region=GCP_REGION,
        project_id=GCP_PROJECT_ID,
    )

    delete_dataproc_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        project_id=GCP_PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=GCP_REGION,
        trigger_rule='all_done',
    )

    (
    print_execution_date
    >> create_dataproc_cluster
    >> submit_pyspark_job
    >> delete_dataproc_cluster
    )