from datetime import datetime, timedelta
import os
import re
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator
)
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
from airflow.utils.trigger_rule import TriggerRule

# Environment variables
BQ_DATASET_ID = os.environ.get('BQ_DATASET_ID')
COMPOSER_SA = os.environ.get('COMPOSER_SA')
GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
GCP_REGION = os.environ.get('GCP_REGION')
PIPELINE_BUCKET = os.environ.get('PIPELINE_BUCKET')
SPARK_TEMP_BUCKET = os.environ.get('SPARK_TEMP_BUCKET')

# Default arguments
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 4, 21),
}

# Cluster configuration
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

# Functions for tasks
def check_create_reference_table():
    """Check if the processed_files_reference table exists in BigQuery. If not, create it."""
    bq_hook = BigQueryHook(use_legacy_sql=False)
    client = bq_hook.get_client()
    
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.processed_files_reference"
    
    try:
        client.get_table(table_id)
        print(f"Table {table_id} already exists")
    except Exception as e:
        print(f"Table {table_id} does not exist. Creating it...")
        
        schema = [
            bigquery.SchemaField("file_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("processed_at", "TIMESTAMP", mode="REQUIRED")
        ]
        
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)
        print(f"Table {table_id} created successfully")
    
    return True

def get_files_to_process(**kwargs):
    """
    List files in GCS that match pattern and aren't in BQ reference table.
    Returns the list of files and pushes to XCom.
    """
    # Get files from GCS
    gcs_hook = GCSHook()
    files = gcs_hook.list(PIPELINE_BUCKET)
    
    # Filter files matching pattern
    pattern = r'fuel_prices_\d{4}_\d{2}\.csv'
    matching_files = [f for f in files if re.match(pattern, f)]
    
    # Get already processed files from BigQuery
    bq_hook = BigQueryHook(use_legacy_sql=False)
    
    try:
        processed_files = bq_hook.get_pandas_df(
            f"SELECT file_name FROM `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.processed_files_reference`"
        )
        processed_files_list = processed_files['file_name'].tolist() if not processed_files.empty else []
    except Exception as e:
        print(f"Error querying reference table: {e}")
        processed_files_list = []
    
    # Filter only files that need processing
    files_to_process = [f for f in matching_files if f not in processed_files_list]

    print(f'matching_files = {matching_files}')
    print(f'processed_files_list = {processed_files_list}')
    print(f'files_to_process = {files_to_process}')
    
    print(f"Found {len(matching_files)} matching files")
    print(f"Already processed: {len(processed_files_list)} files")
    print(f"Files to process: {len(files_to_process)}")
    
    # Push the file list to XCom for subsequent tasks
    kwargs['ti'].xcom_push(key='files_to_process', value=files_to_process)
    
    return files_to_process

def choose_path(**kwargs):
    """
    Decide whether to process files or skip processing.
    Branch based on whether there are files to process.
    """
    ti = kwargs['ti']
    files_to_process = ti.xcom_pull(key='files_to_process', task_ids='get_files_to_process')
    
    if files_to_process and len(files_to_process) > 0:
        return 'create_cluster'
    else:
        return 'skip_processing'

# def record_processed_file(file_name, **kwargs):
#     """Record the processed file in the reference table."""
#     processed_file_df = spark.createDataFrame(
#             [(file_name, F.current_timestamp())], 
#             ['file_name', 'processed_at']
#         )

#     processed_file_df.write \
#         .format('bigquery') \
#         .option('table', f'{output_dataset}.processed_files_reference') \
#         .mode('append') \
#         .save()
#     return True

def create_pyspark_job(file_name):
    """Create PySpark job configuration for a file."""
    return {
        'reference': {'project_id': GCP_PROJECT_ID},
        'placement': {'cluster_name': CLUSTER_NAME},
        'pyspark_job': {
            'main_python_file_uri': f'gs://{PIPELINE_BUCKET}/spark_jobs/write_to_bq.py',
            'args': [
                f'gs://{PIPELINE_BUCKET}/{file_name}',
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

# Create the DAG
with DAG(
    dag_id='gcs_to_bq',
    default_args=DEFAULT_ARGS,
    description='ETL pipeline to process data from GCS to BigQuery',
    schedule_interval=None,
    catchup=False,
    tags=['data_ingestion'],
) as dag:

    # 1. Initialize the reference table if it doesn't exist
    init_ref_table = PythonOperator(
        task_id='check_create_reference_table',
        python_callable=check_create_reference_table,
    )

    # 2. Get files that need processing
    get_files = PythonOperator(
        task_id='get_files_to_process',
        python_callable=get_files_to_process,
        provide_context=True,
    )

    # 3. Branch: decide whether to process files or skip
    branch_task = BranchPythonOperator(
        task_id='branch_processing',
        python_callable=choose_path,
        provide_context=True,
    )

    # 4. Skip processing path
    skip_processing = EmptyOperator(
        task_id='skip_processing',
    )

    # 5. Create Dataproc cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id='create_cluster',
        project_id=GCP_PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=GCP_REGION,
        cluster_name=CLUSTER_NAME,
    )

    # 6. Join paths (will execute regardless of which branch was taken)
    join_task = EmptyOperator(
        task_id='join_paths',
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    # 7. Delete Dataproc cluster
    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_cluster',
        project_id=GCP_PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=GCP_REGION,
        trigger_rule=TriggerRule.ONE_SUCCESS,  # Execute even if upstream fails
    )

    # Set up the initial task dependencies
    init_ref_table >> get_files >> branch_task
    branch_task >> skip_processing >> join_task
    branch_task >> create_cluster

    # 8. Create dynamic tasks for each file
    # This happens at DAG definition time, creating tasks for ALL possible files
    # At runtime, only the ones needed will run due to the branching
    
    # We'll limit this to a reasonable number to avoid creating too many tasks
    # In a real-world scenario, you might want to adjust this approach
    max_files = 50  # A reasonable limit to avoid creating too many tasks
    
    # Create tasks for all possible files in the bucket
    gcs_hook = GCSHook()
    try:
        all_files = gcs_hook.list(PIPELINE_BUCKET)
        
        # Filter files matching pattern
        pattern = r'fuel_prices_\d{4}_\d{2}\.csv'
        potential_files = [f for f in all_files if re.match(pattern, f)][:max_files]
    except Exception:
        # If we can't access the bucket at DAG definition time, use placeholder
        potential_files = [f"placeholder_{i}" for i in range(5)]
    
    # Create tasks for each potential file
    for file_name in potential_files:
        file_safe_name = file_name.replace(".", "_").replace("-", "_")
        
        # Create job task for this file
        process_task = DataprocSubmitJobOperator(
            task_id=f'process_{file_safe_name}',
            job=create_pyspark_job(file_name),
            region=GCP_REGION,
            project_id=GCP_PROJECT_ID,
            trigger_rule=TriggerRule.ALL_SUCCESS,  # Only execute if cluster was created
        )
        
        # # Record file as processed
        # record_task = PythonOperator(
        #     task_id=f'record_{file_safe_name}',
        #     python_callable=record_processed_file,
        #     op_kwargs={'file_name': file_name},
        #     trigger_rule=TriggerRule.ALL_SUCCESS,  # Only execute if processing succeeded
        # )
        
        # Set up dependencies
        create_cluster >> process_task >> delete_cluster # >> record_task
    
    # Finalize the DAG
    delete_cluster >> join_task