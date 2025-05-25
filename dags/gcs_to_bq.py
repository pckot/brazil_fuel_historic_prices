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
from airflow.models.baseoperator import chain

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

def record_processed_files(**kwargs):
    """Record processed files in BigQuery reference table."""
    ti = kwargs['ti']
    files_processed = ti.xcom_pull(key='files_to_process', task_ids='get_files_to_process')
    
    if not files_processed or len(files_processed) == 0:
        print("No files were processed, skipping recording")
        return True
    
    bq_hook = BigQueryHook(use_legacy_sql=False)
    client = bq_hook.get_client()
    
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET_ID}.processed_files_reference"
    
    # For each file, insert a record
    for file_name in files_processed:
        query = f"""
        INSERT INTO `{table_id}` (file_name, processed_at)
        VALUES ('{file_name}', CURRENT_TIMESTAMP())
        """
        
        job_config = bigquery.QueryJobConfig()
        query_job = client.query(query, job_config=job_config)
        query_job.result()  # Wait for the job to complete
        
        print(f"Recorded {file_name} as processed at {datetime.now()}")
    
    return True

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
    
    # Define placeholder for processing tasks to be defined dynamically
    process_tasks = []

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
    
    # 8. Record processed files
    record_files = PythonOperator(
        task_id='record_processed_files',
        python_callable=record_processed_files,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,  # Only execute if all processing tasks succeed
    )

    # Set up the initial task dependencies
    init_ref_table >> get_files >> branch_task
    branch_task >> skip_processing >> join_task
    branch_task >> create_cluster
    
    # Dynamic task creation based on runtime XCom data
    def process_file(ti, **kwargs):
        """Function to check if a file should be processed based on runtime data."""
        files_to_process = ti.xcom_pull(key='files_to_process', task_ids='get_files_to_process')
        file_name = kwargs.get('file_name')
        
        if files_to_process and file_name in files_to_process:
            print(f"Processing file: {file_name}")
            return 'process_file'
        else:
            print(f"Skipping file: {file_name} (already processed or not matching pattern)")
            return 'skip_file'
    
    # Create tasks for all possible files in the bucket
    gcs_hook = GCSHook()
    try:
        all_files = gcs_hook.list(PIPELINE_BUCKET)
        
        # Filter files matching pattern
        pattern = r'fuel_prices_\d{4}_\d{2}\.csv'
        potential_files = [f for f in all_files if re.match(pattern, f)][:50]  # Limit to 50 files
    except Exception:
        # If we can't access the bucket at DAG definition time, use placeholder
        potential_files = [f"placeholder_{i}" for i in range(5)]
    
    # Create a processing subgraph for each potential file
    for file_name in potential_files:
        file_safe_name = file_name.replace(".", "_").replace("-", "_")
        
        # Decision task: should this file be processed?
        file_branch = BranchPythonOperator(
            task_id=f'check_{file_safe_name}',
            python_callable=process_file,
            provide_context=True,
            op_kwargs={'file_name': file_name},
            trigger_rule=TriggerRule.ALL_SUCCESS,  # Only execute if cluster was created
        )
        
        # Task to process the file
        process_file_task = DataprocSubmitJobOperator(
            task_id=f'process_{file_safe_name}',
            job=create_pyspark_job(file_name),
            region=GCP_REGION,
            project_id=GCP_PROJECT_ID,
        )
        
        # Task to skip processing this file
        skip_file_task = EmptyOperator(
            task_id=f'skip_{file_safe_name}',
        )
        
        # Join branches for this file
        file_join = EmptyOperator(
            task_id=f'join_{file_safe_name}',
            trigger_rule=TriggerRule.ONE_SUCCESS,
        )
        
        # Add to list of processing tasks
        process_tasks.append(file_join)
        
        # Set up dependencies for this file's tasks
        create_cluster >> file_branch
        file_branch >> process_file_task >> file_join
        file_branch >> skip_file_task >> file_join
    
    # Connect all file processing joins to record_files task
    # If no files to process, skip directly to deletion
    if process_tasks:
        chain(*process_tasks, record_files, delete_cluster)
    else:
        create_cluster >> delete_cluster
    
    # Finalize the DAG
    delete_cluster >> join_task # MUST COMMIT AGAIN