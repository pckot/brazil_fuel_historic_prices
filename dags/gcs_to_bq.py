from datetime import datetime, timedelta
import os
import re
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator
)
from airflow.decorators import task, dag
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain

BQ_DATASET_ID = os.environ.get('BQ_DATASET_ID')
COMPOSER_SA = os.environ.get('COMPOSER_SA')
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


@dag(
    dag_id='gcs_to_bq',
    default_args=DEFAULT_ARGS,
    description='ETL pipeline to process data from GCS to BigQuery',
    schedule_interval=None,
    catchup=False,
    tags=['data_ingestion'],
)
def gcs_to_bq_dag():
    
    @task
    def check_create_reference_table():
        """
        Check if the processed_files_reference table exists in BigQuery. 
        If not, create it.
        """
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
        
        return True  # Indicate successful completion
    
    @task
    def get_files_to_process():
        """
        List all files in the GCS bucket that match the pattern fuel_prices_YEAR_SEMESTER.csv
        and check which ones have not been processed yet.
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
        
        print(f"Found {len(matching_files)} matching files")
        print(f"Already processed: {len(processed_files_list)} files")
        print(f"Files to process: {len(files_to_process)}")
        
        # Return list of files to process (automatically pushed to XCom)
        return files_to_process
    
    @task
    def should_create_cluster(files_to_process):
        """
        Determine if we should create a cluster based on whether there are files to process.
        """
        has_files = len(files_to_process) > 0
        print(f"Has files to process: {has_files}")
        return has_files
    
    # Execute the tasks
    ref_table_created = check_create_reference_table()
    files_to_process = get_files_to_process()
    create_cluster_decision = should_create_cluster(files_to_process)
    
    # Set up dependencies for the initial tasks
    chain(ref_table_created, files_to_process, create_cluster_decision)
    
    # Create the cluster using the traditional operator
    create_cluster = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster',
        project_id=GCP_PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=GCP_REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule='none_failed_or_skipped',  # Only create if there are files to process
    )
    
    # Make the create_cluster task depend on the create_cluster_decision
    create_cluster_decision >> create_cluster
    
    # Delete cluster operator
    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster',
        project_id=GCP_PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=GCP_REGION,
        trigger_rule='all_done',  # Run regardless of upstream task status
    )
    
    # Process files only if there are files to process
    with TaskGroup(group_id="process_files_group") as process_files_group:
        @task
        def prepare_job_config(file_name):
            """Create PySpark job config for a specific file."""
            pyspark_job = {
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
            return pyspark_job
        
        @task
        def record_processed_file(file_name):
            """Record the processed file in the reference table."""
            bq_hook = BigQueryHook(use_legacy_sql=False)
            
            query = f"""
            INSERT INTO `{GCP_PROJECT_ID}.{BQ_DATASET_ID}.processed_files_reference` 
            (file_name, processed_at)
            VALUES('{file_name}', CURRENT_TIMESTAMP())
            """
            
            bq_hook.run_query(query)
            print(f"Recorded {file_name} as processed")
            return True
        
        @task
        def process_all_files(files_to_process):
            """Return the list of files to process for dynamic task mapping"""
            return files_to_process if files_to_process else []
        
        # Map over the files to process
        file_list = process_all_files(files_to_process)
        job_configs = prepare_job_config.expand(file_name=file_list)
        
        # Create a list of DataprocSubmitJobOperator tasks
        submit_jobs = []
        for idx, file_name in enumerate(file_list):
            # We need to use this workaround since we can't use .expand() with traditional operators
            file_safe_name = file_name.replace(".", "_").replace("-", "_")
            submit_job = DataprocSubmitJobOperator(
                task_id=f'submit_job_{file_safe_name}',
                job=job_configs[idx],  # This will be resolved at runtime
                region=GCP_REGION,
                project_id=GCP_PROJECT_ID,
            )
            submit_jobs.append(submit_job)
            
            # Record after job submission
            recorded = record_processed_file(file_name)
            submit_job >> recorded
    
    # Create task dependencies
    create_cluster >> process_files_group >> delete_cluster
    
    # Define a task to skip the cluster creation/deletion if no files
    @task(trigger_rule='none_failed')
    def finalize_dag(has_files_to_process):
        if not has_files_to_process:
            print("No files to process, DAG is complete")
        else:
            print("All files processed, DAG is complete")
        return True
    
    # Final task that runs in all cases
    dag_complete = finalize_dag(create_cluster_decision)
    
    # If no files to process, skip the cluster creation and go straight to complete
    create_cluster_decision >> dag_complete
    delete_cluster >> dag_complete

# Create the DAG
dag = gcs_to_bq_dag()