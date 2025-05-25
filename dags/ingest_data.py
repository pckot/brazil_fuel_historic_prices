from datetime import datetime, timedelta
import os
import tempfile
from urllib3.exceptions import IncompleteRead

from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import storage
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PIPELINE_BUCKET = os.environ['PIPELINE_BUCKET']
BASE_URL = 'https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/ca/ca-{0}-{1}.csv'


def check_file_exists(bucket_name, object_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    
    return blob.exists()


def upload_from_temp_file(bucket_name, object_name, temp_file_path):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(temp_file_path)
    print(f'Uploaded {object_name} to {bucket_name}')


def download_with_retry(url, max_retries=3):
    session = requests.Session()
    retries = Retry(
        total=max_retries,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
    )
    session.mount('https://', HTTPAdapter(max_retries=retries))

    for attempt in range(max_retries):
        try:
            with session.get(url, stream=True, timeout=(10, 30)) as response:
                response.raise_for_status()
                with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                    for chunk in response.iter_content(chunk_size=1024*1024):  # 1MB chunks
                        if chunk:
                            tmp_file.write(chunk)
                    return tmp_file.name
        except (requests.exceptions.RequestException, IncompleteRead) as e:
            if attempt < max_retries - 1:
                print(f'Retrying {url} (attempt {attempt + 1}) due to error: {e}')
                continue
            else:
                raise Exception(f'Failed to download {url} after {max_retries} attempts: {e}')


def download_fuel_data():
    current_year = datetime.now().year
    for year in range(2006, current_year + 1):
        for semester in ['01', '02']:
            url = BASE_URL.format(year, semester)
            output_file_name = f'fuel_prices_{year}_{semester}.csv'

            if check_file_exists(PIPELINE_BUCKET, output_file_name):
                print(f'File {output_file_name} already exists in GCS, skipping download.')
                continue

            try:
                tmp_file_path = download_with_retry(url)
                upload_from_temp_file(PIPELINE_BUCKET, output_file_name, tmp_file_path)
                os.unlink(tmp_file_path)  # Clean up temp file
            except Exception as e:
                print(f'Error processing {url}: {e}')
                if os.path.exists(tmp_file_path):
                    os.unlink(tmp_file_path)
                continue

            break
        break

    return 'Data successfully stored in GCS'


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'fuel_price_ingestion',
    default_args=default_args,
    description='Downloads fuel price data from Brazilian government portal',
    schedule_interval=None,
    catchup=False,
    tags=['fuel-prices'],
) as dag:

    ingestion_task = PythonOperator(
        task_id='download_and_ingest_fuel_data',
        python_callable=download_fuel_data,
    )