from datetime import datetime, timedelta
import os
import tempfile
import time
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


def download_with_retry(url, max_retries=5):
    """
    Download file with improved retry logic and error handling
    """
    session = requests.Session()
    
    # Configure retry strategy for connection issues
    retries = Retry(
        total=max_retries,
        backoff_factor=2,  # Increased backoff
        status_forcelist=[429, 500, 502, 503, 504],  # Added 429 (rate limit)
        allowed_methods=["GET"],
        raise_on_status=False
    )
    
    adapter = HTTPAdapter(max_retries=retries)
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    
    # Set headers to mimic a real browser
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    for attempt in range(max_retries):
        tmp_file_path = None
        try:
            print(f'Attempting to download {url} (attempt {attempt + 1}/{max_retries})')
            
            # Use longer timeouts and disable SSL verification if needed
            response = session.get(
                url, 
                stream=True, 
                timeout=(30, 300),  # (connect timeout, read timeout)
                headers=headers,
                allow_redirects=True
            )
            
            response.raise_for_status()
            
            # Get expected content length
            content_length = response.headers.get('content-length')
            if content_length:
                expected_size = int(content_length)
                print(f'Expected file size: {expected_size:,} bytes')
            else:
                expected_size = None
                print('Content-Length header not found')
            
            # Create temp file
            with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                tmp_file_path = tmp_file.name
                downloaded_size = 0
                
                # Download in smaller chunks with progress tracking
                for chunk in response.iter_content(chunk_size=8192):  # Smaller chunks
                    if chunk:
                        tmp_file.write(chunk)
                        downloaded_size += len(chunk)
                        
                        # Log progress every 10MB
                        if downloaded_size % (10 * 1024 * 1024) == 0:
                            if expected_size:
                                progress = (downloaded_size / expected_size) * 100
                                print(f'Downloaded: {downloaded_size:,} bytes ({progress:.1f}%)')
                            else:
                                print(f'Downloaded: {downloaded_size:,} bytes')
            
            # Verify download completed successfully
            if expected_size and downloaded_size != expected_size:
                raise IncompleteRead(downloaded_size, expected_size - downloaded_size)
            
            print(f'Successfully downloaded {downloaded_size:,} bytes')
            return tmp_file_path
            
        except (requests.exceptions.RequestException, IncompleteRead, Exception) as e:
            print(f'Download attempt {attempt + 1} failed: {type(e).__name__}: {e}')
            
            # Clean up partial file
            if tmp_file_path and os.path.exists(tmp_file_path):
                os.unlink(tmp_file_path)
                tmp_file_path = None
            
            if attempt < max_retries - 1:
                # Exponential backoff with jitter
                wait_time = (2 ** attempt) + (attempt * 0.1)
                print(f'Waiting {wait_time:.1f} seconds before retry...')
                time.sleep(wait_time)
                continue
            else:
                raise Exception(f'Failed to download {url} after {max_retries} attempts. Last error: {e}')
    
    return None


def download_fuel_data():
    """
    Download fuel data with improved error handling and logging
    """
    current_year = datetime.now().year
    successful_downloads = 0
    failed_downloads = 0
    
    for year in range(2006, current_year + 1):
        for semester in ['01', '02']:
            url = BASE_URL.format(year, semester)
            output_file_name = f'fuel_prices_{year}_{semester}.csv'

            print(f'Processing {year}-{semester}: {url}')

            if check_file_exists(PIPELINE_BUCKET, output_file_name):
                print(f'File {output_file_name} already exists in GCS, skipping download.')
                continue

            tmp_file_path = None
            try:
                tmp_file_path = download_with_retry(url)
                if tmp_file_path:
                    upload_from_temp_file(PIPELINE_BUCKET, output_file_name, tmp_file_path)
                    successful_downloads += 1
                    print(f'Successfully processed {output_file_name}')
                else:
                    failed_downloads += 1
                    print(f'Failed to download {url}')
                    
            except Exception as e:
                failed_downloads += 1
                print(f'Error processing {url}: {e}')
                
            finally:
                # Always clean up temp file
                if tmp_file_path and os.path.exists(tmp_file_path):
                    os.unlink(tmp_file_path)

    print(f'Download summary: {successful_downloads} successful, {failed_downloads} failed')
    
    if failed_downloads > 0:
        print(f'Warning: {failed_downloads} files failed to download')
    
    return f'Data processing completed. {successful_downloads} files uploaded to GCS, {failed_downloads} failed.'


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,  # Reduced since we have internal retry logic
    'retry_delay': timedelta(minutes=10),  # Increased delay
}

with DAG(
    'fuel_price_ingestion',
    default_args=default_args,
    description='Downloads fuel price data from Brazilian government portal',
    schedule_interval=None,
    catchup=False,
    tags=['fuel-prices'],
    max_active_runs=1,  # Prevent concurrent runs
) as dag:

    ingestion_task = PythonOperator(
        task_id='download_and_ingest_fuel_data',
        python_callable=download_fuel_data,
        execution_timeout=timedelta(hours=2),  # Set execution timeout
    )