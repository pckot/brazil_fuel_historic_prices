import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(filename)s: %(message)s')


def rename_df_columns(df):
    columns = {
        'Regiao - Sigla': 'region',
        'Estado - Sigla': 'state',
        'Municipio': 'city',
        'Revenda': 'reseller',
        'CNPJ da Revenda': 'reseller_doc',
        'Nome da Rua': 'street_name',
        'Numero Rua': 'street_number',
        'Complemento': 'address_complement',
        'Bairro': 'neighbourhood',
        'Cep': 'zip_code',
        'Produto': 'product',
        'Data da Coleta': 'price_collection_date',
        'Valor de Venda': 'reseller_to_customer_sale_price',
        'Valor de Compra': 'distributor_to_reseller_sale_price',
        'Unidade de Medida': 'unit_of_measure',
        'Bandeira': 'distributor',
    }

    for col, repl in columns.items():
        df = df.withColumnRenamed(col, repl)

    return df


def parse_column_to_float(df, column, chunk_size=10000):
    return df.withColumn(column, F.regexp_replace(F.col(column), ',', '.').cast('float'))


def main():
    if len(sys.argv) != 5:
        logger.info('Incorrect arguments provided. Correct usage: gcs_to_bigquery.py <input_path> <output_dataset> <output_table> <temp_bucket>')
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_dataset = sys.argv[2]
    output_table = sys.argv[3]
    temp_bucket = sys.argv[4]
    
    logger.info(f'Input path: {input_path}')
    logger.info(f'Output dataset: {output_dataset}')
    logger.info(f'Output table: {output_table}')
    logger.info(f'Temp bucket: {temp_bucket}')
    
    logger.info('Initializing Spark session')
    spark = SparkSession.builder \
        .appName('GCS to BigQuery ETL') \
        .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.31.1.jar') \
        .getOrCreate()
    
    # spark.sparkContext.setLogLevel('WARN')
    
    try:
        logger.info(f'Spark version: {spark.version}')

        logger.info(f'Attempting to read: {input_path}')
        # df = spark.read.option('header', 'true') \
        #                 .option('inferSchema', 'true') \
        #                 .csv(input_path)
        df = spark.read \
        .options(
            delimiter=';',
            header=True,
            inferSchema=True,
            encoding='UTF-8',
            dateFormat='dd/MM/yyyy'
        ) \
        .csv(input_path)
        
        logger.info(f'Successfully read data from {input_path}')
        # logger.info(f'Row count: {df.count()}')

        df = rename_df_columns(df)

        logger.info(f'Length before dropping null values: {df.count()}')
        df = df.dropna(subset=['reseller_to_customer_sale_price', 'distributor_to_reseller_sale_price'], how='any')
        logger.info(f'Length after dropping null values: {df.count()}')
        
        logger.info(f'Schema: {df.schema}')

        df = parse_column_to_float(df, 'reseller_to_customer_sale_price')
        df = parse_column_to_float(df, 'distributor_to_reseller_sale_price')
        df = df.withColumn('price_collection_date', F.to_date('price_collection_date', 'd/M/y'))

        logger.info('Sample data:')
        df.show(5, truncate=False)
        
        df.write \
            .format('bigquery') \
            .option('table', f'{output_dataset}.{output_table}') \
            .option('temporaryGcsBucket', temp_bucket.replace('gs://', '')) \
            .option('writeMethod', 'direct') \
            .mode('append') \
            .save()
        
        print(f'Successfully wrote data to BigQuery table {output_dataset}.{output_table}')
        
    except Exception as e:
        print(f'Error processing data: {str(e)}')
        raise
    
    # Clean up
    spark.stop()

if __name__ == '__main__':
    main()