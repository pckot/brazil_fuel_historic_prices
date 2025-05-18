import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


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
        sys.exit(1)
    
    input_path = sys.argv[1]
    output_dataset = sys.argv[2]
    output_table = sys.argv[3]
    temp_bucket = sys.argv[4]
    
    spark = SparkSession.builder \
        .appName('GCS to BigQuery ETL') \
        .config('spark.jars', 'gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.31.1.jar') \
        .getOrCreate()
    
    # spark.sparkContext.setLogLevel('WARN')
    
    try:
        df = spark.read \
        .options(
            delimiter=';',
            header=True,
            inferSchema=True,
            encoding='UTF-8',
            dateFormat='dd/MM/yyyy'
        ) \
        .csv(input_path)

        df = rename_df_columns(df)

        df = df.dropna(
            subset=['reseller_to_customer_sale_price', 'distributor_to_reseller_sale_price'],
            how='any'
        )

        df = parse_column_to_float(df, 'reseller_to_customer_sale_price')
        df = parse_column_to_float(df, 'distributor_to_reseller_sale_price')
        df = df.withColumn('price_collection_date', F.to_date('price_collection_date', 'd/M/y'))
        
        df.write \
            .format('bigquery') \
            .option('table', f'{output_dataset}.{output_table}') \
            .option('temporaryGcsBucket', temp_bucket.replace('gs://', '')) \
            .option('writeMethod', 'direct') \
            .mode('append') \
            .save()
        
        print('\n\n')
        print('INPUT PATH:')
        print(input_path.split('/')[-1])
        print('\n\n')

        update_reference_tbl_df = spark.sql(f"""
            SELECT 
                '{input_path.split('/')[-1]}' AS file_name,
                CURRENT_TIMESTAMP() AS processed_at
        """)

        print("DataFrame Schema:")
        update_reference_tbl_df.printSchema()
        
        print("DataFrame Content:")
        update_reference_tbl_df.show(truncate=False)

        update_reference_tbl_df.write \
            .format('bigquery') \
            .option('table', f'{output_dataset}.processed_files_reference') \
            .option('schema', 'file_name:STRING,processed_at:TIMESTAMP') \
            .option('temporaryGcsBucket', temp_bucket.replace('gs://', '')) \
            .option('writeMethod', 'direct') \
            .mode('append') \
            .save()
        
    except Exception as e:
        print(f'Error processing data: {str(e)}')
        raise
    
    spark.stop()


if __name__ == '__main__':
    main()