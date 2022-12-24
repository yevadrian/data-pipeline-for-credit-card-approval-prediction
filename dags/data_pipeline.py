#!/usr/bin/python3

from airflow import DAG
from airflow.decorators import task
from google.cloud import storage, bigquery
from pyspark.sql import SparkSession
from sklearn.utils import parallel_backend
from joblibspark import register_spark
from datetime import datetime
from subprocess import Popen, PIPE
from glob import glob
import pandas as pd
import pickle

with DAG(dag_id='data_pipeline', start_date=datetime.now(), schedule='@once'):

    @task
    def load_data_lake():
        for files in glob('datasets/*.csv'):
            client = storage.Client()
            bucket = client.get_bucket('data_fellowship')
            blob = bucket.blob(files.split('/')[1])
            blob.upload_from_filename(files)
        
    @task
    def load_data_warehouse():
        for files in glob('datasets/*.csv'):
            client = bigquery.Client()
            dataset = client.dataset('data_fellowship')

            application_record_schema = [
                bigquery.SchemaField('ID', 'INTEGER'),
                bigquery.SchemaField('CODE_GENDER', 'STRING'),
                bigquery.SchemaField('FLAG_OWN_CAR', 'STRING'),
                bigquery.SchemaField('FLAG_OWN_REALTY', 'STRING'),
                bigquery.SchemaField('CNT_CHILDREN', 'INTEGER'),
                bigquery.SchemaField('AMT_INCOME_TOTAL', 'FLOAT'),
                bigquery.SchemaField('NAME_INCOME_TYPE', 'STRING'),
                bigquery.SchemaField('NAME_EDUCATION_TYPE', 'STRING'),
                bigquery.SchemaField('NAME_FAMILY_STATUS', 'STRING'),
                bigquery.SchemaField('NAME_HOUSING_TYPE', 'STRING'),
                bigquery.SchemaField('DAYS_BIRTH', 'INTEGER'),
                bigquery.SchemaField('DAYS_EMPLOYED', 'INTEGER'),
                bigquery.SchemaField('FLAG_MOBIL', 'INTEGER'),
                bigquery.SchemaField('FLAG_WORK_PHONE', 'INTEGER'),
                bigquery.SchemaField('FLAG_PHONE', 'INTEGER'),
                bigquery.SchemaField('FLAG_EMAIL', 'INTEGER'),
                bigquery.SchemaField('OCCUPATION_TYPE', 'STRING'),
                bigquery.SchemaField('CNT_FAM_MEMBERS', 'FLOAT')
            ]
            credit_record_schema = [
                bigquery.SchemaField('ID', 'INTEGER'),
                bigquery.SchemaField('MONTHS_BALANCE', 'INTEGER'),
                bigquery.SchemaField('STATUS', 'STRING')
            ]
            if files.split('/')[1].split('.')[0] == 'application_record':
                schema = application_record_schema
            elif files.split('/')[1].split('.')[0] == 'credit_record':
                schema = credit_record_schema

            source_uris = f"gs://data_fellowship/{files.split('/')[1]}"
            table_ref = bigquery.TableReference(dataset, f"batch_{files.split('/')[1].split('.')[0]}")
            range_partitioning = bigquery.RangePartitioning(
                field='ID',
                range_=bigquery.PartitionRange(start=0, end=10000000, interval=100000),
            )
            job_config = bigquery.LoadJobConfig(
                source_format='CSV',
                write_disposition='WRITE_TRUNCATE', 
                skip_leading_rows=1,
                schema=schema,
                range_partitioning=range_partitioning
            )
            client.load_table_from_uri(source_uris, table_ref, job_config=job_config)

    @task
    def create_dim_fact():
        command = "dbt seed --project-dir dbt && dbt run --project-dir dbt --models fact_application_record fact_credit_record"
        process = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
        with process.stdout:
            for line in iter(process.stdout.readline, b''):
                print(line.decode('utf-8').strip())

    @task
    def data_transformation():
        command = "dbt run --project-dir dbt --models training_dataset testing_dataset"
        process = Popen(command, shell=True, stdout=PIPE, stderr=PIPE)
        with process.stdout:
            for line in iter(process.stdout.readline, b''):
                print(line.decode('utf-8').strip())

    @task
    def machine_learning():
        spark = SparkSession \
            .builder \
            .master('spark://spark-master:7077') \
            .appName('credit_card') \
            .config('spark.jars', 'connectors/spark-bigquery.jar') \
            .getOrCreate()

        df = spark.read \
            .format('bigquery') \
            .option('viewsEnabled', 'true') \
            .option('materializationDataset', 'data_fellowship') \
            .load('data_fellowship.testing_dataset') \
            .toPandas()

        register_spark()
        with parallel_backend('spark', n_jobs=-1):
            encoder = pickle.load(open('modeling/dumps/ordinal_encoder.pkl', 'rb'))
            model = pickle.load(open('modeling/dumps/random_forest.pkl', 'rb'))

            X = df.drop(['CLIENT_ID'], axis=1)
            X_number = X[['YEARS_AGE', 'YEARS_EMPLOYED', 'CHILDREN_COUNTS', 'FAMILY_MEMBERS', 'ANNUAL_INCOME']]
            X_object = X.drop(X_number.columns, axis=1)
            X_object = pd.DataFrame(encoder.transform(X_object), columns=X_object.columns)
            X = pd.concat([X_number, X_object], axis=1)
            results = model.predict(X)
            y = pd.DataFrame(results, columns=['PREDICTION'])
            df = pd.concat([df, y], axis=1)

            schema = [
                bigquery.SchemaField('CLIENT_ID', 'INTEGER'),
                bigquery.SchemaField('YEARS_AGE', 'INTEGER'),
                bigquery.SchemaField('YEARS_EMPLOYED', 'INTEGER'),
                bigquery.SchemaField('CHILDREN_COUNTS', 'INTEGER'),
                bigquery.SchemaField('FAMILY_MEMBERS', 'INTEGER'),
                bigquery.SchemaField('ANNUAL_INCOME', 'INTEGER'),
                bigquery.SchemaField('INCOME_TYPE', 'STRING'),
                bigquery.SchemaField('EDUCATION_TYPE', 'STRING'),
                bigquery.SchemaField('FAMILY_STATUS', 'STRING'),
                bigquery.SchemaField('HOUSING_TYPE', 'STRING'),
                bigquery.SchemaField('OCCUPATION_TYPE', 'STRING'),
                bigquery.SchemaField('GENDER_TYPE', 'STRING'),
                bigquery.SchemaField('FLAG_OWN_CAR', 'STRING'),
                bigquery.SchemaField('FLAG_OWN_REALTY', 'STRING'),
                bigquery.SchemaField('FLAG_MOBILE_PHONE', 'STRING'),
                bigquery.SchemaField('FLAG_WORK_PHONE', 'STRING'),
                bigquery.SchemaField('FLAG_HOME_PHONE', 'STRING'),
                bigquery.SchemaField('FLAG_EMAIL_ADDRESS', 'STRING'),
                bigquery.SchemaField('PREDICTION', 'STRING')
            ]

            client = bigquery.Client()
            dataset = client.dataset('data_fellowship')
            table_ref = bigquery.TableReference(dataset, 'prediction_results')
            job_config = bigquery.LoadJobConfig(write_disposition='WRITE_TRUNCATE', schema=schema)
            client.load_table_from_dataframe(df, table_ref, job_config=job_config)

    load_data_lake() >> load_data_warehouse() >> create_dim_fact() >> data_transformation() >> machine_learning()