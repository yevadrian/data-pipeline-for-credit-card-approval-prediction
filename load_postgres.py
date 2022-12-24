#!/usr/bin/python3

import pandas as pd
from sqlalchemy import create_engine

try:
    engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost/postgres')
    print('[INFO] Connection to PostgreSQL Success')
except:
    print('[INFO] Connection to PostgreSQL Failed')

try:
    pd.read_csv('datasets/application_record.csv').to_sql('application_record', con=engine, if_exists='replace', index=False)
    pd.read_csv('datasets/credit_record.csv').to_sql('credit_record', con=engine, if_exists='replace', index=False)
    print('[INFO] Load Data to PostgreSQL Success')
except:
    print('[INFO] Load Data to PostgreSQL Failed')