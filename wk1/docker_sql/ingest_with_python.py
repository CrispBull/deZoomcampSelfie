#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd
import argparse
import polars as pl
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from time import time


# can make more generic
def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db 
    table_name = params.table_name
    url = params.url
    
    green_taxi_parquet = 'data/green_taxi_jan_2024.parquet'
    green_taxi_csv = 'data/green_taxi_jan_2024.csv' 
    yellow_taxi_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
    green_taxi_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-01.parquet'
    
    
    # Download file as it is a parquet file
    os.system(f'wget {url} -O {green_taxi_parquet}')
    
    # Create engine with db if doesn't exist and connect to it
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    if not database_exists(engine.url):
        create_database(engine.url)
        print("Database created successfully")
    else: 
        print("Database already exists")
        
    engine.connect()
    
    # Read parquet file into df with polars
    
    pl_df = pl.read_parquet(green_taxi_parquet)
    
    # Write polars df to database 
    
    t_start = time()
    
    pl_df.write_database(table_name = table_name, connection = engine)
    
    t_end = time()
    
    print('Inserted another chunk...., took %.3f seconds' %(t_end - t_start))
    
    # Using pandas
    # df_iterator = pd.read_csv(f'{green_taxi_csv}', iterator = True, chunksize=100000)
    
    # while True:
    #     t_start = time()  
        
    #     df = next(df_iterator)
    #     df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    #     df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    #     df.to_sql(name = table_name, con = engine, if_exists='append')
        
    #     t_end = time()
    #     print('Inserted another chunk...., took %.3f seconds' %(t_end - t_start))
    


    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV into Postgress')
    
    parser.add_argument('user', help='Name of the postgress user', default='root'),
    parser.add_argument('password', help='Password of the postgress user', default='root'),
    parser.add_argument('host', help='host for postgress', default='localhost'),
    parser.add_argument('db', help='Database name for postgres', default='ny_taxi_db'),
    parser.add_argument('port', type=int, help='Port for postgress', default=5432),
    parser.add_argument('table_name', help='Name of the postgress database table', default='green_taxi_table')
    parser.add_argument('url', help='Url for the csv', default='https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-01.parquet')

    args = parser.parse_args()
    main(args)








