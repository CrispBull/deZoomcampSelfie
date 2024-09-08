#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd
import argparse
import polars as pl
import logging
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.pool import QueuePool
from time import time
import subprocess

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db 
    table_name = params.table_name
    url = params.url
    
    #green_taxi_parquet = 'data/green_taxi_jan_2024.parquet'
    #green_taxi_csv = 'data/green_taxi_jan_2024.csv' 
    #yellow_taxi_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
    #green_taxi_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-01.parquet'
    file_name = os.path.basename(url)
    if table_name == '':
        table_name = os.path.splitext(file_name)[0]
    
    print(f'File name: {file_name}, Table name: {table_name}')
    
    # Download file as it is a parquet file
    #os.system(f'wget {url} -O data/{file_name}')
    command = f"""
    if [ -f "data/{file_name}" ]; then
        echo "File data/{file_name} already exists"
    else 
        wget "{url}" -O "data/{file_name}"
    fi
    """
    subprocess.run(command, shell=True, check=True)
    
    # Create engine with db if doesn't exist and connect to it
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}', 
                           poolclass=QueuePool, pool_size=5, max_overflow=10, connect_args={'connect_timeout': 60, 'options': '-c statement_timeout=3600000'})
    if not database_exists(engine.url):
        create_database(engine.url)
        print("Database created successfully")
    else: 
        print("Database already exists")
        
    engine.connect()
    
    # Read parquet file into df with polars
    
    pl_df = pl.read_parquet(f'data/{file_name}')
    
    # Write polars df to database 
    chunk_threshold = 100_000
    
    t_start = time()
    
    try:
        num_of_rows = pl_df.height
        if num_of_rows > chunk_threshold:
            for i in range(0, num_of_rows, chunk_threshold):
                chunk = pl_df.slice(i, chunk_threshold)
                try:
                    logger.info(f'Inserting chunk {i//chunk_threshold + 1}, rows {i} to {min(i + chunk_threshold, num_of_rows)}')
                    with engine.begin() as conn:
                        chunk.write_database(table_name=table_name, connection=conn, if_table_exists='append')
                except Exception as e:
                    logger.info(f'Error inserting chunk {i//chunk_threshold + 1}: {str(e)}')
                    raise
        else:
            logger.info('Inserting in one chunk!')
            with engine.begin() as conn:
                pl_df.write_database(table_name=table_name, connection=conn, if_table_exists='replace')
            logger.info('Inserting in one chunk completed')
            
        logger.info('Successfully inserted data')
    except SQLAlchemyError as e:
        logger.error(f'Database error occured: {str(e)}')
    except Exception as e:
        logger.error(f'Error inserting data {str(e)}')
    
    t_end = time()
    
    # remove downloaded file
    os.remove(f'data/{file_name}')
    
    print('Inserting data...., took %.3f seconds' %(t_end - t_start))
    
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
    
    parser.add_argument('--user', help='Name of the postgress user', default='root'),
    parser.add_argument('--password', help='Password of the postgress user', default='root'),
    parser.add_argument('--host', help='host for postgress', default='localhost'),
    parser.add_argument('--db', help='Database name for postgres', default='ny_taxi_db'),
    parser.add_argument('--port', type=int, help='Port for postgress', default=5432),
    parser.add_argument('--table_name', help='Name of the postgress database table', default='')
    parser.add_argument('--url', help='Url for the csv', default='https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-01.parquet')

    args = parser.parse_args()
    try:
        main(args)
        logger.info('Data insertion completed!')
    except Exception as e:
        logger.error(f'An error occured: {str(e)}')


# docker run ingest:v001 -it -e POSTGRES_USER="root" -e POSTGRES_PASSWORD="root" -v ${pwd}/ny_taxi_postgres_data:/var/lib/postgresql/data -p 5432:5432 --network=pg-network --name pg-database postgres:16.4






