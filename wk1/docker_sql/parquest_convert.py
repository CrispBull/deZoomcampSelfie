import os
import dask.dataframe as dd
from time import time
import pandas as pd
import polars as pl

# Compares parsing times for dask, pqrs, pandas, pyarrow, fastparquet, and polars
def convert_parquet_to_csv_benchmark():
    parq_file = 'data/yellow_tripdata_2021-01.parquet'
    
    print("Using dask...")
    t_start = time()
    
    df = dd.read_parquet(parq_file)
    df.to_csv('data/dask_converted.csv', index = False) 
    
    t_end = time()
    print('Using Dask took %.3f seconds\n' %(t_end - t_start))
    
    print("Using pqrs...")
    t_start = time()
    
    # be sure you have pqrs rust cmdline tool installed
    os.system(f'pqrs cat {parq_file} --csv > data/pqrs_converted.csv')
    
    t_end = time()
    print('Using pqrs took %.3f seconds\n' %(t_end - t_start))
    
    print("Using pandas with fastparquet...")
    t_start = time()
    
    pandas_parquet_fastparquet = pd.read_parquet(parq_file, engine='fastparquet')
    pandas_parquet_fastparquet.to_csv('data/fastparquet_converted.csv', index=False)
    
    t_end = time()
    print('Using fastparquet took %.3f seconds\n' %(t_end - t_start))
    
    print("Using pandas with pyarrow...")
    t_start = time()
    
    pandas_parquet_pyarrow = pd.read_parquet(parq_file)
    pandas_parquet_pyarrow.to_csv('data/pyarrow_converted.csv', index=False) 
    
    t_end = time()
    print('Using pyarrow took %.3f seconds\n' %(t_end - t_start))
    
    print("Using polars...")
    t_start = time()
    
    polars_parquet = pl.read_parquet(parq_file)
    polars_parquet.write_csv('data/polars_converted.csv')
    
    t_end = time()
    print('Using polars took %.3f seconds' %(t_end - t_start))
    
    
    
if __name__ == "__main__":
    import sys
    print("Running program/n")
    convert_parquet_to_csv_benchmark()
    