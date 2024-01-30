import argparse
from time import time
import os

import numpy as np
import pandas as pd
from sqlalchemy import create_engine

parser = argparse.ArgumentParser(description='Ingest Taxi Parquet into Postgres')

parser.add_argument('--host', help='Postgres host')
parser.add_argument('--port', help='Postgres port')
parser.add_argument('--database', help='Postgres database')
parser.add_argument('--user', help='Postgres user')
parser.add_argument('--password', help='Postgres password')
parser.add_argument('--table', help='Postgres table')
parser.add_argument('--parquet_file_download_link', help='Parquet file download link')
parser.add_argument('--parquet_file', help='Parquet file to ingest')

args = parser.parse_args()

def main(params):
    host = params.host
    port = params.port
    database = params.database
    user = params.user
    password = params.password
    table = params.table
    parquet_file_download_link = params.parquet_file_download_link
    parquet_file = params.parquet_file

    print('Downloading file')
    start = time()
    os.system(f'wget {parquet_file_download_link} -O {parquet_file}')
    end = time()
    print(f'File donloaded in {end - start:.2f} seconds')

    print('Reading parquet file')
    start = time()
    df = pd.read_parquet(parquet_file)
    end = time()
    print(f'File read in {end - start:.2f} seconds')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

    print('Writing to Postgres')   
    df.head(0).to_sql(table, engine, if_exists='replace', index=False)
    dfs = np.array_split(df, 10)
    for df in dfs:
        start = time()
        df.to_sql(table, engine, if_exists='append', index=False)
        end = time()
        print(f'Chunk written to Postgres in {end - start:.2f} seconds')

if __name__ == '__main__':
    
    main(args)
