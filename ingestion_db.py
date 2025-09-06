import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time



logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode = "a"
)

engine = create_engine('sqlite:///inventory.db')

def ingest_db(df,table_name,engine):
    ''' this function will ingest the dataframe into the database table'''
    df.to_sql(
    table_name,
    con=engine,
    if_exists='replace',
    index=False,
    chunksize=20,
    method=None
)

def load_raw_data():
    '''this funtion load the csv as dataframe and ingest into database'''
    start = time.time()
    for file in os.listdir('vendor analysis files'):
        if '.csv' in file:
            df = pd.read_csv('vendor analysis files/'+ file)
            logging.info(f'Ingesting {file} in db')
            ingest_db(df,file[:-4],engine)
    end = time.time()
    total_time = (end - start)/60
    logging.info('Ingestion Completed')
    logging.info(f'Total Time Taken is {total_time} minutes')

if __name__ == '__main__':
    load_raw_data()