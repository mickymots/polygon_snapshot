import csv
import pandas as pd
from pandas.io import api
from dataset_builder import get_ticker_info as builder
from os import getenv
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from time import time
from datetime import datetime, timedelta
import os
import numpy as numpy
# from short_loader import main as get_short_data
import logging

import asyncio


output_file = 'dataset.csv'

def run_batch(batch, apiKey):
    with ThreadPoolExecutor(max_workers=40) as executor:
        try:
            logging.debug(apiKey)
            fn = partial(builder, apiKey)
            executor.map(fn, batch)
            executor.shutdown(wait=True)
        except Exception as e:
            print(e)


async def build_dataset(apiKey):
    ts_start = time()
    # batch_size = int(input('Enter Batch Size : '))
    batch_size = 2
    days_to_query = 1 #int(input('Enter Days to Query : '))
    
    csv_df = pd.read_csv('./tickers.csv', header=0, usecols=['symbol'], chunksize=batch_size, iterator=True)
   
    execute_batch(csv_df, apiKey)
    
    # await process_batch(short_df)    
    logging.info('Total Processing Took %s seconds', time() - ts_start)   




# execute the batch of tickers for given numbers of days
def execute_batch(batch_dataframe, apiKey):
    ts_batch_start = time()
    i = 0
    j = 1
    batch = []

    for df in batch_dataframe:

        df = df.rename(columns={c: c.replace(' ', '') for c in df.columns}) 
        df.index += j
        i+=1
        
        for ind in df.index:
            batch.append(df['symbol'][ind])

        logging.debug(batch)

        run_batch(batch, apiKey)

       
    logging.info('Batch Query Took %s seconds', time() - ts_batch_start)







async def main():
    if(getenv('polygon_api_key')):
        apiKey = getenv('polygon_api_key')
        ts_start = time()
        # logging.debug('called to load short interest data ---')
        # short_load = asyncio.create_task(get_short_data())

        # full = input('Enter f for full query operation else press any key : ')

        # short_df = await short_load
        # if full == 'f':
        await build_dataset(apiKey)
        # else:
        #     await process_batch(short_df)
        logging.info('Took %s seconds', time() - ts_start)  
    else:
        raise EnvironmentError('set the apiKey env variable')


if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)
    asyncio.run(main())