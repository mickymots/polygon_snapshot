from ticker import Ticker
from datetime import datetime
import csv
from os import getenv
from apitools import getDate
import logging



def get_ticker_info(apiKey, ticker):
    log = logging.getLogger(__name__)
    day = getDate()
    log.info(f"{day} - {ticker}")
    
    ticker = Ticker(ticker, day, apiKey)
    data =  ticker.build_dataset()
    
    return data


# write the data to file
def write_to_file(data, file_name):
    csv_columns = data.keys()
    csv_out=csv.DictWriter(open(file_name, 'a'), fieldnames=csv_columns)
    
    csv_out.writerow(data)


#build output file name for the ticker
def build_file_name():

    header = ['tikcer', 'high', 'low', 'last_price']
    file_version = datetime.today().strftime('%d%m%y')
    file_name = f'dataset_{file_version}.csv'

    with open(file_name, 'w', encoding='UTF8') as f:
        writer = csv.writer(f)

        # write the header
        writer.writerow(header)
    
    return file_name


def write_data(data, output_file):

    if data:
        write_to_file(data, file_name= output_file)
    else:
        raise ValueError(f'No record for {day}')



def build_snapshot():

    if(getenv('polygon_api_key')):
        apiKey = getenv('polygon_api_key')

        with open('tickers.csv', newline='') as csvfile:
            csv_reader = csv.DictReader(csvfile, delimiter=',')
            output_file = build_file_name()
            for row in csv_reader:
                ticker =  row['symbol']
                data = get_ticker_info(apiKey, ticker)
                write_data(data, output_file)


    
    else:
        raise EnvironmentError('set the apiKey env variable')



if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log = logging.getLogger(__name__)
    build_snapshot()

   
