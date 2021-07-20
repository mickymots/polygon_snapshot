from multiprocessing import Pool

from requests.adapters import Response
from apitools import *
import pandas as pd
import logging

class Ticker:

    limit = 50000
    log = logging.getLogger(__name__)

    def __init__(self, ticker, day, apiKey):
        self.ticker = ticker
        self.day = day
        self.apiKey = apiKey

    
    def get_current_stats(self):

        response = call_snapshot_api(self.ticker, self.apiKey)
        log.debug(response)
        if 'ticker' in response:
            ticker = response['ticker']
            return {"tikcer": ticker['ticker'], "high": ticker['day']['h'], "low": ticker['day']['l'], "last_price": ticker['lastTrade']['p']}
            
        else:
            return {"ticker": self.ticker, "high": None, "low": None, "last_price": None }



    def build_dataset(self):
        data =  self.get_current_stats()
        return data
