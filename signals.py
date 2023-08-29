from configparser import ConfigParser
from td.credentials import TdCredentials
from td.client import TdAmeritradeClient
from td.utils.enums import OptionaRange
from td.utils.enums import OptionType
from td.utils.enums import ContractType
from td.utils.enums import ExpirationMonth
from td.utils.option_chain import OptionChainQuery
from td.utils.enums import OrderStatus
from td.utils.enums import Markets
from datetime import datetime
import numpy as np

import pandas as pd

import logging
log = logging.getLogger(__name__)

config = ConfigParser()
config.read('config/config.ini')
client_id = config.get('main', 'client_id')
redirect_uri = config.get('main', 'redirect_uri')
account_no = config.get('main', 'account_number')

td_credentials = TdCredentials(
    client_id=client_id,
    redirect_uri=redirect_uri,
    credential_file='config/td_credentials.json'
)

td_client = TdAmeritradeClient(
    credentials=td_credentials
) 

# Market Buy/Sell Signals

# Get 4 bars of data.

def get_signal(symbol):

    price_history_service = td_client.price_history()

    history = price_history_service.get_price_history(symbol, extended_hours_needed=False, period_type='day', period=1, frequency_type='minute', frequency=1)
    if history['candles'] == []:
        history = pd.DataFrame(history)
        return history
    else:
        history = history['candles']
        history = pd.DataFrame(history)
        history['datetime'] = pd.to_datetime(history['datetime'],unit='ms')
        history = history.tail(12)

    if history.empty:
        return "empty"
    else:   
        df = history[['close', 'open', 'high', 'low']]
        open = df['open'] #series
        close = df['close'] #series
        high = df['high'] #series
        low = df['low'] #series
        
    def bullpivot(close, open):

        bullpivot = ((close-open) >= 0.38) & (close > close.shift(2)) \
        & (close < open.shift(2)) & (open <= close.shift(1))
        
        return bullpivot

    def bullpivot3(close, open, high):
        bullpivot3 = ((close - open) >= .38) \
            & (close > close.shift(2)) \
                & (close < high.shift(2)) \
                    & (open < close.shift(1))
        return bullpivot3
    
    def closelong(bullpivot, bullpivot3):
    
        cl1 = bullpivot.shift(10)
        cl3 = bullpivot3.shift(10)
        return cl1, cl3

    # Bull Pivot
    bb1 = bullpivot(close, open)
    bb3 = bullpivot3(close, open, high)
    df['bull_pivot'] = bb1
    df['bull_pivot3'] = bb3

    # close long
    cl1, cl3 = closelong(bb1, bb3)
    df['close_bullpivot'] = cl1
    df['close_bullpivot3'] = cl3

    return df
  