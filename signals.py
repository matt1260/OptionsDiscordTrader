from configparser import ConfigParser
from td.credentials import TdCredentials
from td.client import TdAmeritradeClient
from td.utils.enums import OptionType
from td.utils.enums import ContractType
from td.utils.option_chain import OptionChainQuery
from datetime import datetime
import datetime  # this overrides the previous 'datetime' module
import pytz
import pandas as pd
from csv import writer
import pandas_ta as ta
import time
import os

pd.options.mode.chained_assignment = None  # default='warn'

# How to deploy to heroku
# heroku login
# heroku git:remote -a <app-name>
# % git add .
# % git commit -am "fix"
# % git push heroku master

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
price_history_service = td_client.price_history()


def build_0dte_order(side, symbol):
    # build the order
    today = datetime.date.today()

    # get 1 strike above and 1 below
    options_chain_service = td_client.options_chain()

    option_chain_query = OptionChainQuery(
        symbol=symbol,
        contract_type=ContractType.All,
        from_date=today,
        to_date=today,
        strike_count='2',
        option_type=OptionType.StandardContracts
    )
    chain = options_chain_service.get_option_chain(
        option_chain_query=option_chain_query)
    status = chain['status']
    if status == 'FAILED':
        print('Failed. Chain does not exist.')
        return
    else:
        print('Retrieved current chain.')

    if side == 'long':
        print('Building 0dte long order...')
        call_map = chain['callExpDateMap']
        last_prices = [i['last'] for d in call_map.values()
                       for v in d.values() for i in v]
        call_map = [i['symbol'] for d in call_map.values()
                    for v in d.values() for i in v]
        call = call_map[0]
        mark = last_prices[0]
        print('ITM Call:', call)
        print("Last price:", mark)
        return call, mark
    else:
        print('Building 0dte short order...')
        put_map = chain['putExpDateMap']
        put_map = chain['putExpDateMap']
        last_prices = [i["last"] for d in put_map.values()
                       for v in d.values() for i in v]
        put_map = [i['symbol'] for d in put_map.values()
                   for v in d.values() for i in v]
        put = put_map[-1]
        mark = last_prices[-1]
        print('ITM Put:', put)
        print("Last price:", mark)
        return put, mark


def get_option_mark(contract):
    quote_service = td_client.quotes()

    quote = quote_service.get_quotes(instruments=[contract])

    lastprice = quote[contract]['lastPrice']
    bidprice = quote[contract]['bidPrice']

    print(contract + ' last: ' + str(lastprice) + ', bid: ' + str(bidprice))
    return lastprice, bidprice


def get_signal(symbol):

    import time
    # t = time.time()
    # t_ms = int(t * 1000)
    # t_ms = t_ms + 85000000

    utc_now = datetime.datetime.utcnow()
    eastern_tz = pytz.timezone('US/Eastern')
    eastern_now = utc_now.astimezone(eastern_tz)

    # end_date = datetime.datetime.now()
    # start_date = end_date - datetime.timedelta(days=365 * 10)

    premarket = False

    minute_history = price_history_service.get_price_history(
        symbol, end_date=eastern_now, extended_hours_needed=premarket, period_type='day', period=2, frequency_type='minute', frequency=1)

    # day_history = price_history_service.get_price_history(symbol, start_date=start_date, end_date=end_date, extended_hours_needed=False, period_type='year', period=10, frequency_type='day', frequency=1)

    if minute_history['candles'] == []:  # if no candle data
        minute_history = pd.DataFrame(minute_history)
        print("no candle data")
        return None

    else:
        print('Successfully downloaded ' + symbol + ' data.')
        minute_history = minute_history['candles']
        minute_history = pd.DataFrame(minute_history)
        minute_history['symbol'] = symbol

        df = minute_history

        df['datetime'] = pd.to_datetime(
            df['datetime'], unit='ms') - pd.Timedelta(hours=4)
        df.set_index('datetime', inplace=True)

        df['lowestlow_10'] = df['low'].rolling(10).min()
        open = df['open']  # series
        close = df['close']  # series
        high = df['high']  # series
        low = df['low']  # series

    def bullpivot(close, open, high, lowest10):

        bp = ((close-open) >= 0.38) \
            & (close > close.shift(2)) \
            & (close < open.shift(2)) & (open <= close.shift(1))

        bp2 = ((close - open) >= .20) \
            & (close > close.shift(2)) \
            & (close > high.shift(2)) \
            & (open > open.shift(1)) \
            & (open <= close.shift(1)) \
            & (low.shift(1) <= lowest10)

        bp3 = ((close - open) >= .38) \
            & (close > close.shift(2)) \
            & (close < high.shift(2)) \
            & (open < close.shift(1))

        bp5 = ((close - open) >= .38) \
            & (close > close.shift(2)) \
            & (close > high.shift(2)) \
            & (open < close.shift(1)) \
            & (close >= (high - .02)) \
            & (low.shift(1) <= lowest10)

        avg1 = ta.ema(close, length=5).shift(1)  # ema 5
        avg2 = ta.ema(close, length=10).shift(1)  # ema 10

        bp7 = ((low.shift(1) + 0.40) < avg1.shift(1)) \
            & ((close.shift(1) + 0.40) < avg1.shift(1)) \
            & (avg1.shift(1) < avg2.shift(1)) \
            & (low > low.shift(1)) \
            & (close > open.shift(1)) \
            & ((open.shift(1) - close.shift(1)) > 0.20)

        return bp, bp2, bp3, bp5, bp7

    def bpw(close, high, low, lowest10, bp2, bp5):
        bpw = ((close - open) >= .18) \
            & (close > close.shift(2)) \
            & (high > high.shift(2)) \
            & (close > (high - .03)) \
            & (open > open.shift(1)) \
            & (open <= (high.shift(1) + 0.02)) \
            & ((low.shift(1) <= lowest10)) \
            & (bp2 == False) \
            & (bp5 == False)
        return bpw

    def closelong(bullpivot, bullpivot2, bullpivot3, bullpivot5, bullpivot7, bpw):

        cl1 = bullpivot.shift(10)
        cl2 = bullpivot2.shift(10)
        cl3 = bullpivot3.shift(10)
        cl5 = bullpivot5.shift(10)
        cl7 = bullpivot7.shift(10)
        clw = bpw.shift(10)

        return cl1, cl2, cl3, cl5, cl7, clw

    # populate CSV table with signals

    # Bull Pivot
    lowest10 = df['lowestlow_10']
    bp, bp2, bp3, bp5, bp7 = bullpivot(close, open, high, lowest10)
    bpw = bpw(close, high, low, lowest10, bp2, bp5)

    df['bull_pivot'] = bp
    df['bull_pivot2'] = bp2
    df['bull_pivot3'] = bp3
    df['bull_pivot5'] = bp5
    df['bull_pivot7'] = bp7
    df['bull_pivotw'] = bpw

    # close long
    cl1, cl2, cl3, cl5, cl7, clw = closelong(bp, bp2, bp3, bp5, bp7, bpw)

    df['close_bullpivot'] = cl1
    df['close_bullpivot2'] = cl2
    df['close_bullpivot3'] = cl3
    df['close_bullpivot5'] = cl5
    df['close_bullpivot7'] = cl7
    df['close_bullpivotw'] = clw

    print('Generated signals for ' + symbol + '.')
    df.to_csv(symbol + '_output.csv', index=True)


def get_spx_signal():

    utc_now = datetime.datetime.utcnow()
    eastern_tz = pytz.timezone('US/Eastern')
    eastern_now = utc_now.astimezone(eastern_tz)

    history = price_history_service.get_price_history(
        '$SPX.X', end_date=eastern_now, extended_hours_needed=False,
        period_type='day', period=2, frequency_type='minute', frequency=1
    )

    df = pd.json_normalize(history['candles'])
    df = df.assign(datetime=pd.to_datetime(
        df['datetime'], unit='ms') - pd.Timedelta(hours=4))
    df.set_index('datetime', inplace=True)
    df['symbol'] = 'SPX'

    print('Successfully downloaded SPX data.')

    lookback = 30

    df['sma200'] = df['close'].rolling(window=200).mean()
    df['sma10'] = df['close'].rolling(window=10).mean()

    bullpivot = (
        (df['close'] < df['sma200']) &
        (df['close'] < df['sma10']) &
        ((df['close'] - df['open']) >= 0.38) &
        (df['close'] > df['close'].shift(2)) &
        (df['close'] < df['open'].shift(2)) &
        (df['open'] < df['close'].shift(1)) &
        (df['open'] < df['open'].shift(lookback))
    )

    bullpivot7 = (
        (df['close'] > df['open']) &
        ((df['close'] - df['open']) >= 0.95 * (df['high'] - df['low'])) &
        (df['close'] > df['close'].shift(2)) &
        (df['close'] > df['open'].shift(2)) &
        (df['close'] > df['close'].shift(6)) &
        (df['open'] < df['open'].shift(6)) &
        (df['open'] < df['open'].shift(lookback))
    )

    bullpivot4 = (
        (df['close'] < df['sma200']) &
        ((df['close'] - df['open']) >= 0.38) &
        (df['close'] > df['close'].shift(2)) &
        (df['close'] > df['open'].shift(2)) &
        (df['open'] < df['close'].shift(1)) &
        (df['open'].shift(1) <= df['close'].shift(2)) &
        (df['open'].shift(2) > df['close'].shift(1)) &
        (df['open'].shift(2) > df['open'].shift(1)) &
        (df['open'] < df['open'].shift(lookback))
    )

    df['bullpivot'] = bullpivot
    df['bullpivot4'] = bullpivot4
    df['bullpivot7'] = bullpivot7

    # Define the doublepivot condition
    df['doublepivot'] = bullpivot7 & (bullpivot.rolling(window=14).sum() > 0)

    # Define the LowerBand
    df['lowestlow'] = df['low'].rolling(window=120).min()
    df['LowerBand'] = df['lowestlow'].shift(1)

    # Define the base and OnBand conditions
    df['base'] = (df['LowerBand'].shift(10) <= df['LowerBand'] * 1.001)
    df['OnBand'] = (df['low'] <= df['LowerBand'] * 1.001)

    # Define the bullPivotNearLowerBand condition
    df['bullPivotNearLowerBand'] = (df['lowestlow'].shift(1).rolling(window=10).min(
    ) <= df['LowerBand'] * 1.002) & (df['low'] <= df['LowerBand'] * 1.002)

    # Define the superbullpivot signal
    df['superbullpivot'] = df['doublepivot'] & df['bullPivotNearLowerBand'] & df['base']

    df['close_bullpivot'] = df['bullpivot'].shift(10)
    df['close_bullpivot4'] = df['bullpivot4'].shift(10)
    df['close_bullpivot7'] = df['bullpivot7'].shift(10)
    df['close_bullpivot_s'] = df['superbullpivot'].shift(10)
    df['close_bullpivot_d'] = df['doublepivot'].shift(10)

    print('Generated signals for SPX.')
    df.to_csv('SPX_output.csv', index=True)


def process_signal(symbol):

    # get latest minute data
    # df = pd.read_csv('TEST_output.csv')
    filename = symbol + '_output.csv'
    max_attempts = 5
    pause_seconds = 1
    # loop to attempt loading the file multiple times
    for i in range(max_attempts):
        try:
            # try to load the file
            df = pd.read_csv(filename)
            break  # exit the loop if successful
        except pd.errors.EmptyDataError:
            # handle the error and pause before trying again
            print('Error: No data found in file. Retrying...')
            time.sleep(pause_seconds)

    if 'df' not in locals():
        print('Error: Unable to load data after {} attempts'.format(max_attempts))
        return

    else:
        print('Retrieved ' + symbol + ' CSV output.')

    # last line in csv file changes several times per minute, so get second to last line
    bull_pivot, bull_pivot2, bull_pivot3, bull_pivot5, bull_pivot7, bull_pivotw = [
        df.iloc[-2][f'bull_pivot{i}'] for i in ['', '2', '3', '5', '7', 'w']]

    close = df.iloc[-2]['close']
    previous_low = df.iloc[-3]['low']
    time = str(df.iloc[-2]['datetime'])
    symbol = df.iloc[-1]['symbol']

    print(time, symbol + ' last close: ' + str(close))

    if previous_low > 0:

        # get pivot alert
        if any(val == True for val in [bull_pivot, bull_pivot2, bull_pivot3, bull_pivot5, bull_pivot7]):
            pivot_alert = symbol + " Bull Pivot Alert: $" + str(previous_low)
            if bull_pivot:
                type = 'Bull Pivot 1'
            elif bull_pivot2:
                type = 'Bull Pivot 2'
            elif bull_pivot3:
                type = 'Bull Pivot 3'
            elif bull_pivot5:
                type = 'Bull Pivot 5'
            elif bull_pivot7:
                type = 'Bull Pivot 7'

            print(time + ': ' + pivot_alert + ', (' + type + ')')

        elif bull_pivotw:
            pivot_alert = symbol + \
                " Weak Bull Pivot Alert: $" + str(previous_low)
            print(pivot_alert)
            type = 'Bull Pivot Weak'
        else:
            print(time + ": " + "No Pivot Alert")
            pivot_alert = None
            type = None

        return pivot_alert, type

    else:
        print("No Data, PreMarket time")
        return None, None, None, None


def process_spx_signal():

    df = pd.read_csv('SPX_output.csv')

    # last line in csv file changes several times per minute, so get second to last line
    bullpivot = df.iloc[-2]['bullpivot']
    bullpivot4 = df.iloc[-2]['bullpivot4']
    bullpivot7 = df.iloc[-2]['bullpivot7']
    doublepivot = df.iloc[-2]['doublepivot']
    superbullpivot = df.iloc[-2]['superbullpivot']

    close = df.iloc[-2]['close']
    previous_low = df.iloc[-3]['low']
    time = str(df.iloc[-2]['datetime'])
    symbol = df.iloc[-1]['symbol']

    print(time, symbol + ' last close: ' + str(close))

    if previous_low > 0:

        # get pivot alert
        if any(val == True for val in [bullpivot, bullpivot4, bullpivot7]):
            pivot_alert = symbol + " Bull Pivot Alert: $" + str(previous_low)
            if bullpivot:
                type = 'Bull Pivot 1'
            elif bullpivot4:
                type = 'Bull Pivot 4'
            elif bullpivot7:
                type = 'Bull Pivot 7'

            print(time + ': ' + pivot_alert + ', (' + type + ')')

        elif superbullpivot:
            pivot_alert = symbol + \
                " Super Bull Pivot Alert: $" + str(previous_low)
            print(pivot_alert)
            type = 'Super Bull Pivot'

        elif doublepivot:
            pivot_alert = symbol + \
                " Double Bull Pivot Alert: $" + str(previous_low)
            print(pivot_alert)
            type = 'Double Bull Pivot'

        else:
            print(time + ": " + "No SPX Pivot Alert")
            pivot_alert = None
            type = None

        return pivot_alert, type

    else:
        print("No Data, PreMarket time")
        return None, None, None, None


def save_history(symbol):

    # get historical 1-minute prices
    t = time.time()
    t_ms = int(t * 1000)
    t_ms = t_ms + 85000000

    print('Archiving intraday data from TD Ameritrade...')
    price_history_service = td_client.price_history()
    minute_history = price_history_service.get_price_history(
        symbol, end_date=t_ms, extended_hours_needed=False, period_type='day', period=10, frequency_type='minute', frequency=1)

    if minute_history['candles'] == []:  # if no candle data
        minute_history = pd.DataFrame(minute_history)
        print("no candle data")

    else:

        minute_history = minute_history['candles']
        minute_history = pd.DataFrame(minute_history)
        minute_history['symbol'] = symbol
        minute_history.insert(0, 'symbol', minute_history.pop('symbol'))

        # drop last row if zero (pre market)
        if minute_history.iloc[-1]['open'] == 0:
            minute_history = minute_history.drop(minute_history.index[-1])

        minute_history['datetime'] = pd.to_datetime(
            minute_history['datetime'], unit='ms')
        minute_history.set_index('datetime', inplace=True)

        file = '../data/' + symbol + '_1m_history.csv'
        if os.path.exists(file):
            print('File exists, adding new data.')
            original = pd.read_csv(file, index_col='datetime')
            merged = pd.concat([original, minute_history]
                               ).drop_duplicates(keep='last')
            merged.to_csv(file, index=True)
        else:
            print('File does not exist, creating new one.')
            minute_history.to_csv(file, index=True)

        print('Successfully appended ' + symbol + ' intraday data.')
