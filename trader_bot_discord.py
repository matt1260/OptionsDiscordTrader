import os
from csv import writer
import requests
import discord
import pytz
from discord.ext import tasks, commands
from dotenv import load_dotenv
import datetime
from datetime import datetime, timedelta
import time
from datetime import date
import pandas as pd
import mplfinance as mpf
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import io
from discord import File
from signals import *
from configparser import ConfigParser
import threading
import asyncio

# deploying to heroku
# heroku login
# heroku git:remote -a techtrade-discord-bot
# % git add .
# % git commit -am "fix"
# % git push heroku master


vixdata = pd.read_csv(r'spy_vix.csv', index_col=['date'])
vixdata['vix_open'] = vixdata['vix_open'].round(decimals=0)

###### TD API key must match the TD credentials token used ########
###### Must manually go through OAuth process every three months #########

config = ConfigParser()
config.read('config/config.ini')
client_id = config.get('main', 'client_id')
redirect_uri = config.get('main', 'redirect_uri')
account_no = config.get('main', 'account_number')


load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')
GUILD = os.getenv('DISCORD_GUILD')
API_KEY = os.getenv('API_KEY')

# Initialize Bot Client and Denote The Command Prefix
intents = discord.Intents().all()
bot = commands.Bot(command_prefix="!", intents=intents)

def is_market_open():
    url = f'https://api.tdameritrade.com/v1/marketdata/EQUITY/hours'
    params = {'apikey': API_KEY}
    response = requests.get(url, params=params)
    data = response.json()

    if 'equity' not in data or 'EQ' not in data['equity']:
        print("Error: Unable to get market hours from the TD Ameritrade API.")
        return False

    market_hours = data['equity']['EQ']['sessionHours']['regularMarket']
    
    ny_timezone = pytz.timezone('America/New_York')
    now = datetime.datetime.now(ny_timezone)
    
    for session in market_hours:
        start_time = datetime.datetime.fromisoformat(session['start']).astimezone(ny_timezone)
        end_time = datetime.datetime.fromisoformat(session['end']).astimezone(ny_timezone)
        
        if start_time <= now <= end_time:
            return True
    
    return False

def create_pivot_chart(df):

    # Filter the last 20 minutes of data
    df['datetime'] = pd.to_datetime(df['datetime'])
    last_20_min = df.tail(21)
    
    # Set the index to datetime and rename the columns to match mplfinance requirements
    last_20_min.set_index('datetime', inplace=True)
    last_20_min.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close'}, inplace=True)

    # Create the OHLC candlestick chart
    fig, axes = mpf.plot(last_20_min, type='candle', style='charles', returnfig=True)

    # Add a solid green horizontal line at the low value of the second-to-last row
    low_price = last_20_min['Low'].iloc[-2]
    line_label = str(low_price) + ' Bull Pivot'
    axes[0].axhline(y=low_price, color='green', linewidth=2)

    # Add the price value to the right axis
    axes[0].annotate(f'{line_label}', xy=(0, (low_price+ 0.04)), xycoords=('axes fraction', 'data'), fontsize=12, color='black', ha='left', va='center',
                     bbox=dict(facecolor='green', edgecolor='white', alpha=0.5))

    # Remove the 'Price' label from the right
    axes[0].yaxis.set_label_coords(1.05, 0.5)
    axes[0].set_ylabel('', fontsize=12)

    # Save the chart to a buffer
    chart_buffer = io.BytesIO()
    plt.savefig(chart_buffer, format='png', bbox_inches='tight')
    chart_buffer.seek(0)

    return chart_buffer

def create_bear_pivot_chart(df):

    # Filter the last 20 minutes of data
    df['datetime'] = pd.to_datetime(df['datetime'])
    last_20_min = df.tail(21)
    
    # Set the index to datetime and rename the columns to match mplfinance requirements
    last_20_min.set_index('datetime', inplace=True)
    last_20_min.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close'}, inplace=True)

    # Create the OHLC candlestick chart
    fig, axes = mpf.plot(last_20_min, type='candle', style='charles', returnfig=True)

    # Add a solid green horizontal line at the low value of the second-to-last row
    low_price = last_20_min['High'].iloc[-2]
    line_label = str(low_price) + ' Bear Pivot'
    axes[0].axhline(y=low_price, color='red', linewidth=2)

    # Add the price value to the right axis
    axes[0].annotate(f'{line_label}', xy=(0, (low_price+ 0.04)), xycoords=('axes fraction', 'data'), fontsize=12, color='black', ha='left', va='center',
                     bbox=dict(facecolor='red', edgecolor='white', alpha=0.5))

    # Remove the 'Price' label from the right
    axes[0].yaxis.set_label_coords(1.05, 0.5)
    axes[0].set_ylabel('', fontsize=12)

    # Save the chart to a buffer
    chart_buffer = io.BytesIO()
    plt.savefig(chart_buffer, format='png', bbox_inches='tight')
    chart_buffer.seek(0)

    return chart_buffer

# Run as thread below
def run_signals():
    
    while True:
        if is_market_open():
            print('Market open, running signal generation.')
            get_signal('QQQ')
            get_signal('SPY')
            get_spx_signal()
            
        else:
            print("The regular market is closed.")
        print('Waiting to run signals...')
        time.sleep(20)    


@tasks.loop(seconds=45, reconnect=True)
async def alerts():
    await bot.wait_until_ready()
    test_channel = bot.get_channel(int(973516018523701248)) #bot-test channel
    spy_channel = bot.get_channel(int(1069957799590764554))
    qqq_channel = bot.get_channel(int(1087854852924776519))
    spx_channel = bot.get_channel(int(1070286165082648667))

    tz_NY = pytz.timezone('America/New_York')
    now = datetime.datetime.now(tz_NY)
    time = now.strftime("%B %d, %I:%M %p")
    #marketopen = True
    if is_market_open():
    #if marketopen == True:        
        print ('processing alerts....')
        spy_pivot_alert, spy_type = process_signal('SPY')
        qqq_pivot_alert, qqq_type = process_signal('QQQ')
        spx_pivot_alert, spx_type = process_spx_signal()

        if spy_pivot_alert is not None:

            df = pd.read_csv('SPY_alert_log.csv')
            last_alert = df['alert_name'].iloc[-1]
            last_alert2 = df['alert_name'].iloc[-2]
            last_alert3 = df['alert_name'].iloc[-3]

            if spy_pivot_alert in [last_alert, last_alert2, last_alert3]:
                
                print('Already alerted on SPY!')

            else:
                symbol = 'SPY'
                ITM_contract, option_entry_price = build_0dte_order('long', symbol)

                df = pd.read_csv('SPY_output.csv')
                low = df.iloc[-2]['low']
                entry_price = df.iloc[-1]['open']
                time = str(df.iloc[-1]['datetime'])
                

                retStr = str("""```diff\n+ """ + spy_pivot_alert + ' Entry Price: ' + str(entry_price) + ', ' + ITM_contract + ' @ ' + str(option_entry_price) + """```""")
                embed = discord.Embed(title="\U0001F6A8 Alert")
                embed.add_field(name=time, value=retStr, inline=False)

                chart_buffer = create_pivot_chart(df)

                # Attach the chart to the Discord message
                chart_file = File(chart_buffer, filename='alert_signal.png')
                embed.set_image(url='attachment://alert_signal.png')
                await spy_channel.send(embed=embed, file=chart_file)

                record = [time, symbol, low, spy_pivot_alert, spy_type, entry_price, ITM_contract, option_entry_price]

                with open("spy_alert_log.csv", "a", newline='') as log:
                    csv_output = writer(log)
                    csv_output.writerow(record)
                
                print("Waiting 10 minutes...")
                await asyncio.sleep(600)
                option_last, option_bid = get_option_mark(ITM_contract)
                option_pnl = round(option_last - option_entry_price, 2)
                entry_time = time
                df = pd.read_csv('SPY_output.csv')
                exit = df.iloc[-1]['high']
                pnl = round(exit - entry_price, 2)

                if pnl < 0:
                    close_alert = symbol + " Bull Pivot loss (10m) since last alert at " + entry_time + ": $" + str(pnl) + ' - ' + ITM_contract + ': $' + str(option_pnl)
                    color = 'red'

                    # check if it is already recorded
                    df = pd.read_csv('pnl_log.csv')
                    last_alert = df['alert'].iloc[-1]
                    last_alert2 = df['alert'].iloc[-2]
                    last_alert3 = df['alert'].iloc[-3]

                    tz_NY = pytz.timezone('America/New_York')
                    now = datetime.datetime.now(tz_NY)
                    time = now.strftime("%B %d, %I:%M %p")
                    
                    record = [time, close_alert, pnl, option_pnl]

                    # only record pnl once
                    if close_alert in [last_alert, last_alert2, last_alert3]:
                        
                        print('Already recorded close alert...')
                        close_alert = None

                    else:
                        with open("pnl_log.csv", "a", newline='') as log:
                            csv_output = writer(log)
                            csv_output.writerow(record)

        
                else:
                    close_alert = symbol + " Bull Pivot gain (10m) since last alert at " + entry_time + ": $" + str(pnl) + ' - ' + ITM_contract + ': $' + str(option_pnl)
                    color = 'green'

                    df = pd.read_csv('pnl_log.csv')
                    last_alert = df['alert'].iloc[-1]
                    last_alert2 = df['alert'].iloc[-2]
                    last_alert3 = df['alert'].iloc[-3]
                    
                    tz_NY = pytz.timezone('America/New_York')
                    now = datetime.datetime.now(tz_NY)
                    time = now.strftime("%B %d, %I:%M %p")

                    record = [time, close_alert, pnl, option_pnl]

                    # only record pnl once
                    if close_alert in [last_alert, last_alert2, last_alert3]:
                        print('Already recorded close alert...')
                        close_alert = None

                    else:
                        with open("pnl_log.csv", "a", newline='') as log:
                            csv_output = writer(log)
                            csv_output.writerow(record)
                            
                print(close_alert)
                retStr = str("""``` """ + close_alert + """```""")
                embed = discord.Embed(title="\U0001F3E6 PnL")
                embed.add_field(name='10 minute P/L for last SPY alert',value=retStr, inline=False)
                if color == 'green':
                    embed.color = 0x00FF00
                elif color == 'red':
                    embed.color = 0xFF0000
                else:
                    embed.color = 0x000000
                await spy_channel.send(embed=embed)
            

            df = pd.read_csv('qqq_alert_log.csv')
            last_alert = df['alert_name'].iloc[-1]
            last_alert2 = df['alert_name'].iloc[-2]
            last_alert3 = df['alert_name'].iloc[-3]

            if qqq_pivot_alert in [last_alert, last_alert2, last_alert3]:
                
                print('Already alerted on QQQ!')

            else:
                symbol = 'QQQ'
                ITM_contract, option_entry_price = build_0dte_order('long', symbol)

                df = pd.read_csv('QQQ_output.csv')
                low = df.iloc[-2]['low']
                entry_price = df.iloc[-1]['open']
                time = str(df.iloc[-1]['datetime'])
                
                retStr = str("""```diff\n+ """ + qqq_pivot_alert + ' Entry Price: ' + str(entry_price) + ', ' + ITM_contract + ' @ ' + str(option_entry_price) + """```""")
                embed = discord.Embed(title="\U0001F6A8 Alert")
                embed.add_field(name=time,value=retStr, inline=False)
                
                chart_buffer = create_pivot_chart(df)

                # Attach the chart to the Discord message
                chart_file = File(chart_buffer, filename='alert_signal.png')
                embed.set_image(url='attachment://alert_signal.png')

                await qqq_channel.send(embed=embed, file=chart_file)

                record = [time, symbol, low, qqq_pivot_alert, qqq_type, entry_price, ITM_contract, option_entry_price]

                with open("qqq_alert_log.csv", "a", newline='') as log:
                    csv_output = writer(log)
                    csv_output.writerow(record)

        if qqq_pivot_alert is not None:

            df = pd.read_csv('QQQ_alert_log.csv')
            last_alert = df['alert_name'].iloc[-1]
            last_alert2 = df['alert_name'].iloc[-2]
            last_alert3 = df['alert_name'].iloc[-3]

            if qqq_pivot_alert in [last_alert, last_alert2, last_alert3]:
                
                print('Already alerted on QQQ!')

            else:
                symbol = 'QQQ'
                ITM_contract, option_entry_price = build_0dte_order('long', symbol)

                df = pd.read_csv('QQQ_output.csv')
                low = df.iloc[-2]['low']
                entry_price = df.iloc[-1]['open']
                time = str(df.iloc[-1]['datetime'])
                

                retStr = str("""```diff\n+ """ + qqq_pivot_alert + ' Entry Price: ' + str(entry_price) + ', ' + ITM_contract + ' @ ' + str(option_entry_price) + """```""")
                embed = discord.Embed(title="\U0001F6A8 Alert")
                embed.add_field(name=time, value=retStr, inline=False)

                chart_buffer = create_pivot_chart(df)

                # Attach the chart to the Discord message
                chart_file = File(chart_buffer, filename='alert_signal.png')
                embed.set_image(url='attachment://alert_signal.png')
                await qqq_channel.send(embed=embed, file=chart_file)

                record = [time, symbol, low, qqq_pivot_alert, qqq_type, entry_price, ITM_contract, option_entry_price]

                with open("QQQ_alert_log.csv", "a", newline='') as log:
                    csv_output = writer(log)
                    csv_output.writerow(record)
                
                print("Waiting 10 minutes...")
                await asyncio.sleep(600)
                option_last, option_bid = get_option_mark(ITM_contract)
                option_pnl = round(option_last - option_entry_price, 2)
                entry_time = time
                df = pd.read_csv('QQQ_output.csv')
                exit = df.iloc[-1]['high']
                pnl = round(exit - entry_price, 2)

                if pnl < 0:
                    close_alert = symbol + " Bull Pivot loss (10m) since last alert at " + entry_time + ": $" + str(pnl) + ' - ' + ITM_contract + ': $' + str(option_pnl)
                    color = 'red'

                    # check if it is already recorded
                    df = pd.read_csv('pnl_log.csv')
                    last_alert = df['alert'].iloc[-1]
                    last_alert2 = df['alert'].iloc[-2]
                    last_alert3 = df['alert'].iloc[-3]

                    tz_NY = pytz.timezone('America/New_York')
                    now = datetime.datetime.now(tz_NY)
                    time = now.strftime("%B %d, %I:%M %p")
                    
                    record = [time, close_alert, pnl, option_pnl]

                    # only record pnl once
                    if close_alert in [last_alert, last_alert2, last_alert3]:
                        
                        print('Already recorded close alert...')
                        close_alert = None

                    else:
                        with open("pnl_log.csv", "a", newline='') as log:
                            csv_output = writer(log)
                            csv_output.writerow(record)

        
                else:
                    close_alert = symbol + " Bull Pivot gain (10m) since last alert at " + entry_time + ": $" + str(pnl) + ' - ' + ITM_contract + ': $' + str(option_pnl)
                    color = 'green'

                    df = pd.read_csv('pnl_log.csv')
                    last_alert = df['alert'].iloc[-1]
                    last_alert2 = df['alert'].iloc[-2]
                    last_alert3 = df['alert'].iloc[-3]
                    
                    tz_NY = pytz.timezone('America/New_York')
                    now = datetime.datetime.now(tz_NY)
                    time = now.strftime("%B %d, %I:%M %p")

                    record = [time, close_alert, pnl, option_pnl]

                    # only record pnl once
                    if close_alert in [last_alert, last_alert2, last_alert3]:
                        print('Already recorded close alert...')
                        close_alert = None

                    else:
                        with open("pnl_log.csv", "a", newline='') as log:
                            csv_output = writer(log)
                            csv_output.writerow(record)
                            
                print(close_alert)
                retStr = str("""``` """ + close_alert + """```""")
                embed = discord.Embed(title="\U0001F3E6 PnL")
                embed.add_field(name='10 minute P/L for last QQQ alert',value=retStr, inline=False)
                if color == 'green':
                    embed.color = 0x00FF00
                elif color == 'red':
                    embed.color = 0xFF0000
                else:
                    embed.color = 0x000000
                await qqq_channel.send(embed=embed)   

        if spx_pivot_alert is not None:

            df = pd.read_csv('spx_alert_log.csv')
            last_alert = df['alert_name'].iloc[-1]
            last_alert2 = df['alert_name'].iloc[-2]
            last_alert3 = df['alert_name'].iloc[-3]

            if spx_pivot_alert in [last_alert, last_alert2, last_alert3]:
                
                print('Already alerted on SPX!')

            else:
                symbol = '$SPX.X'
                ITM_contract, option_entry_price = build_0dte_order('long', symbol)

                df = pd.read_csv('SPX_output.csv')
                low = df.iloc[-2]['low']
                entry_price = df.iloc[-1]['open']
                time = str(df.iloc[-1]['datetime'])
                symbol = 'SPX'

                retStr = str("""```diff\n+ """ + spx_pivot_alert + ' Entry Price: ' + str(entry_price) + ', ' + ITM_contract + ' @ ' + str(option_entry_price) + """```""")
                embed = discord.Embed(title="\U0001F6A8 Alert")
                embed.add_field(name=time,value=retStr, inline=False)
                
                chart_buffer = create_pivot_chart(df)

                # Attach the chart to the Discord message
                chart_file = File(chart_buffer, filename='alert_signal.png')
                embed.set_image(url='attachment://alert_signal.png')

                await spx_channel.send(embed=embed, file=chart_file)

                record = [time, symbol, low, spx_pivot_alert, spx_type, entry_price, ITM_contract, option_entry_price]

                with open("spx_alert_log.csv", "a", newline='') as log:
                    csv_output = writer(log)
                    csv_output.writerow(record)
        
                print("Waiting 10 minutes...")
                await asyncio.sleep(600)
                option_last, option_bid = get_option_mark(ITM_contract)
                option_pnl = round(option_last - option_entry_price, 2)
                entry_time = time
                df = pd.read_csv('SPX_output.csv')
                exit = df.iloc[-1]['high']
                pnl = round(exit - entry_price, 2)

                if pnl < 0:
                    close_alert = symbol + " Bull Pivot loss (10m) since last alert at " + entry_time + ": $" + str(pnl) + ' - ' + ITM_contract + ': $' + str(option_pnl)
                    color = 'red'

                    # check if it is already recorded
                    df = pd.read_csv('pnl_log.csv')
                    last_alert = df['alert'].iloc[-1]
                    last_alert2 = df['alert'].iloc[-2]
                    last_alert3 = df['alert'].iloc[-3]

                    tz_NY = pytz.timezone('America/New_York')
                    now = datetime.datetime.now(tz_NY)
                    time = now.strftime("%B %d, %I:%M %p")
                    
                    record = [time, close_alert, pnl, option_pnl]

                    # only record pnl once
                    if close_alert in [last_alert, last_alert2, last_alert3]:
                        
                        print('Already recorded close alert...')
                        close_alert = None

                    else:
                        with open("pnl_log.csv", "a", newline='') as log:
                            csv_output = writer(log)
                            csv_output.writerow(record)

        
                else:
                    close_alert = symbol + " Bull Pivot gain (10m) since last alert at " + entry_time + ": $" + str(pnl) + ' - ' + ITM_contract + ': $' + str(option_pnl)
                    color = 'green'

                    df = pd.read_csv('pnl_log.csv')
                    last_alert = df['alert'].iloc[-1]
                    last_alert2 = df['alert'].iloc[-2]
                    last_alert3 = df['alert'].iloc[-3]
                    
                    tz_NY = pytz.timezone('America/New_York')
                    now = datetime.datetime.now(tz_NY)
                    time = now.strftime("%B %d, %I:%M %p")

                    record = [time, close_alert, pnl, option_pnl]

                    # only record pnl once
                    if close_alert in [last_alert, last_alert2, last_alert3]:
                        print('Already recorded close alert...')
                        close_alert = None

                    else:
                        with open("pnl_log.csv", "a", newline='') as log:
                            csv_output = writer(log)
                            csv_output.writerow(record)
                            
                print(close_alert)
                retStr = str("""``` """ + close_alert + """```""")
                embed = discord.Embed(title="\U0001F3E6 PnL")
                embed.add_field(name='10 minute P/L for last SPX alert',value=retStr, inline=False)
                if color == 'green':
                    embed.color = 0x00FF00
                elif color == 'red':
                    embed.color = 0xFF0000
                else:
                    embed.color = 0x000000
                await spx_channel.send(embed=embed)   
    else:
        print("The regular market is closed.")


@bot.event
async def on_ready():
    for guild in bot.guilds:
        if guild.name == GUILD:
            break

    print(
        f'{bot.user} is connected to the following guild:\n'
        f'{guild.name}(id: {guild.id})'
    )
    if not alerts.is_running():
        alerts.start()


# listen to all the messages
@bot.event
async def on_message(message):
    # Make sure the Bot doesn't respond to it's own messages
    if message.author == bot.user: 
        return
    if message.content == 'dsab1230bxh71nx':
        await message.channel.send('Current SPY trading range is ')

    await bot.process_commands(message)

@bot.command()
async def alert(ctx, arg):
    
    test_channel = bot.get_channel(int(973516018523701248)) #bot-test channel
    spy_channel = bot.get_channel(int(1069957799590764554))
    qqq_channel = bot.get_channel(int(1087854852924776519))
    spx_channel = bot.get_channel(int(1070286165082648667))

    print(arg)

    if arg == 'spxcall':
        
        symbol = '$SPX.X'
        ITM_contract, option_entry_price = build_0dte_order('long', symbol)

        df = pd.read_csv('SPX_output.csv')
        low1 = df.iloc[-1]['low']
        low2 = df.iloc[-2]['low']
        low3 = df.iloc[-3]['low']
        entry_price = df.iloc[-1]['open']
        entry_time = str(df.iloc[-1]['datetime'])
        pivot_low = min(low1,low2,low3)

        symbol = 'SPX'
        spx_pivot_alert = symbol + " Bull Pivot Alert: $" + str(pivot_low)
        retStr = str("""```diff\n+ """ + spx_pivot_alert + ' Entry Price: ' + str(entry_price) + ', ' + ITM_contract + ' @ ' + str(option_entry_price) + """```""")
        embed = discord.Embed(title="\U0001F6A8 Alert")
        embed.add_field(name=entry_time,value=retStr, inline=False)
        
        chart_buffer = create_pivot_chart(df)

        # Attach the chart to the Discord message
        chart_file = File(chart_buffer, filename='alert_signal.png')
        embed.set_image(url='attachment://alert_signal.png')

        await spx_channel.send(embed=embed, file=chart_file)

        spx_type = 'Manual Bull Pivot'
        record = [entry_time, symbol, pivot_low, spx_pivot_alert, spx_type, entry_price, ITM_contract, option_entry_price]

        with open("spx_alert_log.csv", "a", newline='') as log:
            csv_output = writer(log)
            csv_output.writerow(record)

        print("Waiting 10 minutes...")
        await asyncio.sleep(600)

        df = pd.read_csv('SPX_output.csv')
        exit = df.iloc[-1]['high']
        option_last, option_bid = get_option_mark(ITM_contract)
        pnl = round(exit - entry_price, 2)
        option_pnl = round(option_last - option_entry_price)

        if pnl < 0:
            close_alert = symbol + " Bull Pivot loss (10m) since last alert at " + entry_time + ": $" + str(pnl) + ', ' + ITM_contract + ': $' + str(option_pnl)

            tz_NY = pytz.timezone('America/New_York')
            now = datetime.datetime.now(tz_NY)
            exit_time = now.strftime("%B %d, %I:%M %p")
            
            record = [exit_time, close_alert, pnl, option_pnl]

            with open("pnl_log.csv", "a", newline='') as log:
                csv_output = writer(log)
                csv_output.writerow(record)
            
            retStr = str("""``` """ + close_alert + """```""")
            embed = discord.Embed(title="\U0001F3E6 PnL")
            embed.add_field(name='10 minute P/L for last SPX alert',value=retStr, inline=False)
            embed.color = 0xFF0000
            await spx_channel.send(embed=embed)   


        elif pnl < 2:
            close_alert = symbol + " Bull Pivot gain (10m) since last alert at " + entry_time + ": $" + str(pnl) + ', ' + ITM_contract + ': $' + str(option_pnl)
            color = 'green'
            
            tz_NY = pytz.timezone('America/New_York')
            now = datetime.datetime.now(tz_NY)
            exit_time = now.strftime("%B %d, %I:%M %p")

            record = [exit_time, close_alert, pnl, option_pnl]

            with open("pnl_log.csv", "a", newline='') as log:
                csv_output = writer(log)
                csv_output.writerow(record)

            retStr = str("""``` """ + close_alert + """```""")
            embed = discord.Embed(title="\U0001F3E6 PnL")
            embed.add_field(name='10 minute P/L for last SPX alert',value=retStr, inline=False)
            embed.color = 0x00FF00 
            await spx_channel.send(embed=embed)

        else: # we have a runner

            # set stop loss at breakeven
            await spx_channel.send("Setting stoploss at breakeven...")

            # check price every minute
            while True:
                await asyncio.sleep(60)
                df = pd.read_csv('SPX_output.csv')
                last = df.iloc[-1]['open']
                option_last, option_bid = get_option_mark(ITM_contract)
                pnl = round(exit - entry_price, 2)
                option_pnl = round(option_last - option_entry_price)
                
                if last == (entry_price + 1.00):

                    close_alert = "Stopped out. " + symbol + " Bull Pivot gain (10m) since last alert at " + entry_time + ": $" + str(pnl) + ', ' + ITM_contract + ': $' + str(option_pnl)
                    color = 'green'
                    
                    tz_NY = pytz.timezone('America/New_York')
                    now = datetime.datetime.now(tz_NY)
                    exit_time = now.strftime("%B %d, %I:%M %p")

                    record = [exit_time, close_alert, pnl, option_pnl]

                    with open("pnl_log.csv", "a", newline='') as log:
                        csv_output = writer(log)
                        csv_output.writerow(record)

                    retStr = str("""``` """ + close_alert + """```""")
                    embed = discord.Embed(title="\U0001F3E6 PnL")
                    embed.add_field(name='10 minute P/L for last SPX alert',value=retStr, inline=False)
                    embed.color = 0x00FF00 
                    await spx_channel.send(embed=embed)
                    break
                
                if option_last > (option_entry_price + (option_entry_price * .2)): # take profit at 20%
                    close_alert = "20% gain on call. " + symbol + " Bull Pivot gain since last alert at " + entry_time + ": $" + str(pnl) + ', ' + ITM_contract + ': $' + str(option_pnl)
                    color = 'green'
                    
                    tz_NY = pytz.timezone('America/New_York')
                    now = datetime.datetime.now(tz_NY)
                    exit_time = now.strftime("%B %d, %I:%M %p")

                    record = [exit_time, close_alert, pnl, option_pnl]

                    with open("pnl_log.csv", "a", newline='') as log:
                        csv_output = writer(log)
                        csv_output.writerow(record)

                    retStr = str("""``` """ + close_alert + """```""")
                    embed = discord.Embed(title="\U0001F3E6 PnL")
                    embed.add_field(name='10 minute P/L for last SPX alert',value=retStr, inline=False)
                    embed.color = 0x00FF00 
                    await spx_channel.send(embed=embed)
                    break

    #fix chart colors/names  
    if arg == 'spxput':
        
        symbol = '$SPX.X'
        ITM_contract, option_entry_price = build_0dte_order('short', symbol)

        df = pd.read_csv('SPX_output.csv')
        high1 = df.iloc[-1]['high']
        high2 = df.iloc[-2]['high']
        high3 = df.iloc[-3]['high']
        entry_price = df.iloc[-1]['open']
        entry_time = str(df.iloc[-1]['datetime'])
        pivot_high = max(high1,high2,high3)

        symbol = 'SPX'
        spx_pivot_alert = symbol + " Bear Pivot Alert: $" + str(pivot_high)
        retStr = str("""```""" + spx_pivot_alert + ' Entry Price: ' + str(entry_price) + ', ' + ITM_contract + ' @ ' + str(option_entry_price) + """```""")
        embed = discord.Embed(title="\U0001F6A8 Alert")
        embed.add_field(name=entry_time,value=retStr, inline=False)
        
        chart_buffer = create_bear_pivot_chart(df)

        # Attach the chart to the Discord message
        chart_file = File(chart_buffer, filename='alert_signal.png')
        embed.set_image(url='attachment://alert_signal.png')

        await spx_channel.send(embed=embed, file=chart_file)

        spx_type = 'Manual Bear Pivot'
        record = [entry_time, symbol, pivot_high, spx_pivot_alert, spx_type, entry_price, ITM_contract, option_entry_price]

        with open("spx_alert_log.csv", "a", newline='') as log:
            csv_output = writer(log)
            csv_output.writerow(record)

        print("Waiting 10 minutes...")
        await asyncio.sleep(600)

        df = pd.read_csv('SPX_output.csv')
        exit_price = df.iloc[-1]['low']
        option_last, option_bid = get_option_mark(ITM_contract)
        pnl = round(entry_price - exit_price, 2)
        option_pnl = round(option_last - option_entry_price)

        if pnl < 0:
            close_alert = symbol + " Bear Pivot loss (10m) since last alert at " + entry_time + ": $" + str(pnl) + ', ' + ITM_contract + ': $' + str(option_pnl)
            color = 'red'

            tz_NY = pytz.timezone('America/New_York')
            now = datetime.datetime.now(tz_NY)
            exit_time = now.strftime("%B %d, %I:%M %p")
            
            record = [exit_time, close_alert, pnl, option_pnl]

            with open("pnl_log.csv", "a", newline='') as log:
                csv_output = writer(log)
                csv_output.writerow(record)


        else:
            close_alert = symbol + " Bear Pivot gain (10m) since last alert at " + entry_time + ": $" + str(pnl) + ', ' + ITM_contract + ': $' + str(option_pnl)
            color = 'green'
            
            tz_NY = pytz.timezone('America/New_York')
            now = datetime.datetime.now(tz_NY)
            exit_time = now.strftime("%B %d, %I:%M %p")

            record = [exit_time, close_alert, pnl, option_pnl]

            with open("pnl_log.csv", "a", newline='') as log:
                csv_output = writer(log)
                csv_output.writerow(record)

        retStr = str("""``` """ + close_alert + """```""")
        embed = discord.Embed(title="\U0001F3E6 PnL")
        embed.add_field(name='10 minute P/L for last SPX alert',value=retStr, inline=False)
        if color == 'green':
            embed.color = 0x00FF00
        elif color == 'red':
            embed.color = 0xFF0000
        else:
            embed.color = 0x000000
        await spx_channel.send(embed=embed)    

@bot.command()
async def range(ctx, arg): # The name of the function is the name of the command
    print(arg) # this is the text that follows the command
    
    symbol = arg.upper()
    str(symbol)
    print(symbol)

    endpoint = r"https://api.tdameritrade.com/v1/marketdata/{}/quotes".format(symbol)
    payload = {'apikey': API_KEY,
            'periodType' : 'day',
            'frequencyType' : 'minute',
            'frequency' : '1',
            'period' : '2',
            }
    content = requests.get(url=endpoint, params=payload)
    data = content.json()

    h = data[symbol]['highPrice']
    l = data[symbol]['lowPrice']
    avg_price = (l + h) / 2
    rel_percent = ((h - l) / avg_price)*100
    answer = str(round(rel_percent,2))
    
    await ctx.send('Current intraday trading range for ' + symbol + ' is ' + answer + '%')

@bot.command()
async def vix(ctx, *arg):
    
    if not arg:
        endpoint = r"https://api.tdameritrade.com/v1/marketdata/{}/quotes".format('$VIX.X')
        payload = {'apikey': API_KEY,
            'periodType' : 'day',
            'frequencyType' : 'minute',
            'frequency' : '1',
            'period' : '2',
            }
        vix = requests.get(url=endpoint, params=payload)
        data = vix.json()
        last = data['$VIX.X']['lastPrice']
        await ctx.send('Current VIX level: ' + str(last))
    
    else:
        arg = int(round(float(arg[0]),0)) # number comes in as a tuple element e.g. ('xx',)
        results = vixdata.query('vix_open == @arg')
        results = results[['spy_range', 'vix_open']]
        mean = round(results['spy_range'].mean(),2)
        median = round(results['spy_range'].median(),2)
        desc = results.describe()
        minimum = round(desc.loc['min', 'spy_range'],2)
        maximum = round(desc.loc['max', 'spy_range'],2)
        tf = round(desc.loc['25%', 'spy_range'],2)
        sf = round(desc.loc['75%', 'spy_range'],2)
        number = str(round(desc.loc['count']['vix_open']))

        msg = '''\
        VIX has historically opened at {level} approximately {num} times. The mean move prediction for SPY is {mn}%. The median move prediction for SPY is {md}%. At this VIX level, the minimum historical SPY range was {min}% and the maximum range was {max}%. At this level SPY has a 75% probability of moving more than {twentyfive}% and a 75% probability of moving less than {seventyfive}%.\
        '''.format(level=arg, num=number, mn=mean, md=median, min=minimum, max=maximum, twentyfive=tf, seventyfive=sf )
        await ctx.send(msg)

@bot.command()
async def tp(ctx, arg):
    spx_channel = bot.get_channel(int(1070286165082648667))
    test_channel = bot.get_channel(int(973516018523701248)) #bot-test channel

    df = pd.read_csv('SPX_output.csv')
    exit = df.iloc[-1]['high']
    option_entry_price = 9.1
    entry_price = 4060.37
    ITM_contract = arg
    option_last, option_bid = get_option_mark(ITM_contract)
    option_pnl = round(option_last - option_entry_price)
    pnl = round(exit - entry_price, 2)

    entry_time = "12:55:00"
    symbol = "SPX"
    close_alert = symbol + " Bull Pivot gain (10m) since last alert at " + entry_time + ": $" + str(pnl) + ', ' + ITM_contract + ': $' + str(option_pnl)
    
    tz_NY = pytz.timezone('America/New_York')
    now = datetime.datetime.now(tz_NY)
    exit_time = now.strftime("%B %d, %I:%M %p")

    record = [exit_time, close_alert, pnl, option_pnl]

    with open("pnl_log.csv", "a", newline='') as log:
        csv_output = writer(log)
        csv_output.writerow(record)

    retStr = str("""``` """ + close_alert + """```""")
    embed = discord.Embed(title="\U0001F3E6 PnL")
    embed.add_field(name='10 minute P/L for last SPX alert',value=retStr, inline=False)
    embed.color = 0x00FF00 
    await spx_channel.send(embed=embed)

def manage_signals_thread():
    while True:
        signal_thread = threading.Thread(target=run_signals)
        print('Starting signal thread...')
        signal_thread.start()

        # wait for it to complete
        signal_thread.join()

        # Delay before restarting the thread if it stops
        time.sleep(5)

if __name__ == '__main__':
    # Create a new thread for the bot client
    bot_thread = threading.Thread(target=bot.run, args=(TOKEN,), kwargs={'reconnect': True})

    # Manage signal data thread
    thread_manager = threading.Thread(target=manage_signals_thread)
    thread_manager.start()

    print('Starting discord bot...')
    bot_thread.start()
    
    # Wait for both threads to complete
    bot_thread.join()