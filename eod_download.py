import datetime
import time
import pandas as pd
import random
import requests
import sqlalchemy as db
import yaml

# Load configuration file
with open("config.yml", "r") as cfgfile:
    cfg = yaml.load(cfgfile)

# Configurations
# -------------------------------------------------

# Database
USER = cfg["mysql"]["user"]
PASSWORD = cfg["mysql"]["password"]
HOST = cfg["mysql"]["host"]
PORT = cfg["mysql"]["port"]
DATABASE = "DW"
DB_CON = db.create_engine("mysql+pymysql://{user}:{password}@{host}:{port}/{database}".format(
    user=USER, password=PASSWORD, host=HOST, port=PORT, database=DATABASE)).connect()

# Alpha Vantage
API_KEY = cfg["api_key"][0]
API_URL = "https://www.alphavantage.co/query"

# Ticker list to query data for
TICKER_LIST = pd.read_sql_query(
    sql="SELECT DISTINCT Ticker FROM DW.trades", con=DB_CON)
TICKER_LIST = TICKER_LIST["Ticker"].values


# Functions
# -------------------------------------------------
# Print iterations progress
def printProgressBar(iteration, total, prefix='', suffix='', decimals=1, length=100, fill='█'):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 *
                                                     (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('\r%s |%s| %s%% %s' % (prefix, bar, percent, suffix), end='\r')
    # Print New Line on Complete
    if iteration == total:
        print()


def download_price(ticker):
    PARAMS = {"function": "TIME_SERIES_DAILY_ADJUSTED",
              "symbol": ticker,
              "outputsize": "full",
              "datatype": "json",
              "apikey": API_KEY}

    # Request data from alpha vantage
    r = requests.get(url=API_URL, params=PARAMS, timeout=5)

    # Serialize result into json
    data = r.json()

    # Convert json to dataframe
    data = pd.DataFrame.from_dict(
        data['Time Series (Daily)'], orient="index", dtype="float")

    # Assign date index to its own column
    data["tradeday"] = pd.to_datetime(data.index, format="%Y-%m-%d")

    # Add ticker to a new column
    data["ticker"] = ticker

    # Rearrange so date is the first column
    cols = list(data.columns.values)
    cols = cols[-2:] + cols[:-2]
    data = data[cols]

    # Rename columns
    data = data.rename(columns={"1. open": "open", "2. high": "high", "3. low": "low",
                                "4. close": "close", "5. adjusted close": "adj",
                                "6. volume": "volume", "7. dividend amount": "dividend",
                                "8. split coefficient": "split"})

    # Add completion timestamp
    currentDT = datetime.datetime.now()
    data["modified"] = currentDT

    return(data)


# Main
# -------------------------------------------------

# Download data
print("--------------------Downloading Data--------------------")
l = len(TICKER_LIST)
printProgressBar(0, l, prefix='Progress:', suffix='Complete', length=50)

for i, ticker in enumerate(TICKER_LIST):

    printProgressBar(i, l, prefix='Progress:', suffix='Complete', length=50)

    success = False
    while not success:
        try:
            price_series = download_price(ticker=ticker)
            price_series.to_sql(name="eod_price",
                                con=DB_CON, if_exists="append", index=False)
            success = True
        except:
            time.sleep(60)
