import requests
import yaml
import pandas as pd
import time
import datetime
import sqlalchemy as db
import random

# Load configuration file
with open("config.yml", "r") as cfgfile:
    cfg = yaml.load(cfgfile)

# Configurations
# -------------------------------------------------
# API key for Alpha Vantage
API_KEY = 1
# Database
USER = cfg["mysql"]["user"]
PASSWORD = cfg["mysql"]["password"]
HOST = cfg["mysql"]["host"]
PORT = cfg["mysql"]["port"]
DATABASE = "price_data"
DB_CON = db.create_engine("mysql+pymysql://{user}:{password}@{host}:{port}/{database}".format(
    user=USER, password=PASSWORD, host=HOST, port=PORT, database=DATABASE)).connect()

# Alpha Vantage URL
API_URL = "https://www.alphavantage.co/query"

# Proxy to mask ip
PROXY_LIST = pd.read_sql_query(
    sql="SELECT proxy FROM config.proxy_list", con=DB_CON)
PROXY_LIST = PROXY_LIST["proxy"].values

# use only fast proxy
PROXY_USE = list()
for proxy in PROXY_LIST:
    try:
        requests.get("http://"+proxy, timeout=0.2)
        PROXY_USE.append(proxy)
    except:
        pass

# Tickers to download
ticker_list = ["APRN", "JD", "TEAM", "CRM", "KO",
               "KHC", "LQD", "DIS", "FB", "GOOGL", "STZ"]


def download_price(ticker, api_key, proxy):
    PARAMS = {"function": "TIME_SERIES_DAILY_ADJUSTED",
              "symbol": ticker,
              "outputsize": "full",
              "datatype": "json",
              "apikey": api_key}

    # Request proxy
    proxy = "http://" + proxy
    proxy = {"http": proxy,
             "https": proxy}

    # Request data from alpha vantage
    r = requests.get(url=API_URL, params=PARAMS, proxies=proxy, timeout=5)

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


# Download data
proxy = random.choice(PROXY_USE)
for ticker in ticker_list:
    success = False
    while not success:
        try:
            price_series = download_price(
                ticker=ticker, api_key=API_KEY, proxy=proxy)
            price_series.to_sql(name="eod_stock_history",
                                con=DB_CON, if_exists="append", index=False)
            print(datetime.datetime.now(), "----", ticker,
                  "---- is written to price_data.eod_stock_history.")
            success = True
        except:
            proxy = random.choice(PROXY_USE)
            pass
