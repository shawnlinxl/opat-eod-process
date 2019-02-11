import requests
import yaml
import pandas as pd
import time
import datetime

from sqlalchemy import create_engine

# Load configuration file
with open("config.yml", "r") as cfgfile:
    cfg = yaml.load(cfgfile)

# Configurations
# -------------------------------------------------
# API key for Alpha Vantage
API_KEY = cfg["api_key"].pop()
# Database
USER = cfg["mysql"]["user"]
PASSWORD = cfg["mysql"]["password"]
HOST = cfg["mysql"]["host"]
PORT = cfg["mysql"]["port"]
DATABASE = cfg["mysql"]["database"]
DB_CON = create_engine("mysql+pymysql://{user}:{password}@{host}:{port}/{database}".format(
    user=USER, password=PASSWORD, host=HOST, port=PORT, database=DATABASE))
# Alpha Vantage URL
API_URL = "https://www.alphavantage.co/query"

# Tickers to download
ticker_list = ["APRN", "JD", "TEAM", "CRM", "KO",
               "KHC", "LQD", "DIS", "FB", "GOOGL", "STZ"]


def download_price(ticker, api_key):
    PARAMS = {"function": "TIME_SERIES_DAILY_ADJUSTED",
              "symbol": ticker,
              "outputsize": "full",
              "datatype": "json",
              "apikey": api_key}

    # Request data from alpha vantage
    r = requests.get(url=API_URL, params=PARAMS)

    # Serialize result into json
    data = r.json()

    # Convert json to dataframe
    data = pd.DataFrame.from_dict(
        data['Time Series (Daily)'], orient="index", dtype="float")

    # Assign date index to its own column
    data["date"] = pd.to_datetime(data.index, format="%Y-%m-%d")

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
for ticker in ticker_list:
    try:
        price_series = download_price(ticker=ticker, api_key=API_KEY)
        price_series.to_sql(name="eod_stock_history",
                            con=DB_CON, if_exists="append", index=False)
        print(datetime.datetime.now(), "----", ticker,
              "---- is written to price_data.eod_stock_history.")
    except:
        # Alpha Vantage limits the number of API calls per minute.
        time.sleep(60)

        price_series = download_price(ticker=ticker, api_key=API_KEY)
        price_series.to_sql(name="eod_stock_history",
                            con=DB_CON, if_exists="append", index=False)
        print(datetime.datetime.now(), "----", ticker,
              "---- is written to price_data.eod_stock_history.")
