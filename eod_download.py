import requests
import yaml
import pandas as pd
import time

# Load configuration file
with open("config.yml", "r") as cfgfile:
    cfg = yaml.load(cfgfile)

# Assign initial api key
API_KEY = cfg["api_key"].pop()
ticker_list = ["APRN", "JD", "TEAM", "CRM", "KO", "KHC", "LQD",]

# Alpha Vantage URL
API_URL = "https://www.alphavantage.co/query"


def download_price(ticker, api_key):
    PARAMS = {"function": "TIME_SERIES_DAILY_ADJUSTED",
              "symbol": ticker,
              "outputsize": "full",
              "datatype": "json",
              "apikey": api_key}
    r = requests.get(url=API_URL, params=PARAMS)
    data = r.json()
    return(pd.DataFrame.from_dict(data['Time Series (Daily)'], orient="index").head())


# Download data
for ticker in ticker_list:
    try:
        print(download_price(ticker=ticker, api_key=API_KEY))
    except:
        # Alpha Vantage limits the number of API calls per minute.
        time.sleep(60)
        print(download_price(ticker=ticker, api_key=API_KEY))
