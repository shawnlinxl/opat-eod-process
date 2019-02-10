import requests
import yaml
import pandas as pd
import time

# Load configuration file
with open("config.yml", "r") as cfgfile:
    cfg = yaml.load(cfgfile)

# Assign initial api key
API_KEY = cfg["api_key"].pop()
ticker_list = ["APRN", "JD", "TEAM", "CRM", "KO", "KHC", "LQD", "DIS", "FB", "GOOGL", "STZ"]

# Alpha Vantage URL
API_URL = "https://www.alphavantage.co/query"


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
    data = pd.DataFrame.from_dict(data['Time Series (Daily)'], orient="index")

    # Assign date index to its own column
    data["date"] = data.index

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
        
    return(data)


# Download data
for ticker in ticker_list:
    try:
        price_series = download_price(ticker=ticker, api_key=API_KEY)
        print(price_series.tail())
    except:
        # Alpha Vantage limits the number of API calls per minute.
        time.sleep(60)

        price_series = download_price(ticker=ticker, api_key=API_KEY)
        print(price_series.tail())
