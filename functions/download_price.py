import datetime
import requests
import pandas as pd
import yaml

with open("./config.yml", "r") as cfgfile:
    cfg = yaml.load(cfgfile)

# Alpha Vantage
API_KEY = cfg["api_key"][0]
API_URL = "https://www.alphavantage.co/query"

def download_price(ticker):
    """Download price data from Alpha Vantage API
    
    Arguments:
        ticker {string} -- the ticker to download price data for 
    """

    PARAMS = {"function": "TIME_SERIES_DAILY_ADJUSTED",
              "symbol": ticker,
              "outputsize": "full",
              "datatype": "json",
              "apikey": API_KEY}

    # Request data from Alpha Vantage
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