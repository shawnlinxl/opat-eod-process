import time
import pandas as pd
import sqlalchemy as db
import yaml

from functions.download_price import download_price
from functions.progress_bar import progress_bar

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

# Ticker list to query data for
TICKER_LIST = pd.read_sql_query(
    sql="SELECT DISTINCT Ticker FROM DW.trades", con=DB_CON)
TICKER_LIST = TICKER_LIST["Ticker"].values

# Download data
# -------------------------------------------------
l = len(TICKER_LIST)
progress_bar(0, l, prefix='Progress:', suffix='Complete', length=50)

for i, ticker in enumerate(TICKER_LIST):


    success = False
    while not success:
        try:
            price_series = download_price(ticker=ticker)
            price_series.to_sql(name="eod_price_archive",
                                con=DB_CON, if_exists="append", index=False)
            success = True
            progress_bar(i, l, prefix='Progress:', suffix='Complete', length=50)
        except:
            time.sleep(60)


progress_bar(l, l, prefix='Progress:', suffix='Complete', length=50)