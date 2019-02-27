import datetime
import pandas as pd
import sqlalchemy as db
import yaml
import opat.portfolio

# Load configuration file
with open("config.yml", "r") as cfgfile:
    cfg = yaml.load(cfgfile, Loader=yaml.FullLoader)

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

# Load transactions
# -------------------------------------------------
trades = pd.read_sql_query(
    sql="SELECT tradeday, account, ticker, action, quantity FROM DW.trades", con=DB_CON)
splits = pd.read_sql_query(
    sql="SELECT tradeday, ticker, close, split FROM DW.eod_price", con=DB_CON)

# Create holdings
# -------------------------------------------------
holdings = trades.groupby("account").apply(opat.portfolio.create_holdings, splits = splits)
holdings = holdings.reset_index()
holdings = holdings[["tradeday", "account", "ticker", "quantity"]]

# Write holdings to database
# -------------------------------------------------
holdings.to_sql(name="eod_holdings", con=DB_CON, if_exists="replace", index=False)