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

# Load transactions and price data
# -------------------------------------------------
trades = pd.read_sql_query(
    sql="SELECT tradeday, account, ticker, action, price, quantity FROM DW.trades", con=DB_CON)
prices = pd.read_sql_query(
    sql="SELECT tradeday, ticker, close, dividend FROM DW.eod_price", con=DB_CON)

# Create pnl
# -------------------------------------------------
pnl = trades.groupby("account").apply(opat.portfolio.create_pnl, prices=prices).reset_index()
pnl = pnl[["tradeday", "account", "ticker", "pnl"]]
pnl["pnl"] = round(pnl["pnl"], 6)

# Write holdings to database
# -------------------------------------------------
pnl.to_sql(name="eod_pnl", con=DB_CON, if_exists="replace", index=False)