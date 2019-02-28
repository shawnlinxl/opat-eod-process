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
    sql="SELECT tradeday, ticker, close, dividend, split FROM DW.eod_price", con=DB_CON)
flows = pd.read_sql_query(
    sql="SELECT tradeday, account, amount FROM DW.flows", con=DB_CON)


# Create NAV
# -------------------------------------------------
nav = pd.DataFrame()
for account in flows["account"].unique():
    trades_use = trades.copy()[trades["account"] == account]
    flows_use = flows.copy()[flows["account"] == account]
    nav_account = opat.portfolio.create_nav(trades_use, prices, flows_use)
    nav_account["account"] = account
    nav = nav.append(nav_account[["tradeday", "account", "type", "ticker", "nav"]])


# Write NAV to database
# -------------------------------------------------
nav.to_sql(name="eod_nav", con=DB_CON, if_exists="replace", index=False)
