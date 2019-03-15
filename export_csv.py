import pandas as pd
import sqlalchemy as db
import yaml


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

# Load data
# -------------------------------------------------
attr = pd.read_sql_query(sql="SELECT tradeday, account, ticker, attr FROM DW.eod_attr;", con=DB_CON)

nav = pd.read_sql_query(sql="SELECT tradeday, account, SUM(nav) AS nav FROM DW.eod_nav GROUP BY tradeday, account", con=DB_CON)

holdings = pd.read_sql_query(sql="SELECT * FROM DW.eod_holdings;", con=DB_CON)

prices = pd.read_sql_query(sql="SELECT * FROM DW.eod_price;", con=DB_CON)

prices = prices[prices["ticker"].isin(holdings["ticker"].values)]



# Write to csv files 
# -------------------------------------------------
attr.to_csv(r"~/Documents/Projects/opat/data-warehouse/csv/attr.csv", index=False)
nav.to_csv(r"~/Documents/Projects/opat/data-warehouse/csv/nav.csv", index=False)
prices.to_csv(r"~/Documents/Projects/opat/data-warehouse/csv/prices.csv", index=False)
holdings.to_csv(r"~/Documents/Projects/opat/data-warehouse/csv/holdings.csv", index=False)
