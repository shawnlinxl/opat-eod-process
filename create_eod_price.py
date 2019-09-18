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
query = """
SELECT main.tradeday, main.ticker, open, high, low, close, adj, volume, dividend, split
  FROM DW.eod_price_archive AS main
 INNER JOIN 
      (SELECT tradeday, ticker, MAX(modified) AS modified
       FROM DW.eod_price_archive
       GROUP BY tradeday, ticker) AS aux
    ON main.tradeday = aux.tradeday AND main.ticker = aux.ticker AND main.modified = aux.modified
 ORDER BY tradeday, ticker
"""

price = pd.read_sql_query(sql=query, con=DB_CON)

# Price for APRN is wrong historically after Aug
# 05, 2019
# -------------------------------------------------

query = """
SELECT tradeday, ticker, open, high, low, close, adj, volume, dividend, split
  FROM DW.eod_price_archive 
 WHERE ticker = 'APRN' AND modified BETWEEN '2019-08-05' AND '2019-08-06'
 ORDER BY tradeday, ticker
"""

aprn_fix = pd.read_sql_query(sql=query, con=DB_CON)

price = price[(price.ticker == 'APRN' & price.tradeday > "2019-08-05") | (price.ticker != 'APRN')]
price = pd.concat([price, aprn_fix], ignore_index=True)

# Write price data to database
# -------------------------------------------------
price.to_sql(name="eod_price", con=DB_CON, if_exists="replace", index=False)
