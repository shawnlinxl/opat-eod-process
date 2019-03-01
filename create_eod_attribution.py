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
pnl = pd.read_sql_query(
    sql="SELECT * FROM DW.eod_pnl;", con=DB_CON)
nav = pd.read_sql_query(
    sql="SELECT tradeday, account, SUM(nav) AS nav FROM DW.eod_nav GROUP BY tradeday, account", con=DB_CON)
flows = pd.read_sql_query(
    sql="SELECT tradeday, account, SUM(amount) AS amount FROM DW.flows GROUP BY tradeday, account", con=DB_CON)


# Create attribution
# -------------------------------------------------
attr = pd.DataFrame()

for account in flows["account"].unique():
    pnl_use = pnl.copy()[pnl["account"] == account].drop("account", axis=1)
    nav_use = nav.copy()[nav["account"] == account].drop("account", axis=1)
    nav_use = nav_use.set_index("tradeday")
    nav_use["prev_nav"] = nav_use["nav"].shift(1, fill_value=0)
    nav_use = nav_use.reset_index()
    flows_use = flows.copy()[flows["account"] == account].drop("account", axis=1)

    attr_use = pnl_use.merge(nav_use, how="left", on="tradeday")
    attr_use = attr_use.merge(flows_use, how="left", on="tradeday")
    attr_use["amount"] = attr_use["amount"].fillna(value=0)
    attr_use["attr"] = attr_use["pnl"]/(attr_use["prev_nav"] + attr_use["amount"])
    attr_use["account"] = account

    attr = attr.append(attr_use[["tradeday", "account", "ticker", "attr"]])



# Write attribution to database
# -------------------------------------------------
attr.to_sql(name="eod_attr", con=DB_CON, if_exists="replace", index=False)
