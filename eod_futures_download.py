import quandl
import yaml
import sqlalchemy as db
import pandas as pd

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
DATABASE = "config"
DB_CON = db.create_engine("mysql+pymysql://{user}:{password}@{host}:{port}/{database}".format(
    user=USER, password=PASSWORD, host=HOST, port=PORT, database=DATABASE)).connect()

# Ticker list to query data for
TICKER_LIST = pd.read_sql_query(sql="SELECT DISTINCT `Quandl Code`, `Number of Contracts` FROM config.quandl_continuous_futures", con=DB_CON)


# quandl.ApiConfig.api_key = cfg["quandl_api_key"]
# data = quandl.get('CHRIS/CME_ES1', start_date='1999-07-12', end_date="2019-07-12")