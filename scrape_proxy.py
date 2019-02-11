import requests
import yaml
from bs4 import BeautifulSoup
import pandas as pd
import datetime
import sqlalchemy as db 

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
DATABASE = "config"
DB_CON = db.create_engine("mysql+pymysql://{user}:{password}@{host}:{port}/{database}".format(
    user=USER, password=PASSWORD, host=HOST, port=PORT, database=DATABASE)).connect()

res = requests.get('https://free-proxy-list.net/',
                   headers={'User-Agent': 'Mozilla/5.0'})
soup = BeautifulSoup(res.text, "lxml")
result = list()
for items in soup.select("tbody tr"):
    proxy_list = ':'.join([item.text for item in items.select("td")[:2]])
    if items.select("td")[4].text == "elite proxy":
        result.append(proxy_list)

res = requests.get('https://www.us-proxy.org/',
                   headers={'User-Agent': 'Mozilla/5.0'})
soup = BeautifulSoup(res.text, "lxml")
for items in soup.select("tbody tr"):
    proxy_list = ':'.join([item.text for item in items.select("td")[:2]])
    if items.select("td")[4].text == "elite proxy":
        result.append(proxy_list)



result = pd.DataFrame(
    data={'proxy': result, 'modified': datetime.datetime.now()})

result.to_sql(name="proxy_list", con=DB_CON, if_exists="append", index=False)
