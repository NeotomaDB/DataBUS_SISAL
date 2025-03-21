import json
import os
import psycopg2
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timedelta

load_dotenv()
data = json.loads(os.getenv('PGDB_TANK'))
conn = psycopg2.connect(**data, connect_timeout = 5)
cur = conn.cursor()

a_list = list(range(33479, 33491))
print(a_list)
query = """SELECT siteid, recdatecreated
           FROM ndb.sites
           ORDER BY recdatecreated DESC
           LIMIT 1"""

cur.execute(query)
data = cur.fetchall()
data = pd.DataFrame(data, columns = ['siteid', 'recdatecreated'])

for index, row in data.iterrows():
    site = row['siteid']
    date = row['recdatecreated']
    print(f"site: {site}")
    if (datetime.now() - date).days <= 1:
        #query = """SELECT ts.deletesite(%(site)s)"""
        #cur.execute(query, {'site': site})
        print(f"Removed Site: {site}.")
    else:
        print("Handle not removed")
conn.commit()
print("Finished")