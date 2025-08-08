import json
import os
import psycopg2
from dotenv import load_dotenv
import pandas as pd
import codecs
from datetime import datetime, timedelta

load_dotenv()
raw = os.getenv('PGDB_TANK')
data = codecs.decode(raw.encode('utf-8'), 'unicode_escape')
data = json.loads(data)
conn = psycopg2.connect(**data, connect_timeout = 5)
cur = conn.cursor()

query = """SELECT s.siteid, c.recdatecreated FROM ndb.sites s
           JOIN ndb.collectionunits c ON s.siteid = c.siteid
           JOIN ndb.datasets d ON c.collectionunitid = d.collectionunitid
           JOIN ndb.datasettypes dt ON d.datasettypeid = dt.datasettypeid
           WHERE dt.datasettypeid = 44
           LIMIT 60;"""

cur.execute(query)
data = cur.fetchall()
data = pd.DataFrame(data, columns = ['siteid', 'recdatecreated'])
print(data)
for index, row in data.iterrows():
    site = row['siteid']
    date = row['recdatecreated']
    print(f"site: {site}")
    if (datetime.now() - date).days <= 1:
        query = """SELECT ts.deletesite(%(site)s)"""
        cur.execute(query, {'site': site})
        print(f"Removed Site: {site}.")
    else:
        print("Handle not removed")
    conn.commit()
print("Finished")