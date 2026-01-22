import json
import os
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import pandas as pd

load_dotenv()
data = json.loads(os.getenv('PGDB_LOCAL'))
conn = psycopg2.connect(**data, connect_timeout = 5)
cur = conn.cursor()

df = pd.read_csv('data/references_entities.csv')
data_tuples = [tuple(x) for x in df.to_numpy()]
columns = ', '.join(df.columns)

query = f"INSERT INTO ndb.entityrelationship ({columns}) VALUES %s"
execute_values(cur, query, data_tuples)

conn.commit()
cur.close()
conn.close()