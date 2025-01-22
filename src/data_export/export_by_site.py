import mysql.connector
from dotenv import load_dotenv
from json import loads
from os import getenv
import csv

def write_to_csv(sisal:list, colnames:tuple):
    keys = colnames
    tofile = []
    for i in sisal:
        outdict = {}
        for k in range(len(keys)):
            if keys[k] not in outdict.keys():
                outdict[keys[k]] = i[k] 
        tofile.append(outdict)
    new_keys = outdict.keys()
    with open(f'../../data/sisal_site_{sisal[0][0]}.csv', 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, new_keys)
        dict_writer.writeheader()
        dict_writer.writerows(tofile)
    return None


call_siteids = """
SELECT DISTINCT site.site_id
FROM site"""

load_dotenv()
CONN_STRING = loads(getenv("SISAL_CONNECT"))
        
con = mysql.connector.connect(**CONN_STRING)

cur = con.cursor()
cur.execute(call_siteids)
row = cur.fetchall()

# Load in the larger query:
with open('sql/wide_export.sql') as f:
    big_query = f.read()

for i in row:
    cur.execute(big_query, i)
    colnames = cur.column_names
    site_output = cur.fetchall()
    write_to_csv(site_output, colnames)

