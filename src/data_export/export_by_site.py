from mysql.connector import connect
from dotenv import load_dotenv
from json import loads
from os import getenv
from os import path
import csv
from re import sub


def write_to_csv(sisal:list, colnames:tuple) -> None:
    """_Write the SISAL Query output for a single site to file._

    Args:
        sisal (list): _A list of tuples returned from the query cursor._
        colnames (tuple): _A tuple of colnames that are linked to the sisal list._

    Returns:
        _None_: _This function writes to file, and does not return anything._
    """
    assert all(len(x) == len(sisal[0]) for x in sisal), "All list elements must be the same length."
    assert len(colnames) == len(sisal[0]), "The number of column names must be the same as the list element lengths."
    keys = colnames
    tofile = []
    for i in sisal:
        out_dict = {}
        for k in range(len(keys)):
            if keys[k] not in outdict.keys():
                outdict[keys[k]] = i[k] 
        tofile.append(outdict)
    new_keys = outdict.keys()
    with open(f'../../data/sisal_site_{sisal[0][0]}.csv', 'w', newline='', encoding="UTF-8") as output_file:
        dict_writer = csv.DictWriter(output_file, new_keys)
        dict_writer.writeheader()
        dict_writer.writerows(to_file)
    return None

assert path.isdir('../../data'), "There is no directory at '../../data'."

call_siteids = """
    SELECT DISTINCT site_id
    FROM site
"""

    Returns:
        bool: _Does the directory or file exist?_
    """
    if filename:
        clean_path = sub('/$', '', data_path)
        output = path.isfile(f'{clean_path}{filename}')
    else:
        output = path.isdir(data_path)
    return output

# Create database connector:
load_dotenv()
CONN_STRING = loads(getenv("SISAL_CONNECT"))
        
con = connect(**CONN_STRING)

# Get all unique site_id values from SISAL v3
call_siteids = """
SELECT DISTINCT site.site_id
FROM site"""

cur = con.cursor()
cur.execute(call_siteids)
row = cur.fetchall()

assert len(row) > 0, "No sites were returned. Is the database present and intact?"

# Load in the larger query:
with open('sql/wide_export.sql') as f:
    big_query = f.read()

for i in row:
    cur.execute(big_query, {'site_id': i[0]})
    colnames = cur.column_names
    site_output = cur.fetchall()
    if data_path_exists('../../data/'):
        write_to_csv(site_output, colnames, data_path='../../data/')
