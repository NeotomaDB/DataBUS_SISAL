import mysql.connector
from dotenv import load_dotenv
from json import loads
from os import getenv, path
import csv
from re import sub

def write_to_csv(sisal:list, col_names:tuple, data_path = '../../data/') -> None:
    """_Take dict objects from SISAL db query and export to csv files._
    The script assumes a 

    Args:
        sisal (list): _A list of rows from the SQL query_
        col_names (tuple): _A tuple with the column names to be used._
        data_path (str): _A string pointing to a valid folder path._

    """
    keys = col_names
    to_file = []
    for i in sisal:
        out_dict = {}
        for k in range(len(keys)):
            if keys[k] not in out_dict.keys():
                out_dict[keys[k]] = i[k] 
        to_file.append(out_dict)
    new_keys = out_dict.keys()
    with open(f'{data_path}sisal_site_{sisal[0][0]}.csv', 'w', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file, new_keys)
        dict_writer.writeheader()
        dict_writer.writerows(to_file)
    return None

def data_path_exists(data_path:str, filename:str = None) -> bool:
    """_Check to see if a file or path exists, and return either True or False._

    Args:
        path (str): _A folder path for the data files._
        filename (_str_, optional): _An optional filename if looking to overwrite (for example)_. Defaults to None.

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
con = mysql.connector.connect(**CONN_STRING)

# Get all unique site_id values from SISAL v3
call_siteids = """
SELECT DISTINCT site.site_id
FROM site"""

cur = con.cursor()
cur.execute(call_siteids)
row = cur.fetchall()

# Load in the larger query:
with open('sql/wide_export.sql') as f:
    big_query = f.read()

for i in row:
    cur.execute(big_query, {'site_id': i[0]})
    colnames = cur.column_names
    site_output = cur.fetchall()
    if data_path_exists('../../data/'):
        write_to_csv(site_output, colnames, data_path='../../data/')

