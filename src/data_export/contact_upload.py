import mysql.connector
import psycopg2
from dotenv import load_dotenv
from json import loads, dumps, load
from os import getenv, path
from re import sub

# Create database connector:
load_dotenv()
CONN_STRING = loads(getenv("SISAL_CONNECT"))
NDB_STRING = loads(getenv("DBAUTH"))
con = mysql.connector.connect(**CONN_STRING)
con_ndb = psycopg2.connect(**NDB_STRING)

# Get all unique site_id values from SISAL v3
call_entityids = """
SELECT DISTINCT entity.contact
FROM entity"""

cur = con.cursor()
cur.execute(call_entityids)
row = cur.fetchall()

contacts = []

for i in row:
    name = i[0].split(',')
    for j in name:
        j = j.strip()
        neotoma_contact = {'familyname': j.split(' ')[-1],
                           'contactname': j.split(' ')[-1] + ', ' + ' '.join(j.split(' ')[:-1]),
                           'contactstatusid': 1,
                           'givennames': ' '.join(j.split(' ')[:-1])}
        contacts.append(neotoma_contact)

short = [loads(i) for i in set([dumps(i) for i in contacts])]

insert_contact = """
INSERT INTO ndb.contacts(contactname, contactstatusid, familyname, givennames, notes)
VALUES(%(contactname)s, %(contactstatusid)s, %(familyname)s, %(givennames)s, 'Inserted through SISAL Bulk Upload');
"""

for i in short:
    with con_ndb.cursor() as cur:
        cur.execute(insert_contact, i)
    con_ndb.commit()
