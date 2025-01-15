import psycopg2
import json
import os
import glob
from dotenv import load_dotenv
import neotomaUploader as nu
import csv

load_dotenv()

data = json.loads(os.getenv('PGDB_LOCAL'))
conn = psycopg2.connect(**data, connect_timeout = 5)



def check_loc(cur, lat, long):
    query = '''
        SELECT "name_0", "name_1", "name_2", "name_3" FROM ap.gadm AS ga
        WHERE ST_Within(ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326), ga.shape)
        '''
    cur.execute(query, {'longitude': long, 'latitude': lat})
    return cur.fetchone()

def check_lake(cur, lat, long):
    query = '''
        SELECT "lake_name" FROM ap.hydrolakes AS lk
        WHERE ST_Distance(ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326)::geometry,
                                 ST_Transform(lk.shape, 4326)::geometry) < 50
        ORDER BY ST_Distance(ST_SetSRID(ST_MakePoint(%(longitude)s, %(latitude)s), 4326)::geometry,
                                 ST_Transform(lk.shape, 4326)::geometry) ASC
        LIMIT 5;
        '''
    cur.execute(query, {'longitude': long, 'latitude': lat})
    return cur.fetchone()

with open('data/e_asian_ostracode.csv', 'r', encoding='utf8') as f:
    infile = list(csv.DictReader(f))

cur = conn.cursor()

ctry = []
lake = []

for i in infile:
    result = check_loc(cur, i.get('decimalLatitude'), i.get('decimalLongitude'))
    lake_res = check_lake(cur, i.get('decimalLatitude'), i.get('decimalLongitude'))
    if result is None:
        ctry.append(None)
    else:
        ctry.append(result)
    if lake_res is None:
        lake.append(None)
    else:
        lake.append(lake_res)

with open('countrytest.json', 'w', encoding='utf8') as out:
    json.dump(aa, out)
