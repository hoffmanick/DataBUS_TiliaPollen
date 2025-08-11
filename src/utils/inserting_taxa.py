## Inserting Taxa Data

import psycopg2
import pandas as pd
from dotenv import load_dotenv
import json
import os

load_dotenv()
data = json.loads(os.getenv('PGDB_LOCAL'))

conn = psycopg2.connect(**data, connect_timeout = 5)
cur = conn.cursor()

taxa = pd.read_csv('data-all/original/NODE/taxa.csv', sep=',')
taxa['ID'] = None

print(f"Taxa to be inserted: {taxa.shape[0]}")
query = """SELECT ts.inserttaxon(_code := %(_code)s,
                                  _name := %(_name)s,
                                  _extinct := %(_extinct)s,
                                  _groupid := %(_groupid)s,
                                  _higherid := %(_higherid)s,
                                  _author := %(_author)s)
        """
higher_taxa = [['Amplocypris', 'Zalanyi, 1944'], ['Dolekiella', 'Gido et al., 2007']]

for t in higher_taxa:
    query_params = {
        '_code': t[0].strip(),
        '_name': t[0].strip(),
        '_extinct': False,
        '_groupid': 'OST',
        '_higherid': None,
        '_author': t[1].strip()
    }
    cur.execute(query, query_params)
    id = cur.fetchone()[0]
    t.append(id)
    conn.commit()

for index, row in taxa.iterrows():
    query_params = {
            '_code': row['Taxa'].strip(),
            '_name': row['Taxa'].strip(),
            '_extinct': False,
            '_groupid': 'OST',
            '_higherid': int(row['higherTaxon']) if pd.notna(row['higherTaxon']) else None,
            '_author': row['AuthorName'].strip() if pd.notna(row['AuthorName']) else None
        }
    if not pd.isna(row['higherTaxon']):
        try:
            cur.execute(query, query_params)
            id = cur.fetchone()[0]
            taxa.at[index, 'ID'] = id
            print(f"Inserted {row['Taxa']} with ID {id}")
            conn.commit()
        except Exception as e:
            print(f"Error inserting {row['Taxa']}: {e}")
            taxa.at[index, 'ID'] = None
            conn.rollback()
            continue
    else:
        if 'amplocypris' in row['Taxa'].lower() or 'dolekiella' in row['Taxa'].lower():
            taxa.at[index, 'higherTaxon'] = int(next(t[2] for t in higher_taxa if t[0].lower() in row['Taxa'].lower()))
            query_params['_higherid'] = int(taxa.loc[index, 'higherTaxon'])
            try:
                cur.execute(query, query_params)
                id = cur.fetchone()[0]
                taxa.at[index, 'ID'] = id
                conn.commit()
                print(f"Inserted {row['Taxa']} with ID {id}")
            except Exception as e:
                print(f"Error inserting {row['Taxa']}: {e}")
                taxa.at[index, 'ID'] = None
                conn.rollback()
                continue

# save ids into a csv file
taxa.to_csv('data-all/original/NODE/taxa.csv', index=False)