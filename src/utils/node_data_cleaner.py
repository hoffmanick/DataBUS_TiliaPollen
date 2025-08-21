import pandas as pd
import re
from csv_splitter import csv_splitter

data = pd.read_excel('data-all/original/NODE database 22May2024.xls')
references = pd.read_csv('data-all/original/NODE_reference_list.tsv',
                         sep='\t', 
                         usecols=['NODE FULL REFERENCES', 'NODE REFERENCE CITATIONS'])
authors = pd.read_csv('data-all/original/NODE/authors_list2.csv', index_col=False)

data = data.merge(references, left_on='REFERENCE', right_on='NODE REFERENCE CITATIONS', how='left') 
data.columns = data.columns.str.lower()

# Site Data
#data['sitename'] = data['site'].fillna(data['locality'])
data['longitude'] = data['londeg'] + data['lonmin'] / 60 + data['lonsec'] / 3600
data['latitude'] = data['latdeg'] + data['latmin'] / 60 + data['latsec'] / 3600
def fill_site(x):
    non_null = x.dropna()
    if not non_null.empty:
        most_common = non_null.value_counts().idxmax()
        return pd.Series([most_common] * len(x), index=x.index)
    else:
        return x
data['sitename'] = data['locality'].fillna('country')
mask = data['sitename'].str.contains('unspecified|no_precise_loc', case=False, na=False)
data.loc[mask, 'sitename'] = data.loc[mask, 'region'].fillna(data.loc[mask, 'country'])

data['sitename'] = data['sitename'].str.strip()
data['sitename'] = data.groupby(['longitude', 'latitude'])['sitename'].transform(fill_site)
data = data.assign(**{'sample_analyst': 'Horne, David',
                      'dataset_processor': 'Horne, David'})

# CU
data['collectionunit_name'] = data['locality'].fillna('') + ' ' + data['site'].fillna('')
data['collectionunit_name'] = data['collectionunit_name'].fillna('handle_complete').str.strip()
data['collectiondate'] = pd.to_datetime(
    {'year': pd.to_numeric(data['year.1'], errors='coerce'),
     'month': pd.to_numeric(data['month'], errors='coerce'), 
     'day': pd.to_numeric(data['day'], errors='coerce').fillna(1)},
    errors='coerce')
data = data.rename(columns = {'year.1': 'collectionyear'})

# Taxon Data
pattern = ['? To be checked', '?to be checked', '? to be checked', '?To be checked', '?',
           'n.sp', 'n. sp.', 'cf.', 'cf', 'aff.', 'aff', 'sp. nov', 'sp', 'nov', '.', 'spec',]

data['taxonname'] = data['taxonname'] = data['genus'].str.strip() + ' ' + data['species'].str.strip()
for pat in pattern:
    data['taxonname'] = data['taxonname'].str.replace(pat, '')
data['taxonname'] = data['taxonname'].str.replace('  ', ' ')

data['taxonname'] = data['taxonname'].str.replace('Cypris biinosa', 'Cypris bispinosa')
data['taxonname'] = data['taxonname'].str.replace('Strandesia inulosa', 'Strandesia spinulosa')
data['taxonname'] = data['taxonname'].str.replace('Candonocypris aezelandiae',	'Candonocypris novaezelandiae')
data['taxonname'] = data['taxonname'].str.replace('Fabaeformiscandona  balatonica', 'Fabaeformiscandona balatonica')
data['taxonname'] = data['taxonname'].str.replace('Mixtacandona andli', 'Mixtacandona spandli')
data['taxonname'] = data['taxonname'].str.replace('Pseudocandona preica', 'Pseudocandona prespica')
data['taxonname'] = data['taxonname'].str.replace('Vestalenula ecC', 'Vestalenula spec.C')

regex = r'^(\w+(?:\s+\w+)?)'
data['taxonname'] = data['taxonname'].astype(str).apply(lambda x:
                                                      re.match(regex, x).group(1)
                                                      if re.match(regex, x) else None)
data['count'] = 'presence/absence'
data['value'] = 1
data['variableelement'] = 'valve (undiff)'
data['context'] = 'live'

# Habitat Data
data['natural habitat'] = data['natural habitat'].fillna(data['artificial habitat'])

# Clean columns
data.drop(columns=['artificial habitat',
                   'londeg', 'lonmin', 'lonsec',
                   'latdeg', 'latmin', 'latsec',
                   'reference', 'node reference citations'], inplace=True)

data['natural habitat'] = data['natural habitat'].str.replace(r'lake', 'lacustrine', flags=re.IGNORECASE)
data = data.rename(columns={'natural habitat': 'habitat',
                            'name in reference': 'name in record',
                            'node full references': 'references'})

## PI and Collector
data['pi'] = None
for idx, row in authors.iterrows():
    title = row['citation']
    data.loc[data['references'].str.contains(title, na=False, regex=False), 'pi'] = row['authors']

# Add missing data
data['collection_type'] = 'modern'
data['age_model'] = 'Collection Date'
data['age_type'] = 'Calendar years BP'

# Record Data
data['record_number'] = 'NODE-R' + (
    data.groupby(['longitude', 'latitude', 'collectionunit_name', 'collectionyear'], dropna=False).ngroup().add(1).astype(str)
)
data['handle_complete'] = data['record_number']
columns = [
    # Site
    'record_number', 'sitename', 'site', 'longitude', 'latitude', 'altitude', 'depth', 
    # CU
    'handle_complete', 'collectionunit_name', 'habitat', 'water chemistry', 'collection_type',
    'substrate', 'vegetation', 'ph', 'temp', 'cond', 'environment', 'duration', 'collectiondate',
    # Chronologies
    'age', 'age_model', 'age_type', 'collectionyear',
    # Publications
    'references', 'year', 'published?',
    # GPU
    'country', 'region', 'locality', 'day', 'month',
    # Taxa
    'taxonname', 'count', 'value', 'variableelement',
    'context', 'name in record', 'genus', 'species', 'subspecies', 
    'no spec', 'males?', 'sex ratio', 
    # Contacts
    'pi', 'sample_analyst', 'dataset_processor',
    # Notes
    'zone of coll', 'comments', 'validation status of coordinates'
]
data = data[columns]
data.sort_values(by=['record_number'], inplace=True)
data.to_excel('data-all/original/NODE-cleanedAug2025.xlsx', index=False)

csv_splitter(data, params='record_number')
