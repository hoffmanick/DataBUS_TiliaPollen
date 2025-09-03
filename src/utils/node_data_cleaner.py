import pandas as pd
import re
from csv_splitter import csv_splitter

data = pd.read_excel('data-all/original/NODE_03Sept2025.xlsx')
references = pd.read_csv('data-all/original/NODE_reference_list.tsv',
                         sep='\t', 
                         usecols=['NODE FULL REFERENCES', 'NODE REFERENCE CITATIONS'])
data = data.merge(references, left_on='REFERENCE', right_on='NODE REFERENCE CITATIONS', how='left') 
data.columns = data.columns.str.lower()

# PIs and Collectors
data['node full references'] = data['node full references'].str.replace(r'(?<=\b)[Il](?=\d)', "1", regex=True)
data['pi'] = data['node full references'].str.extract(r'^(.*?)(?:\s+\d{4}|\s+unpublished data)',
                                                           flags=re.IGNORECASE)
data['pi'] = data['pi'].str.strip().str.replace(r'\s+', ' ', regex=True)

data['pi'] = data['pi'].str.replace(r'&', '|', regex=True)
data['pi'] = data['pi'].str.replace(r'\.,', '. |', regex=True)
data['pi'] = data['pi'].str.replace(r'(\.)\s*,\s*(?=[A-Z])', r'\1| ', regex=True)
data['pi'] = data['pi'].str.replace(r'\s+et al\.?$', '', regex=True)
data['pi'] = data['pi'].str.replace(r'\s+and\s+', '|', regex=True)
data['pi'] = data['pi'].str.strip()

# Site Data
#data['sitename'] = data['site'].fillna(data['locality'])
data['longitude'] = round(data['londeg'] + data['lonmin'] / 60 + data['lonsec'] / 3600, 6)
data['latitude'] = round(data['latdeg'] + data['latmin'] / 60 + data['latsec'] / 3600, 6)
# def fill_site(x):
#     non_null = x.dropna()
#     if not non_null.empty:
#         most_common = non_null.value_counts().idxmax()
#         return pd.Series([most_common] * len(x), index=x.index)
#     else:
#         return x
data['sitename'] = data['locality'].fillna('') + ' ' + data['site'].fillna('')
#mask = data['sitename'].str.contains('unspecified|no_precise_loc', case=False, na=False)
#data.loc[mask, 'sitename'] = data.loc[mask, 'region'].fillna(data.loc[mask, 'country'])

data['sitename'] = data['sitename'].str.strip()
#data['sitename'] = data.groupby(['longitude', 'latitude'])['sitename'].transform(fill_site)
data = data.assign(**{'sample_analyst': 'Horne, David',
                      'dataset_processor': 'Horne, David'})

# CU
#data['collectionunit_name'] = data['locality'].fillna('') + ' ' + data['site'].fillna('')

data = data.rename(columns = {'year': 'taxonname_year',
                              'field23': 'year'})
data['day'] = data['day'].fillna(1)
data['collectiondate'] = pd.to_datetime(data[["year", "month", "day"]], errors='coerce').dt.date
data = data.rename(columns={'year': 'collectionyear'})

# Record Data
data['record_number'] = 'NODE-R' + (
    data.groupby(['longitude', 'latitude', 'sitename', 'collectionyear'], dropna=False).ngroup().add(1).astype(str)
)
data['handle_complete'] = data['record_number']

# Taxon Data

data['taxonname'] = data['taxonname'] = data['genus'].str.strip() + ' ' + data['species'].str.strip()
data['taxonname'] = data['taxonname'].str.replace('  ', ' ')

regex = r'^(\w+(?:\s+\w+)?)'
data['taxonname'] = data['taxonname'].astype(str).str.extract(regex)

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
                            'name in ref': 'name_in_publication',
                            'node full references': 'references',
                            'age': 'age_of_waterbody'})

# Add missing data
data['collection_type'] = 'modern'
data['age_model'] = 'Collection Date'
data['age_type'] = 'Calendar years BP'

columns = [
    # Site
    'record_number', 'sitename', 'site', 'longitude', 'latitude', 'altitude', 'depth', 
    # CU
    'handle_complete', 'habitat', 'water chemistry', 'collection_type',
    'substrate', 'vegetation', 'ph', 'temp', 'cond', 'environment', 'duration', 'collectiondate',
    # Chronologies
    'age_of_waterbody', 'age_model', 'age_type', 'collectionyear',
    # Publications
    'references', 'published?',
    # GPU
    'country', 'region', 'locality', 'day', 'month',
    # Taxa
    'taxonname','taxonname_year', 'count', 'value', 'variableelement',
    'context', 'name_in_publication', 'genus', 'species', 'subspecies', 
    'no spec', 'males?', 'sex ratio', 
    # Contacts
    'pi', 'sample_analyst', 'dataset_processor',
    # Notes
    'zone of coll', 'comments', 'combo403'
]
data = data[columns]
data.sort_values(by=['record_number'], inplace=True)
data.to_excel('data-all/original/NODE-cleanedAug2025.xlsx', index=False)

#csv_splitter(data, params='record_number')