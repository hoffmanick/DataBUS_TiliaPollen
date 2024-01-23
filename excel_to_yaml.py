import pandas as pd
import numpy as np
import yaml
import ast

df = pd.read_excel('GOOD_template_spreadsheet.xlsx')
df = df[df['Column']!="—NA—"]
df = df.replace({np.nan: None})

df.columns = map(str.lower, df.columns)

df['vocab'] = df['vocab'].str.replace("'", '"')
df['vocab'] = df['vocab'].str.replace("‘", '"')
df['vocab'] = df['vocab'].str.replace("’", '"')
#df['vocab'] = df['vocab'].apply(lambda x: ast.literal_eval(x) if x is not None else None)

data = df.groupby(['column', 'neotoma']).apply(lambda x: x.to_dict(orient='index')).to_dict()

data_list = []

for key, value in data.items():
    data_list.append(list(value.values())[0])

units_entries = list()
uncertainty_units_entries = list()
uncertainty_entries = list()
for entry in data_list:
    if entry['unitcolumn'] is None:
        del entry['unitcolumn']
    else:
        units_dict = {'column': entry['unitcolumn'],
                     'neotoma': 'ndb.variableunits.variableunits',
                     'required': False,
                     'rowwise': True,
                     'type': 'string',
                     'vocab': entry['vocab']}
        units_entries.append(units_dict)
    if entry['uncertaintycolumn'] is None:
        del entry['uncertaintycolumn']
        del entry['uncertaintybasis']
        del entry['uncertaintyunitcolumn']
    else:
        entry['uncertainty'] = {'uncertaintycolumn': entry['uncertaintycolumn'], 
                                'uncertaintybasis': entry['uncertaintybasis'], 
                                'unitcolumn': entry['uncertaintyunitcolumn']}
        
        uncertainty_dict = {'column': entry['uncertaintycolumn'],
                            'formatorrange': entry['formatorrange'],
                            'neotoma': 'ndb.values',
                            'required': False,
                            'rowwise': True,
                            'taxonid': entry['taxonid'],
                            'taxonname': entry['taxonname'],
                            'type': entry['type'],
                            'vocab': None}
        uncertainty_entries.append(uncertainty_dict)
        del entry['uncertaintycolumn']
        del entry['uncertaintybasis']
        uncertainty_unit_dict = {'column': entry['uncertaintyunitcolumn'],
                     'neotoma': 'ndb.values',
                     'notes': entry['notes'],
                     'required': False,
                     'rowwise': True,
                     'type': entry['type'],
                     'vocab': entry['vocab']}
        uncertainty_units_entries.append(uncertainty_unit_dict)
        del entry['uncertaintyunitcolumn']       
    if entry['vocab'] is None:
        del entry['vocab']
    if entry['formatorrange'] is None:
        del entry['formatorrange']
    if entry['constant'] is None:
        del entry['constant']
    if entry['taxonname'] is None:
        del entry['taxonname']
    if entry['taxonid'] is None:
        del entry['taxonid']
    if entry['notes'] is None:
        del entry['notes']



data_list = data_list + units_entries + uncertainty_entries + uncertainty_units_entries

data_list = sorted(data_list, key=lambda x: x['column'])

final_dict = {'apiVersion': 'neotoma v2.0',
              'headers': 2,
              'kind': 'Development',
              'databaseid': 37,
              'lab_number': 5,
              'metadata': data_list}

with open('template.yml', "w") as f:
    yaml.dump(final_dict, f)