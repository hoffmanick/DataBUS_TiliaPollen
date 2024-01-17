import pandas as pd
import numpy as np
import yaml

df = pd.read_excel('GOOD_template_spreadsheet.xlsx')
print(df.head())

df = df[df['Column']!="—NA—"]
df = df.replace({np.nan: None})

df.columns = map(str.lower, df.columns)

data = df.groupby('column').apply(lambda x: x.to_dict(orient='index')).to_dict()

data_list = []

for key, value in data.items():
    data_list.append(list(value.values())[0])

print(data_list[0])

units_entries = list()
uncertainty_entries = list()
for entry in data_list:
    if entry['notes'] is None:
        del entry['notes']
    if entry['formatorrange'] is None:
        del entry['formatorrange']
    if entry['vocab'] is None:
        del entry['vocab']
    if entry['constant'] is None:
        del entry['constant']
    if entry['taxonname'] is None:
        del entry['taxonname']
    if entry['taxonid'] is None:
        del entry['taxonid']
    if entry['units'] is None:
        del entry['units']
    if entry['unitcolumn'] is None:
        del entry['unitcolumn']   
    else:
        unit_dict = {'column': entry['unitcolumn'],
                     'neotoma': 'ndb.variableunits.variableunits',
                     'notes': entry['notes'],
                     'required': False,
                     'rowwise': True,
                     'type': entry['type'],
                     'vocab': entry['vocab']}
        units_entries.append(unit_dict)   
    if entry['uncertaintycolumn'] is None:
        del entry['uncertaintycolumn']
        del entry['uncertaintybasis']
    else:
        entry['uncertainty'] = {'uncertaintycolumn': entry['uncertaintycolumn'], 
                                'uncertaintybasis': entry['uncertaintybasis'], 
                                'unitcolumn': entry['unitcolumn']}
        uncertainty_dict = {'column': entry['uncertaintycolumn'],
                            'formatorrange': entry['formatorrange'],
                            'neotoma': 'ndb.variableunits.variableunits',
                            'required': False,
                            'rowwise': True,
                            'taxonid': entry['taxonid'],
                            'taxonname': entry['taxonname'],
                            'type': entry['type'],
                            'units': entry['units']}
        uncertainty_entries.append(uncertainty_dict)

data_list = data_list + units_entries + uncertainty_entries

final_dict = {'apiVersion': 'neotoma v2.0',
              'headers': 2,
              'kind': 'Development',
              'databaseid': 37,
              'lab_number': 5,
              'metadata': data_list}

with open('new_template.yaml', "w") as f:
    yaml.dump(final_dict, f)