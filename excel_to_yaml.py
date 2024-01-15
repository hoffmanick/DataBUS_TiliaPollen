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
    if entry['uncertaintycolumn'] is None:
        del entry['uncertaintycolumn']
        del entry['uncertaintybasis']
    else:
        entry['uncertainty'] = {'uncertaintycolumn': entry['uncertaintycolumn'], 
                                'uncertaintybasis': entry['uncertaintybasis'], 
                                'unitcolumn': entry['unitcolumn']}

with open('new_template.yaml', "w") as f:
    yaml.dump(data_list, f)