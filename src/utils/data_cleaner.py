import pandas as pd
from parse_text import parse_text
from csv_splitter import csv_splitter
import re

original = pd.read_excel('data-all/original/EANOD published data June 2024.xlsx')
fix = original.assign(**{'Sample Analyst': 'Smith Robin J.',
                   'Dataset Processor': 'Simth Robin J.'})
fix.columns = [col.replace('*', '') for col in fix.columns]
fix['handleComplete'] = fix['Handle'].astype(str).apply(lambda x: f"EANOD/{x}/OST")
regex = r'^(\w+\s*\w+)'
fix['Taxonname'] = fix['scientificName'].astype(str).apply(lambda x: re.match(regex, x).group(1))
fix['habitat'] = fix['habitat'].str.replace('lake', 'lacustrine')

# inconsistencies
name_inconsistencies = pd.read_csv('data-all/EANODE/inconsistencies/contact_inconsistencies.csv')
taxa_inconsisntecies = pd.read_csv('data-all/EANODE/inconsistencies/taxa_inconsistencies.csv')
pub_inconsistencies = pd.read_csv('data-all/EANODE/inconsistencies/publication_inconsistencies.csv')

dfs = [name_inconsistencies, taxa_inconsisntecies, pub_inconsistencies]

for df in dfs:
    mapping_dict = dict(zip(df.iloc[:, 0], df.iloc[:, 1]))
    columns_to_replace = ["Taxonname", "recordedBy", "bibliographicCitation"]
    fix[columns_to_replace] = fix[columns_to_replace].replace(mapping_dict)

expanded_rows = []
for _, row in fix.iterrows():
    parsed = parse_text(row['males and females text'])
    for entry in parsed:
        new_row = row.to_dict()
        new_row.update(entry)
        expanded_rows.append(new_row)

fix = pd.DataFrame(expanded_rows)

csv_splitter(fix, params=['Handle'])