import pandas as pd
from parse_text import parse_text
from csv_splitter import csv_splitter
import re

original = pd.read_excel('data-all/original/NODE database 22May2024.xls')
print(original)
fix = original.assign(**{'Sample Analyst': 'Horne, David',
                   'Dataset Processor': 'Horne, David'})

fix['Record Number'] = 'NODE-R' + (fix.groupby('LOCALITY').ngroup() + 1).astype(str)

fix['handleComplete'] = fix['Record Number'].astype(str).apply(lambda x: f"NODE/{x}")
regex = r'^(\w+\s*\w+)'
fix['Taxonname'] = fix['NAME IN REFERENCE'].str.replace('? To be checked', ''
                                                   ).str.replace('? ', ''
                                                   ).str.replace('?', '')
fix['Taxonname'] = fix['Taxonname'].astype(str).apply(lambda x:
                                                      re.match(regex, x).group(1)
                                                      if re.match(regex, x) else None)

fix['habitat'] = fix['NATURAL HABITAT']
fix['habitat'] = fix['habitat'].fillna(fix['ARTIFICIAL HABITAT'])

fix['habitat'] = fix['habitat'].str.replace('lake', 'lacustrine')
fix['habitat'] = fix['habitat'].str.replace('LAKE', 'lacustrine')

fix['CollectionType'] = "modern"
fix['Age Model'] = "Collection Date"
fix['Age Type'] = 'Calendar years BP'

fix = fix.rename(columns={'NAME IN REFERENCE': 'Name In Publication'})
# inconsistencies
#name_inconsistencies = pd.read_csv('data-all/EANODE/inconsistencies/contact_inconsistencies.csv')
#taxa_inconsisntecies = pd.read_csv('data-all/EANODE/inconsistencies/taxa_inconsistencies.csv')
#pub_inconsistencies = pd.read_csv('data-all/EANODE/inconsistencies/publication_inconsistencies.csv')

# dfs = [name_inconsistencies, taxa_inconsisntecies, pub_inconsistencies]

# for df in dfs:
#     mapping_dict = dict(zip(df.iloc[:, 0], df.iloc[:, 1]))
#     columns_to_replace = ["Taxonname", "recordedBy", "bibliographicCitation"]
#     fix[columns_to_replace] = fix[columns_to_replace].replace(mapping_dict)


fix.to_excel('data-all/original/NODE database 22May2024-fixed.xlsx', index=False)

csv_splitter(fix, params=['Record Number'])