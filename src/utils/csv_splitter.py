import pandas as pd
import os

# 'decimalLatitude', 'decimalLongitude' need to be merged into a Geog column
# to add 'siteID' if non existent.

## NODE might be a different splitter where I also make a new column for geog and I might have to group by geog

def csv_splitter(data, params=['Site name', 'Handle']):
    split_files = 'data-all/splitted'
    if not os.path.exists(split_files):
                os.makedirs(split_files)

    print(data.head())
    print(data.columns)
    data_groups = data.groupby(by=params)
    print(data_groups.head())

    for group, rows in data_groups:
        filename = f"data-all/splitted/{group[1]}.csv"
        rows.to_csv(filename, index=False)