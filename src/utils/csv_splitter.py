import pandas as pd
import os

# 'decimalLatitude', 'decimalLongitude' need to be merged into a Geog column
# to add 'siteID' if non existent.

## NODE might be a different splitter where I also make a new column for geog and I might have to group by geog

def csv_splitter(data, params=['Handle'], path = 'data-all/splitted'):
    split_files = path
    if not os.path.exists(split_files):
                os.makedirs(split_files)

    data_groups = data.groupby(by=params)

    for group, rows in data_groups:
        filename = f"{path}/{group[0]}.csv"
        rows.to_csv(filename, index=False)
    print("Finished")