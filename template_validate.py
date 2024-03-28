"""_Validate 210Pb csv Files_
   Assumes there is a `data` folder from which the python script is run.
   The script obtains all `csv` files in ./data and then reads through
   each of them, validating each field to ensure they are acceptable for
   valid upload.
"""

import glob
import json
import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv
import neotomaUploader as nu

# Obtain arguments and parse them to handle command line arguments
args = nu.parse_arguments()
load_dotenv()
data = json.loads(os.getenv('PGDB_TANK'))
conn = psycopg2.connect(**data, connect_timeout = 5)
cur = conn.cursor()

filenames = glob.glob(args['data'] + "*.csv")

for filename in filenames:
    print(filename)
    logfile = []

    hashcheck = nu.hash_file(filename)
    filecheck = nu.check_file(filename)
    logfile = logfile + hashcheck['message'] + filecheck['message']

    if hashcheck['pass'] and filecheck['pass']:
        print("  - File is correct and hasn't changed since last validation.")
    else:
        # Load the yml template as a dictionary
        yml_dict = nu.template_to_dict(temp_file=args['template'])
        yml_data = yml_dict['metadata']
        validator = {}
        csv_file = nu.read_csv(filename)
        
        # Get the unitcols and units to be used
        # Check that the vocab in the template matches the csv vcocab
        vocab_ = nu.vocabDict(yml_data)

        logfile.append('=== Validating File ===')
        csvValid = nu.csv_validator(filename = filename,
                                   yml_data = yml_data)
        logfile.append(csvValid)

        logfile.append('=== Checking Template Unit Definitions ===')
        df = pd.read_csv(filename)
        validator['units'] = nu.validUnits(df, vocab_)
        logfile.append(f"units: {validator['units']}")

        logfile.append('=== Checking Against Current Sites ===')
        validator['sites'] = nu.valid_site(cur = cur,
                                 yml_dict = yml_dict,
                                 csv_file = csv_file)
        logfile.append(f"units: {validator['sites']}")
        print(validator['sites'])
        break
        ########### Collection Date
        # colldate
        logfile.append('=== Checking All Date Formats ===')
        # format is retrieved in validDate via the yml
        dateCheck = nu.valid_date(yml_dict,
                                csv_file)
        logfile = logfile + dateCheck['message']
        validator['date'] = dateCheck['pass']

        ########### Collection Units
        logfile.append('=== Checking Against Collection Units ===')
        nameCheck = nu.valid_collectionunit(cur,
                                    yml_dict,
                                    csv_file)
        logfile = logfile + nameCheck['message']
        validator['colunits'] = nameCheck['pass']
        
        ########### Geopolitical unit:
        #logfile.append('=== Checking Against Geopolitical Units ===')
        # Commenting for now so that I can run the script
        # namecheck = nu.validGeoPol(cur, geog, coords)
        #logfile = logfile + namecheck['message']
        #validator['geopol'] = namecheck['pass']

        ########### PI names:
        logfile.append('=== Checking Against Contact Names ===')
        namecheck = nu.valid_agent(cur,
                                  csv_file,
                                  yml_dict)
        logfile = logfile + namecheck['message']

        ########### Make sure the dating horizon is in the analysis units:
        logfile.append('=== Checking the Dating Horizon is Valid ===')
        horizoncheck = nu.valid_horizon(yml_dict,
                                       csv_file)
        validator['datinghorizon'] = horizoncheck['pass']
        logfile = logfile + horizoncheck['message']

        ########### Taxa names:
        logfile.append('=== Checking Against Taxa Names ===')
        namecheck = nu.valid_taxa(cur,
                                  csv_file,
                                  yml_dict)
        logfile = logfile + namecheck['message']

        ########### Write to log.
        with open(filename + '.log', 'w', encoding = "utf-8") as writer:
            for i in logfile:
                writer.write(i)
                writer.write('\n')