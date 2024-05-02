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
from neotomaUploader.logging_dict import logging_dict

# Obtain arguments and parse them to handle command line arguments
args = nu.parse_arguments()
load_dotenv()
data = json.loads(os.getenv('PGDB_LOCAL2'))
conn = psycopg2.connect(**data, connect_timeout = 5)
cur = conn.cursor()

filenames = glob.glob(args['data'] + "*.csv")
valid_logs = 'data/validation_logs'
if not os.path.exists(valid_logs):
            os.makedirs(valid_logs)

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

        logfile.append('=== File Validation ===')
        validator['csvValid'] = nu.csv_validator(filename = filename,
                                   yml_data = yml_data)
        logfile = logging_dict(validator['csvValid'], logfile)

        logfile.append('\n === Validating Template Unit Definitions ===')
        df = pd.read_csv(filename)
        validator['units'] = nu.validUnits(df, vocab_)
        logfile = logging_dict(validator['units'], logfile)

        logfile.append('\n === Validating Sites ===')
        validator['sites'] = nu.valid_site(cur = cur,
                                 yml_dict = yml_dict,
                                 csv_file = csv_file)
        logfile = logging_dict(validator['sites'], logfile, 'sitelist')
        
        ########### Geopolitical unit:
        # logfile.append('=== Checking Against Geopolitical Units ===')
        # validator['geopol'] = nu.validGeoPol(cur = cur,
        #                            yml_dict = yml_dict,
        #                            csv_file = csv_file)
        # logfile.append(f"Geopol: {validator['geopol']}")

        logfile.append('\n === Checking Against Collection Units ===')
        validator['collunits'] = nu.valid_collectionunit(cur = cur,
                                                         yml_dict = yml_dict,
                                                         csv_file = csv_file)
        logfile = logging_dict(validator['collunits'], logfile, 'sitelist')

        # TODO: Validate Analysis Units
        logfile.append('\n === Checking Against Analysis Units ===')
        validator['analysisunit'] = nu.valid_analysisunit(yml_dict = yml_dict,
                                                          csv_file = csv_file)
        logfile = logging_dict(validator['analysisunit'], logfile)

        # TODO: Validate chronologies:
        logfile.append('\n === Checking Chronologies ===')
        validator['chronologies'] = nu.valid_chronologies(yml_dict = yml_dict,
                                                          csv_file = csv_file)
        logfile = logging_dict(validator['chronologies'], logfile)

        # TODO: Validate chroncontrols
        logfile.append('\n === Checking Chron Controls ===')
        validator['chron_controls'] = nu.valid_chroncontrols(yml_dict = yml_dict,
                                                          csv_file = csv_file)
        logfile = logging_dict(validator['chron_controls'], logfile)

        # TODO: Validate dataset
        logfile.append('\n === Checking Dataset ===')
        validator['dataset'] = nu.valid_dataset(cur = cur,
                                                yml_dict = yml_dict,
                                                csv_file = csv_file)
        logfile = logging_dict(validator['dataset'], logfile)

        ########### PI names:
        logfile.append('\n === Checking Against Contact Names ===')
        validator['agent'] = nu.valid_agent(cur,
                                            csv_file,
                                            yml_dict)
        logfile = logging_dict(validator['agent'], logfile)

        ########### Make sure the dating horizon is in the analysis units:
        logfile.append('\n === Checking the Dating Horizon is Valid ===')
        validator['horizoncheck'] = nu.valid_horizon(yml_dict,
                                                     csv_file)
        logfile = logging_dict(validator['horizoncheck'], logfile)

        logfile.append('\n === Validating Taxa Names ===')
        validator['taxa'] = nu.valid_taxa(cur,
                                          csv_file,
                                          yml_dict)
        logfile = logging_dict(validator['taxa'], logfile)
     
        ########### Write to log.
        modified_filename = filename.replace('data/', 'data/validation_logs/')
        with open(modified_filename + '.valid.log', 'w', encoding = "utf-8") as writer:
            for i in logfile:
                writer.write(i)
                writer.write('\n')