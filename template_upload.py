import json
import os
import psycopg2
import glob
from dotenv import load_dotenv
import neotomaHelpers as nh
import neotomaUploader as nu
from neotomaValidator.csv_validator import csv_validator
from neotomaValidator.check_file import check_file
from neotomaHelpers.logging_dict import logging_dict
"""
Use this command after having validated the files to 
upload to Neotoma.
To run, you can use: 
python template_upload.py

In that case, the default template 'template.yml' is used.

You can also use a different template file by running:
python template_upload.py --template='template_xlsx.xlsx'

Change 'template_xlsx.xlsx' to desired filename as long as 
template file that has an .xlsx or .yml extension
"""

load_dotenv()
data = json.loads(os.getenv('PGDB_LOCAL2'))

conn = psycopg2.connect(**data, connect_timeout = 5)

cur = conn.cursor()

args = nh.parse_arguments()
overwrite = args['overwrite']

filenames = glob.glob(args['data'] + "*.csv")
upload_logs = 'data/upload_logs'
if not os.path.exists(upload_logs):
            os.makedirs(upload_logs)

uploaded_files = "data/uploaded_files"

for filename in filenames:
    test_dict = {}
    print(filename)
    logfile = []
    hashcheck = nh.hash_file(filename)
    filecheck = check_file(filename)

    if hashcheck['pass'] is False and filecheck['pass'] is False:
        csv_template = nh.read_csv(filename)
        logfile.append("File must be properly validated before it can be uploaded.")
    else:
        csv_template = nh.read_csv(filename)
        # This possibly needs to be fixed. How do we know that there is one or more header rows?

    uploader = {}
 
    yml_dict = nh.template_to_dict(temp_file=args['template'])
    yml_data = yml_dict['metadata']

    # Verify that the CSV columns and the YML keys match
    csv_valid = csv_validator(filename = filename,
                                yml_data = yml_data)

    logfile.append('\n=== Inserting New Site ===')
    uploader['sites'] = nu.insert_site(cur = cur,
                                    yml_dict = yml_dict,
                                    csv_template = csv_template)
    logfile = logging_dict(uploader['sites'], logfile, 'sitelist')
    
    #     # logfile.append('=== Inserting Site Geopol ===')
    #     # uploader['geopolid'] = nu.insert_geopol(cur = cur,
    #     #                                        yml_dict = yml_dict,
    #     #                                        csv_template = csv_template,
    #     #                                        uploader = uploader)
    #     # logfile.append(f"Geopolitical Unit: {uploader['geopolid']}")

    logfile.append('\n === Inserting Collection Units ===')
    # Placeholders are present
    uploader['collunitid'] = nu.insert_collunit(cur = cur,
                                            yml_dict = yml_dict,
                                            csv_template = csv_template,
                                            uploader = uploader)
    logfile = logging_dict(uploader['collunitid'], logfile, "collunits['collunit']")
  
    logfile.append('\n=== Inserting Analysis Units ===')
    uploader['anunits'] = nu.insert_analysisunit(cur = cur,
                                                yml_dict = yml_dict,
                                                csv_template = csv_template,
                                                uploader = uploader)
    logfile = logging_dict(uploader['anunits'], logfile)

    logfile.append('\n=== Inserting Chronology ===')
    # Placeholders exist
    uploader['chronology'] = nu.insert_chronology(cur = cur,
                                                yml_dict = yml_dict,
                                                csv_template = csv_template,
                                                uploader = uploader)
    logfile = logging_dict(uploader['chronology'], logfile)
    
    logfile.append('\n=== Inserting Chroncontrol ===')
    uploader['chroncontrol'] = nu.insert_chron_control(cur = cur,
                                                    yml_dict = yml_dict,
                                                    csv_template = csv_template,
                                                    uploader = uploader)
    logfile = logging_dict(uploader['chroncontrol'], logfile)
    #logfile.append(f"chroncontrol: {uploader['chroncontrol']}")

    # logfile.append('\n=== Inserting Dataset ===')
    # # Placeholders exist
    # uploader['datasetid'] = nu.insert_dataset(cur = cur,
    #                                         yml_dict = yml_dict,
    #                                         csv_template = csv_template,
    #                                         uploader = uploader)
    # logfile.append(f"datasetid: {uploader['datasetid']}")

    # logfile.append('\n=== Inserting Dataset PI ===')
    # uploader['datasetpi'] = nu.insert_dataset_pi(cur = cur,
    #                                             yml_dict = yml_dict,
    #                                             csv_template = csv_template,
    #                                             uploader = uploader)
    # logfile.append(f"datasetPI: {uploader['datasetpi']}")
 
    # logfile.append('\n=== Inserting Data Processor ===')
    # uploader['processor'] = nu.insert_data_processor(cur = cur,
    #                                                 yml_dict = yml_dict,
    #                                                 csv_template = csv_template,
    #                                                 uploader = uploader)
    # logfile.append(f"dataset Processor: {uploader['processor']}")
 
    #     # Not sure where to get this information from
    #     # logfile.append('=== Inserting Repository ===')
    #     # uploader['repository'] = nu.insert_dataset_repository(cur = cur,
    #     #                                                     yml_dict = yml_dict,
    #     #                                                     csv_template = csv_template,
    #     #                                                     uploader = uploader)
    #     # logfile.append(f"dataset Processor: {uploader['repository']}")

    # logfile.append('\n=== Inserting Dataset Database ===')
    # uploader['database'] = nu.insert_dataset_database(cur = cur,
    #                                                 yml_dict = yml_dict,
    #                                                 uploader = uploader)
    # logfile.append(f"Dataset Database: {uploader['database']}")

    # logfile.append('\n=== Inserting Samples ===')
    # uploader['samples'] = nu.insert_sample(cur, 
    #                                     yml_dict = yml_dict,
    #                                     csv_template = csv_template,
    #                                     uploader = uploader)
    # logfile.append(f"Dataset Samples: {uploader['samples']}")

    # logfile.append('\n=== Inserting Sample Analyst ===')
    # uploader['sampleAnalyst'] = nu.insert_sample_analyst(cur, 
    #                                     yml_dict = yml_dict,
    #                                     csv_template = csv_template,
    #                                     uploader = uploader)
    # logfile.append(f"Sample Analyst: {uploader['sampleAnalyst']}")

    # logfile.append('\n === Inserting Sample Age ===')
    # uploader['sampleAge'] = nu.insert_sample_age(cur, 
    #                                     yml_dict = yml_dict,
    #                                     csv_template = csv_template,
    #                                     uploader = uploader)
    # logfile.append(f"Sample Age: {uploader['sampleAge']}")

    # logfile.append('\n === Inserting Data ===')
    # # TaxonID PlaceHolder
    # uploader['data'] = nu.insert_data(cur, 
    #                                 yml_dict = yml_dict,
    #                                 csv_template = csv_template,
    #                                 uploader = uploader)
    # logfile.append(f"Data: {uploader['data']}")

    modified_filename = filename.replace('data/', 'data/upload_logs/')
    with open(modified_filename + '.upload.log', 'w', encoding = "utf-8") as writer:
        for i in logfile:
            writer.write(i)
            writer.write('\n')
    
    all_true = all([uploader[key]['valid'] for key in uploader if 'valid' in uploader[key]])

    if all_true:
        print(f"{filename} was uploaded.\nMoved {filename} to the 'uploaded_files' folder.")
        #conn.commit()
        conn.rollback()
        #if not os.path.exists(uploaded_files):
        #    os.makedirs(uploaded_files)
        #uploaded_path = os.path.join(uploaded_files, os.path.basename(filename))
        #os.replace(filename, uploaded_files)
        

    else:
        print(f"filename {filename} could not be uploaded.")
        conn.rollback()