import logging
import neotomaHelpers as nh
import datetime
with open('./sqlHelpers/sample_query.sql', 'r') as sql_file:
    sample_query = sql_file.read()

def insert_sample(cur, yml_dict, csv_template, uploader):
    """
    Inserts sample data into Neotoma.

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_template (str): File path to the CSV template.
        uploader (dict): Dictionary containing uploader details.

    Returns:
        response (dict): A dictionary containing information about the inserted samples.
            - 'samples' (list): List of sample IDs inserted into the database.
            - 'valid' (bool): Indicates if all insertions were successful.
    """
    response = {'samples': list(), 'valid': list(), 'message': list()}
    params = ['sampledate', 'analysisdate', 'prepmethod', 
              'notes', 'taxonname', 'samplename']       
    inputs = nh.pull_params(params, yml_dict, csv_template, 'ndb.samples')
    inputs = dict(map(lambda item: (item[0], None if all([i is None for i in item[1]]) else item[1]),
                      inputs.items()))
    inputs['labnumber'] = nh.retrieve_dict(yml_dict, 'ndb.samples.labnumber')
    inputs['labnumber'] = inputs['labnumber'][0]['value']
    for j in range(len(uploader['anunits']['anunits'])):
        get_taxonid = """SELECT * FROM ndb.taxa WHERE taxonname %% %(taxonname)s;"""
        cur.execute(get_taxonid, {'taxonname': inputs['taxonname']})
        taxonid = cur.fetchone()
        if taxonid != None:
            taxonid = int(taxonid[0])
        else:
            taxonid = None

        try:    
            inputs_dict = {'analysisunitid': int(uploader['anunits']['anunits'][j]),
                           'datasetid': int(uploader['datasetid']['datasetid']),
                           'samplename': inputs['samplename'],
                           'sampledate': inputs['sampledate'],
                           'analysisdate': inputs['analysisdate'],
                           'taxonid': taxonid,
                           'labnumber': inputs['labnumber'],
                           'prepmethod': inputs['prepmethod'],
                           'notes': inputs['notes']}
            cur.execute(sample_query, inputs_dict)
            sampleid = cur.fetchone()[0]
            response['samples'].append(sampleid)
            response['valid'].append(True)
            response['message'].append(f"✔  Adding Sample {sampleid}.")

        except Exception as e:
            logging.error(f"✗ Samples data is not correct. {e}")
            response['message'].append(f"✗ Samples data is not correct. {e}")
            cur.execute(sample_query, {'analysisunitid': int(uploader['anunits']['anunits'][j]),
                                    'datasetid': int(uploader['datasetid']['datasetid']),
                                    'samplename': None,
                                    'sampledate': datetime.datetime.today().date(),
                                    'analysisdate': datetime.datetime.today().date(),
                                    'taxonid': None,
                                    'labnumber': None,
                                    'prepmethod': None,
                                    'notes': None})
            sampleid = cur.fetchone()[0]
            response['samples'].append(sampleid)
            response['valid'].append(False)
            response['message'].append(f"Temporary sample ID {sampleid} created.")

    assert len(uploader['anunits']['anunits']) == len(response['samples']), "Analysis Units and Samples do not have same length."
    response['valid'] = all(response['valid'])
    return response