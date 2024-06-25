import logging
import neotomaHelpers as nh

def insert_data_processor(cur, yml_dict, csv_template, uploader):
    """
    Inserts data processors into Neotoma

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_template (str): File path to the CSV template.
        uploader (dict): Dictionary containing uploader details.

    Returns:
        response (dict): A dictionary containing information about the inserted data processors.
            - 'processorid' (list): List of processors' IDs.
            - 'valid' (bool): Indicates if all insertions were successful.
    """
    response = {'processorid': list(), 'valid': list(), 'message': list()}
    params = ['contactid']
    inputs = nh.pull_params(params, yml_dict, csv_template, 'ndb.sampleanalysts')
    
    inputs['contactid'] = list(set(inputs['contactid']))
    contids = nh.get_contacts(cur, inputs['contactid'])
    processor = """SELECT ts.insertdataprocessor(_datasetid := %(datasetid)s, 
                                                 _contactid := %(contactid)s)"""
    
    for contact in contids:
        try:
            cur.execute(processor, {'datasetid': int(uploader['datasetid']['datasetid']), 
                                    'contactid': int(contact['id'])})
            response['valid'].append(True)
            response['message'].append(f"✔ Processor {contact['id']} inserted.")

        except Exception as e:
            logging.error(f"✗ Data processor information is not correct. {e}")
            response['message'].append("✗ Data processor information is not correct. {e}")
            cur.execute(processor, {'datasetid': int(uploader['datasetid']['datasetid']), 
                                    'contactid': None})
            response['valid'].append(False)
            response['message'].append("✗ Temporary insertion")
    response['dataset_pi_ids'] = contids
    response['valid'] = all(response['valid'])
    return response