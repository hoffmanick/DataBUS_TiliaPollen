import logging
import neotomaHelpers as nh

def insert_dataset_pi(cur, yml_dict, csv_template, uploader):
    """
    Inserts dataset principal investigator data into Neotomas.

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_template (str): File path to the CSV template.
        uploader (dict): Dictionary containing uploader details.

    Returns:
        response (dict): A dictionary containing information about the inserted dataset principal investigators.
            - 'dataset_pi_ids' (list): List of dictionaries containing details of the contacts, including their IDs and order.
            - 'valid' (bool): Indicates if all insertions were successful.
    """
    response = {'dataset_pi_ids': list(), 'valid': list(), 'message': list()}
    params = ['contactid']
    inputs = nh.pull_params(params, yml_dict, csv_template, 'ndb.datasetpis')

    # Use this method to preserve order.
    inputs['contactid'] = list(dict.fromkeys(inputs['contactid']))
    contids = nh.get_contacts(cur, inputs['contactid'])
    pi_query = """SELECT ts.insertdatasetpi(_datasetid := %(datasetid)s, 
                                            _contactid := %(contid)s,
                                            _piorder := %(piorder)s);"""
    for contact in contids:
        try:
            cur.execute(pi_query, {'datasetid': int(uploader['datasetid']['datasetid']), 
                                   'contid': int(contact['id']),
                                   'piorder': int(contact['order'])})
            response['valid'].append(True)
            response['message'].append(f"✔ PI inserted")
        except Exception as e:
            logging.error(f"✗ DatasetPI is not correct. {e}")
            response['message'].append(f"✗ DatasetPI is not correct. {e}")
            cur.execute(pi_query, {'datasetid': int(uploader['datasetid']['datasetid']), 
                                   'contid': None,
                                   'piorder': None})
            response['valid'].append(False)
            response['message'].append(f"✗ Temporary insertion of PIs")

    response['dataset_pi_ids'] = contids
    response['valid'] = all(response['valid'])
    return response