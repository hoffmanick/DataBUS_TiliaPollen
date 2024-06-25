import logging
import neotomaHelpers as nh
with open('./sqlHelpers/dataset_query.sql', 'r') as sql_file:
    dataset_query = sql_file.read()

def insert_dataset(cur, yml_dict, csv_template, uploader):
    """
    Inserts a dataset associated with a collection unit into a database.

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_template (str): File path to the CSV template.
        uploader (dict): Dictionary containing uploader details.

    Returns:
        response (dict): A dictionary containing information about the inserted dataset.
            'datasetid' (int): IDs for the inserted dataset.
            'valid' (bool): Indicates if insertions were successful.
    """
    response = {'datasetid': None, 'valid': list(), 'message': list()}

    #params = ['datasetname', 'datasettypeid']
    #inputs = nh.pull_params(params, yml_dict, csv_template, 'ndb.datasets')
    #inputs = dict(map(lambda item: (item[0], None if all([i is None for i in item[1]]) else item[1]),
    #                  inputs.items()))
    inputs = dict()
    ds_name = nh.retrieve_dict(yml_dict, 'ndb.datasets.datasetname')
    inputs['datasetname'] = ds_name[0]['value']
    ds_id = nh.retrieve_dict(yml_dict, 'ndb.datasettypes.datasettypeid')
    inputs['datasettype'] = ds_id[0]['value']

   # inputs['datasettype'] = inputs['datasettypeid']['value'] # Placeholder! Where in the template should this go?
    query = "SELECT datasettypeid FROM ndb.datasettypes WHERE LOWER(datasettype) = %(ds_type)s"
    cur.execute(query,{'ds_type': inputs['datasettype'].lower()})
    inputs['datasettypeid'] = cur.fetchone()[0]
    if inputs['datasettypeid'] == None:
        response['valid'].append(False)
        response['message'].append("✗ Dataset Type ID is required.")
    else:
        id_q = """SELECT datasettype from ndb.datasettypes
                  WHERE datasettypeid = %(datasettypeid)s"""
        cur.execute(id_q, {'datasettypeid': int(inputs['datasettypeid'])})
        datasettype = cur.fetchone()
        if len(datasettype) == 0:
            response['message'].append(f"✗ No datasettype found for {inputs['datasettypeid']}.")
            response['valid'].append(False)
        else:
            response['message'].append(f"✔ Datasettype is: {datasettype[0]}.")

    try:
        inputs_dict = {'collunitid': int(uploader['collunitid']['collunitid']),
                       'datasettypeid': inputs['datasettypeid'],
                       'datasetname': inputs['datasetname']}
        cur.execute(dataset_query, inputs_dict)
        response['datasetid'] = cur.fetchone()[0]
        response['valid'].append(True)
        response['message'].append(f"✔ Dataset {response['datasetid']} added to Neotoma.")
    
    except Exception as e:
        logging.error(f"✗ Dataset Info is not correct. {e}")
        cur.execute(dataset_query, {'collunitid': int(uploader['collunitid']['collunitid']),
                                    'datasettypeid': None,
                                    'datasetname': None})
        response['datasetid'] = cur.fetchone()[0]
        response['valid'].append(False)
        response['message'].append("✗ Dataset Info is not correct. Creating temporary Dataset info.")
    response['valid'] = all(response['valid'])
    return response