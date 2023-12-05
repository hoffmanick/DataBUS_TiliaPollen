import logging
import numpy as np
from .pull_params import pull_params

def insert_dataset (cur, yml_dict, csv_template, uploader):
    """
    Inserts a dataset associated with a collection unit into a database.

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_template (str): File path to the CSV template.
        uploader (dict): Dictionary containing uploader details.

    Returns:
        results_dict (dict): A dictionary containing information about the inserted dataset.
            'datasetid' (int): IDs for the inserted dataset.
            'valid' (bool): Indicates if insertions were successful.
       
    """
    results_dict = {'datasetid': np.nan, 'valid': False}
    dataset_query = """SELECT ts.insertdataset(_collectionunitid:= %(collunitid)s,
                                               _datasettypeid := %(datasettypeid)s,
                                               _datasetname := %(datasetname)s);"""

    params = ['datasetname', 'datasettypeid']
    inputs = pull_params(params, yml_dict, csv_template, 'ndb.datasets')
    
    inputs = dict(map(lambda item: (item[0], None if all([i is None for i in item[1]]) else item[1]),
                      inputs.items()))
    try:
        cur.execute(dataset_query, {'collunitid': int(uploader['collunitid']['collunitid']),
                                    'datasettypeid': int(5), #inputs['datasettypeid'],
                                    'datasetname': inputs['datasetname']})
        results_dict['datasetid'] = cur.fetchone()[0]
        results_dict['valid'] = True
    
    except Exception as e:
        logging.error(f"Dataset Info is not correct. {e}")
        cur.execute(dataset_query, {'collunitid': int(uploader['collunitid']['collunitid']),
                                    'datasettypeid': np.nan,
                                    'datasetname': None})
        results_dict['datasetid'] = cur.fetchone()[0]
        results_dict['valid'] = False
    
    return results_dict
