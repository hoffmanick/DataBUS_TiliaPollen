import logging
from .pull_params import pull_params
import numpy as np

def insert_sample_age(cur, yml_dict, csv_template, uploader):
    """
    Inserts sample age data into a database.

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_template (str): File path to the CSV template.
        uploader (dict): Dictionary containing uploader details.

    Returns:
        results_dict (dict): A dictionary containing information about the inserted sample ages.
            - 'sampleAge' (list): List of IDs for the inserted sample age data.
            - 'valid' (bool): Indicates if all insertions were successful.
    """
    results_dict = {'sampleAge': [], 'valid': []}

    sample_age_query = """
                       SELECT ts.insertsampleage(_sampleid := %(sampleid)s, 
                                                 _chronologyid := %(chronologyid)s, 
                                                 _age := %(age)s, 
                                                 _ageyounger := %(ageyounger)s, 
                                                 _ageolder := %(ageolder)s)
                       """
    
    params = ['age']
    inputs = pull_params(params, yml_dict, csv_template, 'ndb.sampleages')

    inputs['age'] = [float(value) if value != 'NA' else None for value in inputs['age']]
    inputs['uncertainty'] = [float(value) if value != 'NA' else None for value in inputs['uncertainty']]

    for i in range(len(uploader['samples']['samples'])):
        
        #index = i % len(inputs['age'])
        try:
            age_younger = inputs['age'][i]-inputs['uncertainty'][i]
        except Exception as e:
            age_younger = None
        
        try:
            age_older = inputs['age'][i]+inputs['uncertainty'][i]
        except Exception as e:
            age_older = None

        try:
            cur.execute(sample_age_query, {'sampleid': uploader['samples']['samples'][i],
                                           'chronologyid': uploader['chronology']['chronology'],
                                           'age': inputs['age'][i],
                                           'ageyounger': age_younger, 
                                           'ageolder': age_older})
            
            result = cur.fetchone()[0]
            results_dict['sampleAge'].append(result)
            results_dict['valid'].append(True)

        except Exception as e:
            logging.error(f"Samples Age data is not correct. {e}")
            cur.execute(sample_age_query, {'sampleid': uploader['samples']['samples'][i],
                                           'chronologyid': uploader['chronology']['chronology'],
                                           'age': None,
                                           'ageyounger': None, 
                                           'ageolder': None})
            
            result = cur.fetchone()[0]
            results_dict['sampleAge'].append(result)
            results_dict['valid'].append(False)
    results_dict['valid'] = all(results_dict['valid'])
    return results_dict