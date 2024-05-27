import logging
import neotomaHelpers as nh
with open('./sqlHelpers/sample_age_query.sql', 'r') as sql_file:
    sample_age_query = sql_file.read()

def insert_sample_age(cur, yml_dict, csv_template, uploader):
    """
    Inserts sample age data into a database.

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_template (str): File path to the CSV template.
        uploader (dict): Dictionary containing uploader details.

    Returns:
        response (dict): A dictionary containing information about the inserted sample ages.
            - 'sampleAge' (list): List of IDs for the inserted sample age data.
            - 'valid' (bool): Indicates if all insertions were successful.
    """
    response = {'sampleAge': list(), 'valid': list(), 'message': list()}

    params = ['age']
    inputs = nh.pull_params(params, yml_dict, csv_template, 'ndb.sampleages')

    inputs['age'] = [float(value) if value != 'NA' else None for value in inputs['age']]
    inputs['uncertainty'] = [float(value) if value != 'NA' else None for value in inputs['uncertainty']]

    for i in range(len(uploader['samples']['samples'])):
        if isinstance(inputs['age'][i], (int, float)):
            age_younger = inputs['age'][i]-inputs['uncertainty'][i]
            age_older = inputs['age'][i]+inputs['uncertainty'][i]
        else:
            response['message'].append("? Age is set to None. Ageyounger/Ageolder will be None.")
            age_younger = None
            age_older = None
        try:
            cur.execute(sample_age_query, {'sampleid': int(uploader['samples']['samples'][i]),
                                           'chronologyid': int(uploader['chronology']['chronology']),
                                           'age': inputs['age'][i],
                                           'ageyounger': age_younger, 
                                           'ageolder': age_older})
            
            result = cur.fetchone()[0]
            response['sampleAge'].append(result)
            response['valid'].append(True)
            response['message'].append(f"✔ Adding sample age for sample {uploader['samples']['samples'][i]}")

        except Exception as e:
            logging.error(f"✗ Samples Age data is not correct. {e}")
            cur.execute(sample_age_query, {'sampleid': uploader['samples']['samples'][i],
                                           'chronologyid': uploader['chronology']['chronology'],
                                           'age': None,
                                           'ageyounger': None, 
                                           'ageolder': None})
            response['message'].append(f"✗  Sample Age is not correct. Executing temporary query.")
            result = cur.fetchone()[0]
            response['valid'].append(False)
    
    response['valid'] = all(response['valid'])
    return response