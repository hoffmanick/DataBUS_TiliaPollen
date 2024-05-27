import neotomaHelpers as nh
import numpy as np
with open('./sqlHelpers/repository_query.sql', 'r') as sql_file:
    repo_query = sql_file.read()

def insert_data_repository(cur, yml_dict, csv_template, uploader):
    """
    """
    response = {'repoid': None, 'valid': list(), 'message': list()}
    params = ['acronym', 'repo']
    print(params)
    inputs = nh.pull_params(params, yml_dict, csv_template, 'ndb.repository')
    print(inputs)
    response['valid'].append(True)
    print(response)
    return response