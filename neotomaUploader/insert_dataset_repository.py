import neotomaHelpers as nh
import psycopg2
from datetime import datetime
with open('./sqlHelpers/repository_query.sql', 'r') as sql_file:
    repo_query = sql_file.read()

def insert_dataset_repository(cur, yml_dict, csv_template, uploader):
    """
    """
    response = {'repoid': None, 'valid': list(), 'message': list()}
    params = ['acronym', 'repo', 'recdatecreated', 'recdatemodified', 'notes']
    inputs = nh.pull_params(params, yml_dict, csv_template, 'ndb.repository')
    inputs = dict(map(lambda item: (item[0], None if all([i is None for i in item[1]]) else item[1]),
                      inputs.items()))
    for i in inputs:
        if i == None:
            response['message'].append(f"? {i} is None.")

    if inputs['repo'] == None:
        response['valid'].append(True)
        response['message'].append(f"✔ No repository information to be added.")

    else:
        response['message'].append(f"? Reposiory {inputs['repoid']} given.")
        query = """
                SELECT repository from ndb.repositoryinstitutions 
                WHERE repositoryid = %(repoid)s"""
        try:
            cur.execute(query, {'repoid': inputs['repoid']})
            repo_name = cur.fetchone()
            if len(repo_name) == 1:
                response['message'].append(f"✔ Repository found: {repo_name[0]}")
                response['valid'].append(True)
            else:
                response['message'].append("✗ Repo not found. Make sure Repo exists in ndb.repoinstitutions")
                response['valid'].append(False)
        except Exception as e:
            response['message'].append(f"✗ Error in query {e} or repository not found.")
            response['valid'].append(False)

        query = """
                SELECT datasetid, repositoryid 
                FROM ndb.repositoryspecimens
                WHERE repositoryid = %(repoid)s 
                AND datasetid = %(datasetid)s;
                """
        try:
            cur.execute(query, {'repoid': int(inputs['repoid']),
                                'datasetid': int(uploader['datasetid']['datasetid'])})
            info = cur.fetchall()
        except psycopg2.Error as e:
            response['message'].append(f"✗ Database error: {e}")

        if len(info) == 0: 
            response['message'].append("✗ (repositoryid, datasetid) already exist in ndb.repositoryspecimens table. \nTable will not be updated.")
            response['valid'].append(False)
        
        elif len(info) == 1:
            cur.execute(repo_query)
            up_query =  """
            SELECT insertrepositoryspecimen(_datasetid := %(datasetid)s, 
                                            _repositoryid := %(repositoryid)s,
                                            _notes := %(notes)s,
                                            _recdatecreated := %(recdatecreated)s,
                                            _recdatemodified := %(recdatemodified)s)
                                            """
            cur.execute(up_query, {'datasetid': uploader['datasetid']['datasetid'],
                                   'repositoryid': inputs['repoid'],
                                   'notes': inputs['notes'],
                                   'recdatecreated': datetime.now().date(),
                                   'recdatemodified': inputs['recdatemodified']})
            response['message'].append(f"✔ Inserting new (repositoryid, datasetid) row.")
            response['valid'].append(True)
    
    response['valid'] = all(response['valid'])
    return response