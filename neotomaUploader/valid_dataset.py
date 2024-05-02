from .pull_params import pull_params

def valid_dataset(cur, yml_dict, csv_file):
    """_Validating Datasets_"""
    response = {'valid': [],
                'message': []}
    
    params = ['datasetname', 'datasettypeid']
    inputs = pull_params(params, yml_dict, csv_file, 'ndb.datasets')
    
    query = "SELECT DISTINCT datasettypeid FROM ndb.datasettypes"

    cur.execute(query)
    all_datasets = cur.fetchall()
    all_datasets = [value[0] for value in all_datasets]

    if inputs['datasettypeid'] in all_datasets:
        response['message'].append("✔ Dataset type exists in neotoma.")
        response['valid'].append(True)
    else:
        response['message'].append(f"✗ Dataset type is not known to neotoma. Add it first")
        response['valid'].append(False)
    
    for k in inputs:
        if len(inputs[k]) == 0:
            response['message'].append(f'? {k} has no values.')
            response['valid'].append(False)
        else:
            response['message'].append(f'✔ {k} looks valid.')
            response['valid'].append(True)

    response['valid'] = all(response['valid'])
    
    return response