from .pull_params import pull_params

def valid_chronologies(yml_dict, csv_file):
    """_Inserting analysis units_"""
    response = {'valid': [],
                'message': []}
    params = ["contactid", "agemodel", "notes"]
    inputs = pull_params(params, yml_dict, csv_file, 'ndb.chronologies')

    params2 = ['age']
    inputs_age = pull_params(params2, yml_dict, csv_file, 'ndb.sampleages')
    inputs_age['age'] = [float(value) if value != 'NA' else None for value in inputs_age['age']]
    agetype = list(set(inputs_age['unitcolumn']))
    agetype = agetype[0]
    
    if agetype == 'cal yr BP':
        response['message'].append("✔ The provided age type is correct.")
        response['valid'].append(True)
    elif agetype == 'CE/BCE':
        response['message'].append("✔ The provided age type is correct.")
        response['valid'].append(True)
    else:
        response['message'].append("✗ The provided age type is incorrect..")
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