from .pull_params import pull_params

def valid_chroncontrols(yml_dict, csv_file):
    """_Validating Chron Controls_"""
    response = {'valid': [],
                'message': []}
    
    params = ["depth", "thickness"]
    inputs = pull_params(params, yml_dict, csv_file, 'ndb.chroncontrols')

    params_age = ['age']
    inputs_age = pull_params(params_age, yml_dict, csv_file, 'ndb.sampleages')
    inputs_age['age'] = [float(value) if value != 'NA' else None for value in inputs_age['age']]
    inputs_age['uncertainty'] = [float(value) if value != 'NA' else None for value in inputs_age['uncertainty']]
    
    if len(inputs['depth']) == len(inputs_age['age']) == len(inputs['thickness']):
        response['message'].append(f"✔ The number of depths (analysis units), ages, and thicknesses are the same.")
        response['valid'].append(True)
    else:
        response['message'].append(f"✗ The number of depths (analysis units), ages, and thicknesses is not the same. Please check.")
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