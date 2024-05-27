import logging
import neotomaHelpers as nh
with open('./sqlHelpers/chron_control_query.sql', 'r') as sql_file:
    addcontrol = sql_file.read()

def insert_chron_control(cur, yml_dict, csv_template, uploader):
    """
    Inserts chronological control data into a database.

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_template (str): File path to the CSV template.
        uploader (dict): Dictionary containing uploader details.

    Returns:
        response (dict): A dictionary containing information about the inserted chronological control units.
            'chron_control_units' (list): List of IDs for the inserted chronological control units.
            'valid' (bool): Indicates if all insertions were successful.

    Raises:
        AssertionError: If the number of analysis units, ages, and thicknesses are not consistent.
    """
    response = {'chron_control_units': list(), 'valid': list(), 'message': list()}
    
    params = ["depth", "thickness", 'notes',]
    inputs = nh.pull_params(params, yml_dict, csv_template, 'ndb.chroncontrols')
    inputs = {k: (v if v else None) for k, v in inputs.items()}
    if isinstance(inputs['notes'] ,list):
        inputs['notes'] = [None if x is None else x for x in inputs['notes']]
    elif inputs['notes'] == None:
        inputs['notes'] = [None] * len(inputs['depth'])

    # In template.xls I have ndb.geochroncontrols.age
    params_age = ['age']
    inputs_age = nh.pull_params(params_age, yml_dict, csv_template, 'ndb.sampleages')

    inputs_age['age'] = [float(value) if value != 'NA' else None for value in inputs_age['age']]
    inputs_age['uncertainty'] = [float(value) if value != 'NA' else None for value in inputs_age['uncertainty']]
    agetype = list(set(inputs_age['unitcolumn']))
    agetype = agetype[0]
    
    assert len(uploader['anunits']['anunits']) == len(inputs_age['age']) == len(inputs['thickness']), \
           "The number of analysis units, ages, and thicknesses is not the same. Please check."

    for i in range(len(uploader['anunits']['anunits'])):
        if inputs_age['unitcolumn'][i] == 'cal yr BP':
            agetypeid = 2
        elif inputs_age['unitcolumn'][i] == 'CE/BCE':
            agetypeid = 1
        else:
            logging.error("The provided age type is incorrect.")
            response['message'].append("The provided age type is incorrect.")
        
        if isinstance(inputs_age['age'][i], (int, float)):
            ageyoung = inputs_age['age'][i] + inputs_age['uncertainty'][i]
            ageold = inputs_age['age'][i] -  inputs_age['uncertainty'][i]
        else:
            ageyoung = None
            ageold = None

        try:
            cur.execute(addcontrol, {'chronid': int(uploader['chronology']['chronology']), #There is only one chronology
                                    'annuid': int(uploader['anunits']['anunits'][i]),
                                    'depth': inputs['depth'][i],
                                    'thickness': inputs['thickness'][i],
                                    'agetypeid': agetypeid,
                                    'age': inputs_age['age'][i],
                                    'notes':inputs['notes'][i],
                                    'ageyoung': ageyoung,
                                    'ageold': ageold})
            chron_control = cur.fetchone()[0]
            response['chron_control_units'].append(chron_control)
            response['valid'].append(True)
            response['message'].append(f"✔ Adding Chron Control {chron_control}.")

        except Exception as e:
            logging.error(f"Chron Control Data is not correct. Error message: {e}")
            cur.execute(addcontrol, {'chronid': int(uploader['chronology']['chronology']),
                                    'annuid': int(uploader['anunits']['anunits'][i]),
                                    'depth': None,
                                    'thickness': None,
                                    'agetypeid': None,
                                    'age': None,
                                    'notes': None,
                                    'ageyoung': None,
                                    'ageold': None})
            chron_control = cur.fetchone()[0]
            response['chron_control_units'].append(chron_control)
            response['message'].append(f"✗ Adding temporary chron controls {chron_control}.")
            response['valid'].append(False)
    response['valid'] = all(response['valid'])
    return response