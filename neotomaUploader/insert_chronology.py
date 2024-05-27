import datetime
import logging
import datetime
import neotomaHelpers as nh
with open('./sqlHelpers/chron_query.sql', 'r') as sql_file:
    addChron = sql_file.read()

def insert_chronology(cur, yml_dict, csv_template, uploader):
    """
    Inserts chronology data into Neotoma.

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_template (str): File path to the CSV template.
        uploader (dict): Dictionary containing uploader details.

    Returns:
        response (dict): Dictionary containing information about the inserted chronology.
        Contains keys:
            'chronology': ID of the inserted chronology.
            'valid': Boolean indicating if the insertion was successful.
    """
    response = {'chronology': None, 'valid': list(), 'message': list()}
    
    get_cont = """SELECT contactid FROM ndb.contacts WHERE %(contactname)s = contactname;"""    
    
    params = ["contactid", "agemodel", "notes"]
    inputs = nh.pull_params(params, yml_dict, csv_template, 'ndb.chronologies')

    params2 = ['age']
    inputs_age = nh.pull_params(params2, yml_dict, csv_template, 'ndb.sampleages')

    inputs_age['age'] = [float(value) if value != 'NA' else None for value in inputs_age['age']]
    agetype = list(set(inputs_age['unitcolumn']))
    agetype = agetype[0]

    cur.execute(get_cont, {'contactname': inputs['contactid'][0]})
    contactid = cur.fetchone()[0]

    if agetype == 'cal yr BP':
        agetypeid = 2
    elif agetype == 'CE/BCE':
        agetypeid = 1
    else:
        logging.error("✗ The provided age type is incorrect..")
        response['message'].append("✗ The provided age type is incorrect..")
    
    if isinstance(inputs_age['age'], (int, float)):
        maxage = int(max(inputs_age['age']))
        minage = int(min(inputs_age['age']))
    else:
        response['message'].append("? Age is set to None. Minage/maxage will be None.")
        maxage = None
        minage = None

    try:
        cur.execute(addChron, {'collunitid': int(uploader['collunitid']['collunitid']), 
                               'contactid': contactid,
                               'chronologyname': 'Default 210Pb',  # This is a default but might be better to specify in template
                               'agetype': agetypeid, # Comming from column X210Pb.Date.Units which should be linked to params3
                               'dateprepared': datetime.datetime.today().date(),  # Default but should be coming from template s
                               'agemodel': inputs['agemodel'][0],
                               'maxage': maxage, 
                               'minage': minage})
        chron = cur.fetchone()[0]
        response['chronology'] = chron
        response['valid'].append(True)
        response['message'].append(f"✔ Adding Chronology {chron}.")

    except Exception as e:
        logging.error(f"Chronology Data is not correct. Error message: {e}")
        cur.execute(addChron, {'collunitid': int(uploader['collunitid']['collunitid']), 
                            'contactid': contactid,
                            'chronologyname': 'NULL', 
                            'agetype': None, 
                            'dateprepared': datetime.datetime.today().date(),  
                            'agemodel': None,
                            'maxage': None, 
                            'minage': None})
        chron = cur.fetchone()[0]
        response['valid'].append(False)
        response['message'].append(f"✗ Adding temporary Chronology {chron}.")
        response['chronology'] = chron
    response['valid'] = all(response['valid'])
    return response
