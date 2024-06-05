import logging
import neotomaHelpers as nh
with open('./sqlHelpers/aunit_query.sql', 'r') as sql_file:
    aunit_query = sql_file.read()

def insert_analysisunit(cur, yml_dict, csv_template, uploader):
    """_Inserting analysis units_

    Args:
        cur (_psycopg2.extensions.cursor_): _A cursor pointing to the Neotoma 
            Paleoecology Database._
        yml_dict (_dict_): _A `dict` returned by the YAML template._
        csv_template (_dict_): _The csv file with the required data to be uploaded._
        uploader (_dict_): A `dict` object that contains critical information about the
          object uploaded so far.

    Returns:
        _int_: _The integer value of the newly created siteid from the Neotoma Database._
    """
    response = {'anunits': [], 'valid': [], 'message': []}

    params = ["analysisunitname", "depth", "thickness", "faciesid", "mixed", "igsn", "notes"]
    inputs = nh.pull_params(params, yml_dict, csv_template, 'ndb.analysisunits')
    inputs = {k: (v if v else None) for k, v in inputs.items()}

    kv = {'mixed': False, 'faciesid': None, 'igsn': None, 'notes': None}
    for k in kv:
        if isinstance(inputs[k] ,list):
            inputs[k] = [kv[k] if x is None else x for x in inputs[k]]
        elif inputs[k] == None:
            inputs[k] = [kv[k]] * len(inputs['depth'])
    #print(uploader)
    for i in range(0, len(inputs['depth'])):
        try:
            inputs_dict = {'collunitid': uploader['collunitid']['collunitid'],
                           'depth': inputs['depth'][i],
                           'thickness': inputs['thickness'][i],
                           'faciesid': inputs['faciesid'][i],
                           'mixed': inputs['mixed'][i],
                           'igsn': inputs['igsn'][i],
                           'notes': inputs['notes'][i]}
            cur.execute(aunit_query, inputs_dict)
            anunitid = cur.fetchone()[0]
            response['anunits'].append(anunitid)
            response['message'].append(f"✔ Adding Analysis Unit {anunitid}.")
            response['valid'].append(True)
        
        except Exception as e:
            logging.error(f"Analysis Unit Data is not correct. Error message: {e}")
            cur.execute(aunit_query, {'collunitid': uploader['collunitid']['collunitid'],
                                      'depth': None,
                                      'thickness': None,
                                      'faciesid': None,
                                      'mixed': None,
                                      'igsn': None,
                                      'notes': 'None'})
            anunitid = cur.fetchone()[0]
            response['anunits'].append(anunitid)
            response['message'].append(f"✗ Adding temporary Analysis Unit {anunitid} to continue process. \nSite will be removed from upload.")
            response['valid'].append(False)
    
    response['valid'] = all(response['valid'])
    return response