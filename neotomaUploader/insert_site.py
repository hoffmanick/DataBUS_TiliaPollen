import numpy as np
import neotomaHelpers as nh
with open('./sqlHelpers/site_query.sql', 'r') as sql_file:
    site_query = sql_file.read()
with open('./sqlHelpers/upsert_site.sql', 'r') as sql_file:
    upsert_query = sql_file.read()

def insert_site(cur, yml_dict, csv_template):
    """_Insert a site to Neotoma_
    Inserts site data into Neotoma.
    Args:
        cur (_psycopg2.extensions.cursor_): _A cursor pointing to the Neotoma 
            Paleoecology Database._
        yml_dict (_dict_): _A `dict` returned by the YAML template._
        csv_template (_dict_): _The csv file with the required data to be uploaded._

    Returns:
        response (dict): A dictionary containing information about the inserted site.
            'siteid' (int): IDs for the inserted site.
            'valid' (bool): Indicates if insertions were successful.
    """
    response = {'s_id': False, 'siteid': np.nan, 'valid': [], 'message': [], 'sitelist': []}
    params = ["siteid", "sitename", "altitude", "area", "sitedescription", "notes", "geog"]
    inputs = nh.pull_params(params, yml_dict, csv_template, 'ndb.sites')
    inputs = dict(map(lambda item: (item[0], None if all([i is None for i in item[1]]) else item[1]),
                      inputs.items()))
    overwrite = nh.pull_overwrite(params, yml_dict, 'ndb.sites')
    
    try:
        # Verify that site name and geometry are present in CSV
        assert all(inputs.get(key) is not None and inputs[key] != [] 
                   for key in ['sitename', 'geog'])
    except AssertionError:
        response['message'].append("✗ The template must contain a sitename and coordinates.", exc_info=True)
        response['valid'].append(False)  
    nh.process_inputs(inputs, response, 'sitename', ['altitude','area','description','notes'])  

    inputs['coordlo'] = float(inputs['geog'][1])
    inputs['coordla'] = float(inputs['geog'][0])
    if inputs['siteid'] is not None and inputs['siteid'] != ["NA"]:
        response['s_id'] = True
        assert len(inputs['siteid']) == 1, "multiple siteIDs given"
        response['message'].append(f"Site ID has been given: {inputs['siteid'][0]}")
    else:
        response['s_id'] = False
        response['message'].append(f"A new site ID will be generated")

    if response['s_id']:
        inputs['siteid'] = int(inputs['siteid'][0])
        response['siteid'] = int(inputs['siteid'])
        cur.execute("""SELECT * from ndb.sites where siteid = %(siteid)s""", 
                    {'siteid': response['siteid']})
        site_info = cur.fetchall()
        if len(site_info) == 1:
            site_info = site_info[0]
            response['message'].append(f"✔  Site ID {response['siteid']} found in Neotoma.")
            site = {'siteid': int(site_info[0]), 'sitename': site_info[1], 
                    'coordlo': float(site_info[2]), 'coordla': float(site_info[3]),
                    'altitude': None, 'area': None, 'sitedescription': None, 'notes': None}
            #response['sitelist'].append(site)
            matched = dict()
            updated_site = dict()
            for element in site:
                if element == 'siteid':
                    updated_site[element] = site[element]
                elif site[element] != inputs[element]:
                    matched[element] = False
                    response['message'].append(f"✗ The {element}s do not match.\nOverwrite for {element} is set to {overwrite[element]}")
                    if overwrite[element] == True:
                        updated_site[element] = inputs[element]
                        response['message'].append(f"Updated {element} to: {inputs[element]}.")        
                    else:
                        updated_site[element] = None
                        response['message'].append(f"? Current {element} in Neotoma will not be updated")                               
                else:
                    updated_site[element] = None
                    matched[element] = True
                    response['valid'].append(True)
                    response['message'].append(f"✔  {element}s match.")
            matched = all(value for value in matched.values())

            response['sitelist'].append({'site': site, 'updated_params': updated_site})            
            updated_site['ew'] = updated_site.pop('coordlo')
            updated_site['ns'] = updated_site.pop('coordla')
            cur.execute(upsert_query) # Defines upsert_site SQL function
            up_query =  """SELECT upsert_site(_siteid := %(siteid)s,
                                              _sitename := %(sitename)s,
                                              _altitude := %(altitude)s,
                                              _area := %(area)s,
                                              _descript := %(sitedescription)s,
                                              _notes := %(notes)s,
                                              _east := %(ew)s,
                                              _north:= %(ns)s)
                                              """
            up_inputs = {'siteid': int(updated_site['siteid']),
                         'sitename': str(updated_site['sitename']),
                         'altitude': None if updated_site['altitude'] is None else int(updated_site['altitude']),
                         'area': None,
                         'sitedescription': str(updated_site['sitedescription']),
                         'notes': str(updated_site['notes']),
                         'ew': None if updated_site['ew'] is None else float(updated_site['ew']),
                         'ns':  None if updated_site['ns'] is None else float(updated_site['ns'])}  
            cur.execute(up_query, up_inputs)
            response['siteid'] = cur.fetchone()[0]

        elif len(site_info) == 0:
            response['valid'].append(False)
            response['message'].append(f"✗ Site ID {response['siteid']} is not currently associated to a site in Neotoma.")
            cur.execute(site_query, inputs)
            response['siteid'] = cur.fetchone()[0]
            site = {'id': response['siteid'], 'name': inputs['sitename'], 'coordlo': inputs['coordlo'], 'coordla': inputs['coordla']}
            response['sitelist'].append({'temporary site': site})
            response['message'].append(f"Continuing process with temporary Site ID {response['siteid']}.\nRevise information or create new site (remove siteIDs from CSV)")
    else:
        try:
            cur.execute(site_query,
                        inputs)
            response['siteid'] = cur.fetchone()[0]
            inputs['siteid'] = response['siteid']
            response['sitelist'].append(inputs)
            response['valid'].append(True)
            response['message'].append(f"✔ Site inserted {response['siteid']}")

        except Exception as e:
            response['message'].append(f"✗ Site Data is not correct. Error message: {e}")
            inputs = {"sitename": 'Placeholder', 
                      "altitude": None,
                      "area": None,
                      "sitedescription": None, 
                      "notes": None, 
                      "geog": None}
            cur.execute(site_query, inputs)
            response['siteid'] = cur.fetchone()[0]
            response['message'].append(f"Temporary SiteID {response['siteid']} used.")
            response['valid'].append(False)
            response['sitelist'].append(inputs)
    response['valid'] = all(response['valid'])
    return response