import numpy as np
#from .pull_params import pull_params

def insert_site(cur, yml_dict, csv_template, overwrite):
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
    response = {'siteid': np.nan, 'valid': [], 'message': [], 'sitelist': []}
    
    site_query = """
        SELECT ts.insertsite(_sitename := %(sitename)s, 
                             _altitude := %(altitude)s,
                             _area := %(area)s,
                             _descript := %(description)s,
                             _notes := %(notes)s,
                             _east := %(ew)s,
                             _north := %(ns)s,
                             _west := %(ew)s,
                             _south := %(ns)s)
                 """
    try:
        # Here we're just checking to make sure that we do have a site coordinate
        # and geometry.
        assert all(element in [d.get('neotoma') for d in yml_dict.get('metadata')]
                   for element in ['ndb.sites.sitename', 'ndb.sites.geog'])
    except AssertionError:
        response['message'].append("✗ The template must contain a sitename and coordinates.", exc_info=True)
    
    params = ["siteid", "sitename", "altitude", "area", "sitedescription", "notes", "geog"]
    inputs = pull_params(params, yml_dict, csv_template, 'ndb.sites')
    inputs = dict(map(lambda item: (item[0], None if all([i is None for i in item[1]]) else item[1]),
                      inputs.items()))

    if isinstance(inputs['sitename'], list): 
        if len(list(set(inputs['sitename']))) > 1:
            response['message'].append("✗ There should only be one site name.")
        inputs['sitename'] = inputs['sitename'][0]
    if inputs['altitude'] is not None:
        inputs['altitude'] = inputs['altitude'][0]
    if inputs['area'] is not None:
        inputs['area'] = inputs['area'][0]
    if inputs['sitedescription'] is not None:
        inputs['description'] = inputs['sitedescription'][0]
    else:
        inputs['description'] = None
    if inputs['notes'] is not None:
        inputs['notes'] = inputs['notes'][0]

    try:
        coords = inputs['geog']
        assert len(coords) == 2
        assert coords[0] >= -90 and coords[0] <= 90
        assert coords[1] >= -180 and coords[1] <= 180
    except AssertionError:
        response['message'].append("✗ Coordinates are improperly formatted. They must be in the form 'LAT, LONG' [-90 -> 90] and [-180 -> 180].")
    inputs['ns'] = coords[0]
    inputs['ew'] = coords[1]

    if inputs['siteid'] is not None or inputs['siteid'] != "NA":
        response['siteid'] = int(inputs['siteid'][0])
        #response['siteid'] = 173 # temporary for testing
        response['message'].append(f"Site ID has been given: {response['siteid']}")
        siteid_query = """SELECT * from ndb.sites where siteid = %(siteid)s"""
        cur.execute(siteid_query, {'siteid': response['siteid']})
        site_info = cur.fetchall()
        if len(site_info) == 1:
            matched = {'sitename': False, 
                       'coordlo': False, 
                       'coordla': False}
            site_info = site_info[0]
            response['message'].append(f"✔  Site ID {response['siteid']} found in Neotoma.")
            site = {'id': str(site_info[0]), 'name': site_info[1], 'coordlo': str(site_info[2]), 'coordla': str(site_info[3])}
            response['sitelist'].append(site)
            if site['name'] != inputs['sitename']:
                response['valid'].append(False)
                response['message'].append(f"✗ The sitenames do not match. Current sitename in Neotoma: {site['name']}. Proposed name: {inputs['sitename'][0]}.")
            else:
                response['valid'].append(True)
                matched['sitename'] = True
                response['message'].append("✔  Names match.")
            
            if float(site['coordlo']) != float(coords[1]):
                response['valid'].append(False)
                response['message'].append(f"✗ Longitudes do not match. Current coords in Neotoma: {site['coordlo']}. Proposed coords: {coords[1]}.") 
            else:
                response['valid'].append(True)
                matched['coordlo'] = True
                response['message'].append("✔  Longitudes match.")

            if float(site['coordla']) != coords[0]:
                response['valid'].append(False)
                response['message'].append(f"✗ Latitudes do not match. Current coords in Neotoma: {site['coordla']}. Proposed coords: {coords[0]}.")
            else:
                response['valid'].append(True)
                matched['coordla'] = True
                response['message'].append("✔  Latitudes match.")
            print('matched', matched)
            matched = all(value for value in matched.values())
            print('matched', matched)
            if overwrite and matched:
                # No changes really
                response['message'].append("Overwrite is set to True and everything matches.")
                response['message'].append(f"SiteID {inputs['siteid']} will be used.")
                response['valid'].append(True)
            elif overwrite and not matched:
                print(matched)
                # Update of any field
                response['message'].append("Overwrite is set to True but some elements are different.")
                response['message'].append(f"SiteID: {response['siteid']}, coordlo: {coords[1]}, coordla: {coords[0]} will be recorded.")
                response['valid'].append(True)
                # TODO: How do I actually upsert here?
            elif not overwrite and matched:
                response['message'].append("Overwrite is set to False - but everything matches. Information will remain the same.")
                response['valid'].append(True)
            elif not overwrite and not matched:
                response['message'].append("Overwrite is set to False and elements do not match. Review the files and upload when ready.")
                response['valid'].append(False)     
        elif len(site_info) == 0:
            if overwrite == False:
                response['valid'].append(False)
            elif overwrite == True:
                response['valid'].append(True)
            response['message'].append(f"✗ Site ID {response['siteid']} is not currently associated to a site in Neotoma.")
            cur.execute(site_query,
                        inputs)
            response['siteid'] = cur.fetchone()[0]
            site = {'id': response['siteid'], 'name': inputs['sitename'], 'coordlo': coords[1], 'coordla': coords[0]}
            print('site', site)
            response['sitelist'].append(site)
            response['message'].append(f"Continuing process. Assigned temporary Site ID {response['siteid']}. Overwrite is set to {overwrite}.")

    else:
        try:
            cur.execute(site_query,
                        inputs)
            response['siteid'] = cur.fetchone()[0]
            response['valid'].append(True)

        except Exception as e:
            response['message'].append(f"Site Data is not correct. Error message: {e}")
            inputs = {"sitename": 'Placeholder', 
                    "altitude": None,
                    "area": None,
                    "sitedescription": None, 
                    "notes": None, 
                    "geog": None}
            cur.execute(site_query, inputs)
            response['siteid'] = cur.fetchone()[0]
            response['valid'].append(False)
    response['valid'] = all(response['valid'])
    return response