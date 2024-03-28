from .yaml_values import yaml_values
from .pull_params import pull_params
import logging

def valid_site(cur, yml_dict, csv_file):
    """_Is the site a valid new site?_
    The function accepts a set of coordinates, a site name, and the appropriate hemisphere and 
    returns a dict with the properties:
        * `pass`: Did the operation work as expected (a site is matched and a valid siteid returned).
        * `sitelist`: A list of site dicts including {'siteid', 'sitename', 'lat', 'long', 'distance'}
        * `hemisphere`: Does the site exist within the expected hemispheres?
        * `matched`: Given a set of "close" sites, does one of them also have the right name? This dict includes two
        properties
            * `namematch`: does one of the close sites have the same name as the target site?
            * `distmatch`: is one site coincident with the current site coordinates?
    Args:
        cur (_psycopg2.extensions.connection_): _A connection to a valid Neotoma database (either local or remote)_
        coords (_list_): _A list containing the coordinates for the site. We expect only a single element, a string lat/long pair._
        hemisphere (_list_): _A single character string, or set of strings indicating acceptable global quadrants for the site (NW, NE, SW, SE)._
        sitename (_string_): The unique site name for the record.
    Returns:
        _dict_: _A dict object with two properties, the boolean `pass` and a `sitelist` with all close sites._
    """
    response = {'pass': False,
                'hemisphere': False, 
                'sitelist': [],
                'matched': {'namematch': False, 'distmatch': False},
                'message': []}

    ## Retrieve the fields needed from the yml.
    #coords = yaml_values(yml_dict, csv_file, 'ndb.sites.geog')

    params = ["sitename", "altitude", "area", "sitedescription", "notes", "geog"]#, "siteid"]
    inputs = pull_params(params, yml_dict, csv_file, 'ndb.sites')
    inputs['siteid'] = None # Placeholder until I get site IDs
    coords = inputs['geog']
    print(coords)

    try:
        assert len(coords) == 2
        assert coords[0] >= -90 and coords[0] <= 90
        assert coords[1] >= -180 and coords[1] <= 180
    except AssertionError:
        if len(coords) > 2:
            logging.error('✗ There are multiple columns mapped to coordinates in your template.')
        else:
            logging.error('✗ There are no columns mapped to coordinates in your template.')
        return response
    sitename = inputs['sitename']
    try:
        assert len(sitename) == 1
    except AssertionError:
        if len(sitename) > 1:
            response['message'].append('✗ There are multiple columns mapped to sitenames in your template.')
        else:
            response['message'].append('✗ There are no columns mapped to sitenames in your template.')

    # Evaluate whether it's a new site, or not.
    coord_dict = {'lat': coords[0],
                  'long': coords[1]}
    
    # Get the allowed hemispheres for the record.
    hemis = ""
    if coord_dict['lat'] >= 0:
        hemis+="N"
    else:
        hemis+="S"
    if coord_dict['long'] >= 0:
        hemis+="E"
    else:
        hemis+="W"
    response['message'].append(f'? This set is expected to be in the {hemis} hemisphere.')

    # When not given a SiteID
    if inputs['siteid'] is None:
        close_site = """
            SELECT st.*,
                ST_SetSRID(st.geog::geometry, 4326)::geography <-> ST_SetSRID(ST_Point(%(long)s, %(lat)s), 4326)::geography AS dist
            FROM   ndb.sites AS st
            WHERE ST_SetSRID(st.geog::geometry, 4326)::geography <-> ST_SetSRID(ST_Point(%(long)s, %(lat)s), 4326)::geography < 10000
            ORDER BY dist;"""
        cur.execute(close_site, coord_dict)
        close_sites = cur.fetchall()
        if len(close_sites) > 0:
            # There are sites within 10km get the siteid, name, coordinates and distance.
            response['valid'] = False
            response['message'].append('?  One or more sites exist close to the requested site.')
            for i in close_sites:
                site = {'id': str(i[0]), 'name': i[1], 'coordlo': str(i[2]), 'coordla': str(i[3]), 'distance (m)': round(i[13], 0)}
                response['sitelist'].append(site)
            # extract only the names of the sites
            sitenames_list = [item['name'] for item in response['sitelist']]
            response['matched']['namematch'] = any(x in sitename for x in sitenames_list)
            # Distmatch should be independent of the sitename
            response['matched']['distmatch'] = next((item['distance (m)'] for item in response['sitelist']), None) == 0
            if response['matched']['namematch'] and response['matched']['distmatch']:
                response['valid'] = True
                response['message'].append('✔  Valid site: Site currently exists at the reported location and the name is matched.')
            elif response['matched']['namematch']:
                response['valid'] = False
                response['message'].append('?  Site name matches, but locations differ.')
            elif response['matched']['distmatch']:
                response['valid'] = False
                response['message'].append('?  Location matches, but site names differ.')
            if response['valid'] is False:
                for i in response['sitelist']:
                    response['message'].append(f"  * siteid: {i['id']};  sitename: {i['name']:<25}; distance (m): {i['distance (m)']:<7} coords: [{i['coordla']}, {i['coordlo']}]")
        else:
            response['valid'] = True
            response['sitelist'] = [{'id': None, 'name': None, 'coordlo': None, 'coordla': None, 'distance (m)': None}]
            response['matched'] = {'namematch': False, 'distmatch': False}
            response['message'].append('✔  There are no sites close to the proposed site.')
    else:
        site_query = """SELECT * from ndb.sites where siteid = %(siteid)s"""
        cur.execute(site_query, inputs['siteid'])
        site_info = cur.fetchall()
        if site_info == None:
            response['valid'] = False
            response['message'].append(f"? Site ID {inputs['siteid']} is not currently associated to a site in Neotoma.")
        else:
            site = {'id': str(i[0]), 'name': i[1], 'coordlo': str(i[2]), 'coordla': str(i[3])}
            response['sitelist'].append(site)
            if site['name'] != inputs['sitename']:
                response['valid'] = False
                response['message'].append(f"✗ The sitenames do not match. Current sitename in Neotoma: {site['name']}. Proposed name: {inputs['sitenmae']}.")
            else:
                response['valid'] = True
                response['message'].append("✔  Names match.")
            
            if site['coordlo'] != inputs['coords'][1]:
                response['valid'] = False
                response['message'].append(f"✗ Longitudes do not match. Current coords in Neotoma: {site['coordlo']}. Proposed coords: {inputs['coords'][1]}.")
            else:
                response['valid'] = True
                response['message'].append("✔  Longitudes match.")

            if site['coordla'] != inputs['coords'][0]:
                response['valid'] = False
                response['message'].append(f"✗ Latitudes do not match. Current coords in Neotoma: {site['coordlo']}. Proposed coords: {inputs['coords'][0]}.")
            else:
                response['valid'] = True
                response['message'].append("✔  Latitues match.")
                
    return response