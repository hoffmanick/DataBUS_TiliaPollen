from neotomaHelpers.pull_params import pull_params

def valid_site(cur, yml_dict, csv_file):
    """
    Validate if the provided site details correspond to a new and valid site entry.

    This function checks the validity of a site based on its coordinates, name, and hemisphere. It returns a dictionary containing:
        * `pass`: Boolean indicating if the site validation was successful.
        * `sitelist`: List of dictionaries for sites that are close to the provided coordinates. Each dictionary includes 'siteid', 'sitename', 'lat', 'long', and 'distance'.
        * `hemisphere`: String indicating the hemisphere of the site based on its coordinates.
        * `matched`: Dictionary with details about name and distance matching with nearby sites, including:
            * `namematch`: Boolean indicating if a nearby site has the same name as the provided site.
            * `distmatch`: Boolean indicating if a nearby site's coordinates exactly match the provided coordinates.

    Args:
        cur (_psycopg2.extensions.connection_): Database connection to a Neotoma database.
        coords (list): Coordinates of the site, expected as [latitude, longitude].
        hemisphere (list): List of strings representing acceptable hemispheres (e.g., ['NW', 'NE', 'SW', 'SE']).
        sitename (str): The unique name of the site.

    Returns:
        dict: Contains the keys `pass`, `sitelist`, `hemisphere`, and `matched` with respective validation results.
    """
    response = {'valid': [],
                'hemisphere': '', 
                'sitelist': [],
                'matched': {'namematch': False, 'distmatch': False},
                'doublematch':False,
                'message': []}

    params = ["sitename", "altitude", "area", "sitedescription", "notes", "geog", "siteid"]
    inputs = pull_params(params, yml_dict, csv_file, 'ndb.sites')
    inputs['siteid'] = None if inputs['siteid'][0] == 'NA' else int(inputs['siteid'][0])
    coords = inputs['geog']

    if len(coords) == 2 and -90 <= coords[0] <= 90 and -180 <= coords[1] <= 180:
        response['valid'].append(True)
    else:
        coord_error = 'multiple' if len(coords) > 2 else 'no'
        response['message'].append(f'✗ There are {coord_error} columns mapped to coordinates in your template.')
        response['valid'].append(False)

    sitename = inputs['sitename']
    if len(sitename) == 1:
        response['valid'].append(True)
    else:
        sitename_error = 'multiple' if len(sitename) > 1 else 'no'
        response['message'].append(f'✗ There are {sitename_error} columns mapped to sitenames in your template.')
        response['valid'].append(False)

    coord_dict = {'lat': coords[0], 'long': coords[1]}
    response['hemisphere'] = ('N' if coord_dict['lat'] >= 0 else 'S') + ('E' if coord_dict['long'] >= 0 else 'W')
    response['message'].append(f"? This set is expected to be in the {response['hemisphere']} hemisphere.")

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
        if close_sites:
            response['message'].append('?  One or more sites exist close to the requested site.')
            for site_data in close_sites:
                site = {'id': str(site_data[0]), 'name': site_data[1], 'coordlo': str(site_data[2]), 'coordla': str(site_data[3]), 'distance (m)': round(site_data[13], 0)}
                response['sitelist'].append(site)
            sitenames_list = [site['name'] for site in response['sitelist']]
            response['matched']['namematch'] = any(sitename in sitenames_list for sitename in sitename)
            response['matched']['distmatch'] = any(site['distance (m)'] == 0 for site in response['sitelist'])
            response['doublematch'] = response['matched']['namematch'] and response['matched']['distmatch']
            match_status = 'matches' if response['doublematch'] else 'differs'
            response['message'].append(f'? Site name {match_status}, but locations differ.')
        else:
            response['valid'].append(True)
            response['sitelist'] = [{'id': None, 'name': None, 'coordlo': None, 'coordla': None, 'distance (m)': None}]
            response['message'].append('✔  There are no sites close to the proposed site.')
    else:
        response['message'].append("Verifying if the site exists already in neotoma with the same siteID")
        site_query = """SELECT * from ndb.sites where siteid = %(siteid)s"""
        cur.execute(site_query, {'siteid': inputs['siteid']})
        site_info = cur.fetchall()
        if not site_info:
            response['valid'].append(False)
            response['message'].append(f"? Site ID {inputs['siteid']} is not currently associated to a site in Neotoma.")
        else:
            response['message'].append("✔  Site ID found in Neotoma:")
            for site_data in site_info:
                site = {'id': str(site_data[0]), 'name': site_data[1], 'coordlo': str(site_data[2]), 'coordla': str(site_data[3])}
                response['sitelist'].append(site)
                name_match = site['name'] == inputs['sitename'][0]
                coord_match = float(site['coordlo']) == coords[1] and float(site['coordla']) == coords[0]
                response['matched']['namematch'] = name_match
                response['matched']['distmatch'] = coord_match
                response['valid'].append(name_match and coord_match)
                response['message'].append(site)
                if not name_match:
                    response['message'].append(f"✗ The sitenames do not match. Current sitename in Neotoma: {site['name']}. Proposed name: {inputs['sitename'][0]}.")
                if not coord_match:
                    response['message'].append("✗ Coordinates do not match.")
                    response['message'].append(f"✗ Current latitude in Neotoma: {site['coordla']}. Proposed latitude: {coords[0]}.")
                    response['message'].append(f"✗ Current longitude in Neotoma: {site['coordlo']}. Proposed longitude: {coords[1]}.")

    response['valid'] = all(response['valid'])
                
    return response