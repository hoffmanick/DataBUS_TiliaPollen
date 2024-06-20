import itertools
from neotomaHelpers.pull_params import pull_params

def valid_collunit(cur, yml_dict, csv_file):
    """
    Validates whether the specified collection unit can be registered as a new unit in the Neotoma database.

    Args:
        cur (_psycopg2.extensions.connection_): A database connection object to interact with a Neotoma database, which can be either local or remote.  
        coords (list): A list containing the geographical coordinates of the site. This should include only one element formatted as a string in the 'latitude, longitude' format.
        collunits (list): A list of unique identifiers or names for the collection units to be validated.

    Returns:
        dict: A dictionary containing the following key-value pairs:
            - 'valid' (bool): Indicates whether the collection unit passed the validation checks.
            - 'collunits' (list): A list of collection unit names that are valid within the specified site context.
    """

    response = {'valid': list(),
                'sitelist': list(),
                'message': list()}
    params = ["handle", "core", "colldate", "geog", "location"]
    inputs = pull_params(params, yml_dict, csv_file, 'ndb.collectionunits')

    # Validate handle uniqueness
    if isinstance(inputs['handle'], list) and len(inputs['handle']) > 1:
        response['message'].append('✗ There can only be a single collection unit handle defined.')
        response['valid'].append(False)
    inputs['handle'] = None if inputs['handle'][0] == 'NA' else str(inputs['handle'][0])
    inputs['core'] = None if inputs['core'][0] == 'NA' else str(inputs['core'][0])
    coords = inputs['geog']
    if len(coords) == 2 and -90 <= coords[0] <= 90 and -180 <= coords[1] <= 180:
        response['valid'].append(True)
    else:
        response['message'].append('✗ Invalid or improperly formatted coordinates.')
        response['valid'].append(False)

    handlename = inputs['handle']
    if not handlename:
        response['message'].append('? Handlename not given. Will create new handle from core code.')
        cur.execute("SELECT handle FROM ndb.collectionunits WHERE handle = %(handle)s;", ({'handle': inputs['core'][:10],}))
        if not cur.fetchall():
            response['message'].append('✔  There are no same handles, a new collection unit will be created')
            response['valid'].append(True)
        else:
            response['message'].append('? There are already handles similar to this core code.')
            response['valid'].append(False)
    else:
        cur.execute("SELECT count(*) FROM ndb.collectionunits WHERE handle = %s;", (handlename,))
        if not cur.fetchone()[0]:
            response['message'].append('✔  There are no same handles, a new collection unit will be created')
            response['valid'].append(True)
        else:
            response['message'].append('? This is an existing handlename. Data will be inserted in this collection unit.')

    # Site checks for collection units
    if len(coords) == 2:
        coord_dict = {'lat': coords[0],
                     'long': coords[1]} 
        close_handles = """
                SELECT st.*, cu.handle,
                    ST_SetSRID(ST_Centroid(st.geog::geometry), 4326)::geography <-> ST_SetSRID(ST_Point(%(long)s, %(lat)s), 4326)::geography AS dist
                FROM   ndb.sites AS st
                INNER JOIN ndb.collectionunits AS cu ON cu.siteid = st.siteid
                WHERE ST_SetSRID(ST_Centroid(st.geog::geometry), 4326)::geography <-> ST_SetSRID(ST_Point(%(long)s, %(lat)s), 4326)::geography < 10000
                ORDER BY dist;"""
        cur.execute(close_handles, coord_dict)
        close_handles = cur.fetchall()
        if len(close_handles) > 0:
            goodcols = [i[-2] for i in close_handles]
            if any([j == handlename for j in goodcols]):
                response['message'].append('?  A collection unit with this name already exists nearby.')

            else:
                response['message'].append('?  The collection unit handle does not occur within close sites.')
                sitecol = itertools.groupby([{'sitename': k[1], 'collunit': k[-2]} for k in close_handles], lambda x: x['sitename'])
                sitemsg = [{'site':key, 'collunits': [k['collunit'] for k in list(value)]} for key, value in sitecol] 
                for i in sitemsg: 
                    #response['message'].append(f"Site: {i['site']}; collunits: {i['collunits']}")
                    site = {'site': i['site'], 'collunits': i['collunits']}
                    response['sitelist'].append(site)
        else:
            response['message'].append('✔  There are no nearby sites, a new collection unit will be created.')
            response['valid'].append(True)
    else:
        response['valid'].append(False)
        response['message'].append('✗  The coordinates for this site are improperly formatted.')
    
    response['valid'] = all(response['valid'])
    return response