import itertools
from .pull_params import pull_params

def valid_collunit(cur, yml_dict, csv_file):
    """Is the collection unit valid as a new unit?

    Args:
        cur (_psycopg2.extensions.connection_): _A connection to a valid Neotoma database (either local or remote)_
        coords (_list_): _A list containing the coordinates for the site. We expect only a single element, a string lat/long pair._
        collunits (_list_): _A list containing unique collection unit names._

    Returns:
        _dict_: _A dict object with properties `pass` (boolean) and `collunits` (a list of valid collection units at the site). _
    """

    response = {'valid': [],
                'sitelist': [],
                'message': []}
    params = ["handle", "colldate", "geog", "location"]
    inputs = pull_params(params, yml_dict, csv_file, 'ndb.collectionunits')
    coords = inputs['geog']
    try:
        assert len(coords) == 2
        assert coords[0] >= -90 and coords[0] <= 90
        assert coords[1] >= -180 and coords[1] <= 180
        response['valid'].append(True)
    except AssertionError:
        if len(coords) > 2:
            response['message'].append('✗ There are multiple columns mapped to coordinates in your template.')
            response['valid'].append(False)
        else:
            response['message'].append('✗ There are no columns mapped to coordinates in your template.')
            response['valid'].append(False)

    handlename = inputs['handle']
    try:
        assert len(handlename) == 1
        # Find if it is an existing Handlename
        if handlename == 'NA':
            response['message'].append('✗ Handlename must not be called NA')
            response['valid'].append(False)
        handle_query = """SELECT count(*) from ndb.collectionunits where handle = %(_handle)s;"""
        cur.execute(handle_query, {'_handle': handlename[0]})
        rows = cur.fetchall()
        # Retrieve site name information; compare against sitename in the case of an exisiting handle
        if rows == [(0,)]:
            response['message'].append('✔  There are no same handles, a new collection unit will be created')
            response['valid'].append(True)
        else:
            response['message'].append('? This is an exisiting handlename. Data will be inserted in this collection unit.')
    
    except AssertionError:
        response['message'].append('✗ There can only be a single collection unit handle defined.')
        response['valid'].append(False)
    
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
            if any([j == handlename[0] for j in goodcols]):
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