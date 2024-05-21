import logging
import neotomaHelpers as nh
with open('./sqlHelpers/collunit_query.sql', 'r') as sql_file:
    collunit_query = sql_file.read()
with open('./sqlHelpers/upsert_collunit.sql', 'r') as sql_file:
    upsert_query = sql_file.read()

def insert_collunit(cur, yml_dict, csv_template, uploader):
    """_Insert a new collection unit to a site_

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
    response = {'collunitid': None, 'valid': list(), 'message': list(), 'collunits': list()}
    try:
        assert all(element in [d.get('neotoma') for d in yml_dict.get('metadata')]
                   for element in ['ndb.collectionunits.handle'])
    except AssertionError:
        logging.error("The template must contain a collectionunit handle.", exc_info = True)
    
    params = ["handle", "colltypeid", "depenvtid", "collunitname", "colldate", "colldevice",
              "gpslatitude", "gpslongitude", "gpsaltitude", "gpserror", "waterdepth", 
              "substrateid", "slopeaspect", "slopeangle", "location", "notes", "geog"]
    inputs = nh.pull_params(params, yml_dict, csv_template, 'ndb.collectionunits')
    overwrite = nh.pull_overwrite(params, yml_dict, 'ndb.sites')
    
    try:
        coords = inputs['geog']
        assert len(coords) == 2
        assert coords[0] >= -90 and coords[0] <= 90
        assert coords[1] >= -180 and coords[1] <= 180
    except AssertionError:
        logging.error("Coordinates are improperly formatted. They must be in the form 'LAT, LONG' [-90 -> 90] and [-180 -> 180].")
    
    if inputs['handle'] is not None and inputs['handle'] != ["NA"]:
        response['given_handle'] = True
        assert len(inputs['handle']) == 1, "multiple handles given"
        response['message'].append(f"Handle has been given: {inputs['handle'][0]}")
    else:
        response['given_handle'] = False
        response['message'].append(f"A new Handle will be generated")
    
    handle =  inputs['handle'][0]
    if response['given_handle']:
        inputs['handle'] = str(inputs['handle'][0])
        response['handle'] = str(inputs['handle'])
        cur.execute("""SELECT * FROM ndb.collectionunits WHERE handle = %(handle)s""", 
                    {'handle': response['handle']})
        coll_info = cur.fetchall()
        if len(coll_info) == 1:
            coll_info = coll_info[0]
            response['message'].append(f"✔  Handle {response['handle']} found in Neotoma.")
            collunit = {'collectionunitid': int(coll_info[0]), "handle": str(coll_info[1]), 
                        'siteid': int(coll_info[2]),"colltypeid": int(coll_info[3]), 
                        "depenvtid": int(coll_info[4]), "collunitname": str(coll_info[5]),
                        "colldate": (coll_info[6]), "colldevice": str(coll_info[7]), 
                        "gpslatitude": float(coll_info[8]), "gpslongitude": float(coll_info[9]), 
                        "gpsaltitude": int(coll_info[10]), "gpserror": coll_info[11], 
                        "waterdepth": float(coll_info[12]), "substrateid": coll_info[13], 
                        "slopeaspect": coll_info[14], "slopeangle": coll_info[15], 
                        "location": str(coll_info[16]), "notes": str(coll_info[17])}
  #         #response['sitelist'].append(site)
            matched = dict()
            updated_collunit = dict()
            for element in collunit:
                if element in ['siteid', 'handle', 'collectionunitid']:
                    updated_collunit[element] = collunit[element]
                elif element not in inputs:
                    updated_collunit[element] = None
                    response['message'].append(f"? {element} not in CSV file. Using Neotoma's information.")
                elif collunit[element] != inputs[element]:
                    matched[element] = False
                    response['message'].append(f"✗ The {element}s do not match. Overwrite for {element} is set to {overwrite[element]}")
                    if overwrite[element] == True:
                        updated_collunit[element] = inputs[element]
                        response['message'].append(f"Updated {element} to: {inputs[element]}.")        
                    else:
                        updated_collunit[element] = None
                        response['message'].append(f"? Current {element} in Neotoma will not be updated")                               
                else:
                    updated_collunit[element] = None
                    matched[element] = True
                    response['valid'].append(True)
                    response['message'].append(f"✔  {element}s match.")
            matched = all(value for value in matched.values())
            response['collunits'].append({'collunit': collunit, 'updated_params': updated_collunit})            
            cur.execute(upsert_query) 
            up_query =  """SELECT upsert_collunit(_collectionunitid := %(collectionunitid)s,
                                                  _handle := %(handle)s,
                                                  _siteid := %(siteid)s,
                                                  _colltypeid := %(colltypeid)s,
                                                  _depenvtid := %(depenvtid)s,
                                                  _collunitname := %(collunitname)s, 
                                                  _colldate := %(colldate)s,
                                                  _colldevice := %(colldevice)s,
                                                  _gpslatitude := %(gpslatitude)s,
                                                  _gpslongitude := %(gpslongitude)s, 
                                                  _gpsaltitude := %(gpsaltitude)s,
                                                  _gpserror := %(gpserror)s,
                                                  _waterdepth := %(waterdepth)s,
                                                  _substrateid := %(substrateid)s,
                                                  _slopeaspect := %(slopeaspect)s,
                                                  _slopeangle := %(slopeangle)s,
                                                  _location := %(location)s,
                                                  _notes := %(notes)s)
                                                  """
            up_inputs = {'collectionunitid': int(updated_collunit['collectionunitid']),
                         'handle': str(updated_collunit['handle']),
                         'siteid': int(updated_collunit['siteid']),
                         'colltypeid': None if updated_collunit['colltypeid'] is None else int(updated_collunit['colltypeid']),
                         'depenvtid': None if updated_collunit['depenvtid'] is None else int(updated_collunit['depenvtid']),
                         'collunitname': None if updated_collunit['collunitname'] is None else str(updated_collunit['collunitname']),
                         'colldate': None if updated_collunit['colldate'] is None else updated_collunit['colldate'],
                         'colldevice': None if updated_collunit['colldevice'] is None else int(updated_collunit['colldevice']),
                         'gpslatitude': None if updated_collunit['gpslatitude'] is None else float(updated_collunit['gpslatitude']),
                         'gpslongitude': None if updated_collunit['gpslongitude'] is None else float(updated_collunit['gpslongitude']),
                         'gpsaltitude': None if updated_collunit['gpsaltitude'] is None else float(updated_collunit['gpsaltitude']),
                         'gpserror': None if updated_collunit['gpserror'] is None else float(updated_collunit['gpserror']),
                         'waterdepth': None if updated_collunit['waterdepth'] is None else float(updated_collunit['waterdepth']),
                         'substrateid': None if updated_collunit['substrateid'] is None else int(updated_collunit['substrateid']),
                         'slopeaspect' : None if updated_collunit['slopeaspect'] is None else int(updated_collunit['slopeaspect']),
                         'slopeangle': None if updated_collunit['slopeangle'] is None else int(updated_collunit['slopeangle']),
                         'location': None if updated_collunit['location'] is None else int(updated_collunit['location']), 
                         'notes': None if updated_collunit['notes'] is None else int(updated_collunit['notes'])}

            cur.execute(up_query, up_inputs)
            response['collunitid'] = cur.fetchone()[0]
            response['valid'].append(True)
            #print(response['collunitid'])

    else:
        inputs = {'handle': handle[:10], # Must be smaller than 10 chars
                'collname': inputs['collunitname'],
                'siteid' : uploader['sites']['siteid'], 
                'colltypeid': 3, # inputs['colltypeid'][0],
                'depenvtid': 19, # inputs['depenvtid'][0],
                'newdate': inputs['colldate'][0],
                'location': inputs['location'][0],
                'ew': coords[1],  
                'ns': coords[0]}
        try:
            cur.execute(collunit_query, inputs)
            response['collunitid'] = cur.fetchone()[0]
            response['valid'].append(True)

        except Exception as e:
            logging.error(f"Collection Unit Data is not correct. Error message: {e}")
            inputs = {'handle': 'Placeholder',
                    'collname': None,
                    'siteid' : uploader['siteid']['siteid'], 
                    'colltypeid': None,
                    'depenvtid': None,
                    'newdate': None,
                    'location': None,
                    'ew': None,  
                    'ns': None}
            cur.execute(collunit_query, inputs)
            response['collunitid'] = cur.fetchone()[0]
            response['valid'].append(False)

    response['valid'] = all(response['valid'])
    #print(response)
    return response