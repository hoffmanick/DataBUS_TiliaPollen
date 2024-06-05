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
    
    params = ["handle", "corecode", "colltypeid", "depenvtid", "collunitname", "colldate", "colldevice",
              "gpslatitude", "gpslongitude", "gpsaltitude", "gpserror", "waterdepth", 
              "substrateid", "slopeaspect", "slopeangle", "location", "notes", "geog"]
    inputs = nh.pull_params(params, yml_dict, csv_template, 'ndb.collectionunits')
    inputs = dict(map(lambda item: (item[0], None if all([i is None for i in item[1]]) else item[1]),
                      inputs.items()))
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
        inputs['handle'] = str(inputs['handle'][0])
        response['message'].append(f"Handle has been given: {inputs['handle']}")
    else:
        response['given_handle'] = False
        response['message'].append(f"A new Handle will be generated")
        inputs['handle'] = str(inputs['corecode'][0])[:10]
    
    if response['given_handle']:
        print("Handle given")
        response['handle'] = str(inputs['handle'])
        cur.execute("""SELECT * FROM ndb.collectionunits WHERE handle = %(handle)s""", 
                    {'handle': response['handle']})
        coll_info = cur.fetchall()
        if len(coll_info) == 1:
            print("Collunit found")
            coll_info = coll_info[0]
            response['message'].append(f"✔  Handle {response['handle']} found in Neotoma.")
            collunit = {'collectionunitid': int(coll_info[0]), "handle": str(coll_info[1]), 
                        'siteid': nh.clean_numbers(coll_info[2]),"colltypeid": nh.clean_numbers(coll_info[3]), 
                        "depenvtid": nh.clean_numbers(coll_info[4]), "collunitname": str(coll_info[5]),
                        "colldate": (coll_info[6]), "colldevice": str(coll_info[7]), 
                        "ns": nh.clean_numbers(coll_info[8]),
                        "ew": nh.clean_numbers(coll_info[9]),
                        "gpsaltitude": nh.clean_numbers(coll_info[10]), "gpserror": coll_info[11], 
                        "waterdepth": nh.clean_numbers(coll_info[12]), "substrateid": coll_info[13], 
                        "slopeaspect": coll_info[14], "slopeangle": coll_info[15], 
                        "location": str(coll_info[16]), "notes": str(coll_info[17])}
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
            cur.execute(upsert_query) 
            up_query =  """SELECT upsert_collunit(_collectionunitid := %(collectionunitid)s,
                                                  _handle := %(handle)s,
                                                  _siteid := %(siteid)s,
                                                  _colltypeid := %(colltypeid)s,
                                                  _depenvtid := %(depenvtid)s,
                                                  _collunitname := %(collunitname)s, 
                                                  _colldate := %(colldate)s,
                                                  _colldevice := %(colldevice)s,
                                                  _ns := %(ns)s,
                                                  _ew := %(gpslongitude)s, 
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
                         'colltypeid': nh.clean_numbers(updated_collunit['colltypeid']),
                         'depenvtid': nh.clean_numbers(updated_collunit['depenvtid']),
                         'collunitname': None if updated_collunit['collunitname'] is None else str(updated_collunit['collunitname']),
                         'colldate': None if updated_collunit['colldate'] is None else updated_collunit['colldate'],
                         'colldevice': nh.clean_numbers(['colldevice']),
                         'ns': nh.clean_numbers(updated_collunit['ns']),
                         'ew': nh.clean_numbers(updated_collunit['ew']),
                         'gpsaltitude': nh.clean_numbers(updated_collunit['gpsaltitude']),
                         'gpserror': nh.clean_numbers(updated_collunit['gpserror']),
                         'waterdepth': nh.clean_numbers(updated_collunit['waterdepth']),
                         'substrateid': nh.clean_numbers(updated_collunit['substrateid']),
                         'slopeaspect' : nh.clean_numbers(updated_collunit['slopeaspect']),
                         'slopeangle': nh.clean_numbers(updated_collunit['slopeangle']),
                         'location': None if updated_collunit['location'] is None else str(updated_collunit['location']), 
                         'notes': None if updated_collunit['notes'] is None else str(updated_collunit['notes'])}
            cur.execute(up_query, up_inputs)
            response['collunitid'] = cur.fetchone()[0]
            response['valid'].append(True)
            response['collunits'].append({'collunit': collunit, 'updated_params': updated_collunit}) 
        elif len(coll_info) == 0:
            response['message'].append(f"Collunit not found")
            inputs = {'handle': inputs['handle'], # Must be smaller than 10 chars
                      'collunitname': inputs['collunitname'],
                      'siteid' : uploader['sites']['siteid'], 
                      'colltypeid': nh.clean_numbers(inputs['colltypeid']),
                      'depenvtid': nh.clean_numbers(inputs['depenvtid']),
                      'colldate': inputs['colldate'][0] if inputs['colldate'] else None,
                      'location': inputs['location'][0] if inputs['location'] else None,
                      'ns': coords[0],
                      'ew': coords[1]}
            response['valid'].append(False)
            response['message'].append(f"✗ Collection Unit ID {response['collunitid']} is not currently associated to a Collection Unit in Neotoma.")
            cur.execute(collunit_query, inputs)
            response['collunits'].append(inputs)
            response['collunitid'] = cur.fetchone()[0]
            response['message'].append(f"Continuing process with temporary CollUnit ID {response['collunitid']}.\nRevise information or create new collection unit (remove handle from CSV)")
    else:
        response['message'].append("Handle not given")
        inputs = {'handle': inputs['handle'], # Must be smaller than 10 chars
                  'collunitname': inputs['collunitname'],
                  'siteid' : uploader['sites']['siteid'], 
                  'colltypeid': nh.clean_numbers(inputs['colltypeid']),
                  'depenvtid': nh.clean_numbers(inputs['depenvtid']),
                  'colldate': inputs['colldate'][0] if inputs['colldate'] else None,
                  'location': inputs['location'][0] if inputs['location'] else None,
                  'ns': coords[0],
                  'ew': coords[1]}
        try:
            cur.execute("SAVEPOINT before_try")
            cur.execute(collunit_query, inputs)
            response['collunitid'] = cur.fetchone()[0]
            response['collunits'].append(inputs)
            response['valid'].append(True)
        except Exception as e:
            cur.execute("ROLLBACK TO SAVEPOINT before_try")
            response['message'].append(f"✗ Collection Unit Data is not correct. Error message: {e}")
            inputs = {'handle': str('Placehold'),
                      'collunitname': None,
                      'siteid' : int(uploader['sites']['siteid']),
                      'colltypeid': None,
                      'depenvtid': None,
                      'colldate': None,
                      'location': None, 
                      'ns': None,
                      'ew': None}
            cur.execute(collunit_query, inputs)
            response['collunitid'] = cur.fetchone()[0]
            response['collunits'].append(inputs)
            response['valid'].append(False)
    response['valid'] = all(response['valid'])
    return response