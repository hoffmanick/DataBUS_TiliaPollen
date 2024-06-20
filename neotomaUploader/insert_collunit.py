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
        response['message'].append("✗ The template must contain a collectionunit handle.", exc_info = True)
    
    params = ["handle", "corecode", "colltypeid", "depenvtid", "collunitname", "colldate", "colldevice",
              "gpsaltitude", "gpserror", "waterdepth", "substrateid", "slopeaspect", "slopeangle", 
              "location", "notes", "geog"]
    # geog retrieves the elements for gpslatitude and gpslongitude
    
    # To clarify: handle and corecode seem to be the same in the template. I am guessing handle is when it has already been given; core is the element how a handle is created?
    # Is it safe to take the first 10 elements of a core to name the handle?
    inputs = nh.pull_params(params, yml_dict, csv_template, 'ndb.collectionunits')
    inputs = dict(map(lambda item: (item[0], None if all([i is None for i in item[1]]) else item[1]),
                      inputs.items()))
    overwrite = nh.pull_overwrite(params, yml_dict, 'ndb.collectionunits')
    nh.process_inputs(inputs, response, 'handle', values = ['corecode', 'depenvtid', 'colldate', 'location'])
    if inputs['depenvtid'] is not None:
        query = """SELECT depenvtid FROM ndb.depenvttypes
                   WHERE LOWER(depenvt) = %(depenvt)s"""
        cur.execute(query, {'depenvt': inputs['depenvtid'].lower()})
        inputs['depenvtid'] = cur.fetchone()[0]

    if response['given_handle']:
        response['handle'] = str(inputs['handle'])
        cur.execute("""SELECT * FROM ndb.collectionunits WHERE handle = %(handle)s""", 
                    {'handle': response['handle']})
        coll_info = cur.fetchall()
        if len(coll_info) == 1:
            coll_info = coll_info[0]
            response['message'].append(f"✔  Handle {response['handle']} found in Neotoma.")
            collunit = {'collectionunitid': int(coll_info[0]), 
                        "handle": str(coll_info[1]), 
                        'siteid': nh.clean_numbers(coll_info[2]),
                        "colltypeid": nh.clean_numbers(coll_info[3]), 
                        "depenvtid": nh.clean_numbers(coll_info[4]), 
                        "collunitname": str(coll_info[5]),
                        "colldate": (coll_info[6]), 
                        "colldevice": str(coll_info[7]), 
                        "ns": nh.clean_numbers(coll_info[8]),
                        "ew": nh.clean_numbers(coll_info[9]),
                        "gpsaltitude": nh.clean_numbers(coll_info[10]), 
                        "gpserror": coll_info[11], 
                        "waterdepth": nh.clean_numbers(coll_info[12]), 
                        "substrateid": coll_info[13], 
                        "slopeaspect": coll_info[14], 
                        "slopeangle": coll_info[15], 
                        "location": str(coll_info[16]), 
                        "notes": str(coll_info[17])}
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
                                                  _gpslatitude := %(ns)s,
                                                  _gpslongitude := %(ew)s, 
                                                  _gpsaltitude := %(gpsaltitude)s,
                                                  _gpserror := %(gpserror)s,
                                                  _waterdepth := %(waterdepth)s,
                                                  _substrateid := %(substrateid)s,
                                                  _slopeaspect := %(slopeaspect)s,
                                                  _slopeangle := %(slopeangle)s,
                                                  _location := %(location)s,
                                                  _notes := %(notes)s)
                                                  """            
            nh.process_inputs(updated_collunit)
            cur.execute(up_query, updated_collunit)
            response['collunitid'] = cur.fetchone()[0]
            response['valid'].append(True)
            response['collunits'].append({'collunit': collunit, 'updated_params': updated_collunit}) 
        elif len(coll_info) == 0:
            response['message'].append(f"Collunit not found")
            inputs['siteid'] = uploader['sites']['siteid']
            response['valid'].append(False)
            response['message'].append(f"✗ Collection Unit ID {response['collunitid']} is not currently associated to a Collection Unit in Neotoma.")
            inputs['handle'] = inputs['handle'][:10]
            cur.execute(collunit_query, inputs)
            response['collunits'].append(inputs)
            response['collunitid'] = cur.fetchone()[0]
            response['message'].append(f"Continuing process with temporary CollUnit ID {response['collunitid']}.\nRevise information or create new collection unit (remove handle from CSV)")
    else:
        response['message'].append("Handle not given")
        inputs['siteid'] = uploader['sites']['siteid']
        try:
            cur.execute("SAVEPOINT before_try")
            cur.execute(collunit_query, inputs)
            response['collunitid'] = cur.fetchone()[0]
            response['collunits'].append(inputs)
            response['valid'].append(True)
        except Exception as e:
            cur.execute("ROLLBACK TO SAVEPOINT before_try") # To clear status of previous error.
            response['message'].append(f"✗ Collection Unit Data is not correct. Error message: {e}")
            inputs = {key: None for key in inputs}
            inputs['siteid'] = uploader['sites']['siteid']
            inputs['handle'] = 'Placehold'
            cur.execute(collunit_query, inputs)
            response['collunitid'] = cur.fetchone()[0]
            response['collunits'].append(inputs)
            response['valid'].append(False)
    response['valid'] = all(response['valid'])
    return response