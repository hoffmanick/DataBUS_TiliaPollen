import neotomaHelpers as nh

def insert_data(cur, yml_dict, csv_template, uploader):
    response = {'data_points': list(), 'valid': list(), 'message': list()}
    data_query = """
                 SELECT ts.insertdata(_sampleid := %(sampleid)s,
                                      _variableid := %(variableid)s,
                                      _value := %(value)s)
                 """
    params = ['value']
    inputs = nh.pull_params(params, yml_dict, csv_template, 'ndb.data')
    
    params2 = ['variableelementid', 'variablecontextid']
    inputs2 = nh.pull_params(params2, yml_dict, csv_template, 'ndb.data')
    
    inputs2['variableelementid'] = inputs2['variableelementid'] \
        if len(inputs2['variableelementid']) != 0 \
            else [None] * len(uploader['samples']['samples'])
    
    inputs2['variablecontextid'] = inputs2['variablecontextid'] \
        if len(inputs2['variablecontextid']) != 0 \
            else [None] * len(uploader['samples']['samples'])

    get_varid = """
                 SELECT variableid FROM ndb.variables 
                           WHERE variableunitsid = %(variableunitsid)s 
                           AND taxonid = %(taxonid)s
                           AND (variableelementid IS NULL OR variableelementid = %(variableelementid)s)
                           AND (variablecontextid IS NULL OR variablecontextid = %(variablecontextid)s)
                """
    
    var_query = """
                 SELECT ts.insertvariable(_taxonid := %(taxonid)s,
                                          _variableelementid := %(variableelementid)s,
                                          _variableunitsid := %(variableunitsid)s,
                                          _variablecontextid := %(variablecontextid)s)
                """
    for i in range(len(uploader['samples']['samples'])):
        counter = 0
        for val_dict in inputs:
            get_taxonid = """SELECT * FROM ndb.taxa WHERE LOWER(taxonname) = %(taxonname)s;"""
            cur.execute(get_taxonid, {'taxonname': val_dict['taxonname'].lower()})
            taxonid = cur.fetchone()
            
            if taxonid != None:
                taxonid = int(taxonid[0])
                response['message'].append(f"✔ Taxon ID {taxonid} found.")
                #taxon_list.append((val_dict['taxonname'].lower(), taxonid))
            else:
                counter +=1
                taxonid = counter # To do temporary insert
                response['message'].append(f"✗  TaxonID for {val_dict['taxonname']} not found. Does it exist in Neotoma?")
                response['message'].append(f"Temporary TaxonID {taxonid} for insert.")
                response['valid'].append(False)
            
            val_dict['value'] = [None if item == 'NA' else item for item in val_dict['value']]
            inputs2['variableelementid'] = [None if item == 'NA' else item for item in inputs2['variableelementid']]
            inputs2['variablecontextid'] = [None if item == 'NA' else item for item in inputs2['variablecontextid']]
            
            # Get UnitsID
            get_unitsid = """SELECT * FROM ndb.variableunits WHERE LOWER(variableunits) = %(units)s;"""
            cur.execute(get_unitsid, {'units': val_dict['unitcolumn'][i].lower()})
            unitsid = cur.fetchone()[0] # This is just getting the varunitsid
            if unitsid != None:
                unitsid = int(unitsid)
                response['message'].append(f"✔ Units ID {unitsid} found.")
            else:
                counter +=1
                unitsid = counter
                response['message'].append(f"✗  UnitsID for {val_dict['unitcolumn'][i].lower()} not found. \nDoes it exist in Neotoma?")
                response['message'].append(f"Temporary UnitsID {unitsid} for insert.")
                response['valid'].append(False)
                response['valid'].append(False)
            var_dict = {'variableunitsid':unitsid, 
                         'taxonid': taxonid, 
                         'variableelementid': inputs2['variableelementid'][i], 
                         'variablecontextid': inputs2['variablecontextid'][i]}
            cur.execute(get_varid, var_dict)
            varid = cur.fetchone()
            
            if varid != None:
                varid = int(varid[0])
                response['message'].append(f"✔ Var ID {varid} found.")
            else:
                response['message'].append("? Var ID not found. Executing ts.insertvariable")
                cur.execute(var_query, {'taxonid': taxonid,
                                        'variableelementid': inputs2['variableelementid'][i],
                                        'variableunitsid': unitsid,
                                        'variablecontextid': inputs2['variablecontextid'][i]}) # inputs[i]['variablecontextid']})
                varid = cur.fetchone()[0]

            try:
                input_dict = {'sampleid': int(uploader['samples']['samples'][i]),
                              'variableid': int(varid),
                              'value': val_dict['value'][i]}
                cur.execute(data_query, input_dict)
                result = cur.fetchone()[0]
                response['data_points'].append(result)
                response['valid'].append(True)

            except Exception as e:
                response['message'].append(f"✗  Samples Data is not correct. {e}")
                input_dict = {'sampleid': int(uploader['samples']['samples'][i]),
                              'variableid': None, 
                              'value': None}
                cur.execute(data_query, input_dict)
                result = cur.fetchone()[0]
                response['data_points'].append(result)
                response['valid'].append(False)
    # Keep only error messages for taxonID, varID, UnitsID once
    response['message'] = list(dict.fromkeys(response['message'])) 
    response['valid'] = all(response['valid'])
    return response 