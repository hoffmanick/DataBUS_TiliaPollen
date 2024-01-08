import logging
from .pull_params import pull_params

def insert_data(cur, yml_dict, csv_template, uploader):
    results_dict = {'data_points': [], 'valid': []}
    data_query = """
                 SELECT ts.insertdata(_sampleid := %(sampleid)s,
                                      _variableid := %(variableid)s,
                                      _value := %(value)s)
                 """
    params = ['value']
    inputs = pull_params(params, yml_dict, csv_template, 'ndb.data')

    params2 = ['variableelementid', 'variablecontextid']
    inputs2 = pull_params(params2, yml_dict, csv_template, 'ndb.data')
    
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
    # Set isolation to read even uncommitted transactions 
    # This is to try to get the correct variableids
    #cur.execute("BEGIN ISOLATION LEVEL READ UNCOMMITTED")
    for i in range(len(uploader['samples']['samples'])):
        counter = 0

        for val_dict in inputs:
            get_taxonid = """SELECT * FROM ndb.taxa WHERE taxonname %% %(taxonname)s;"""
            cur.execute(get_taxonid, {'taxonname': val_dict['taxonname']})
            taxonid = cur.fetchone()
            if taxonid != None:
                taxonid = int(taxonid[0])
            else:
                counter +=1
                taxonid = counter #placeholder
            
            val_dict['value'] = [None if item == 'NA' else item for item in val_dict['value']]
            inputs2['variableelementid'] = [None if item == 'NA' else item for item in inputs2['variableelementid']] # None
            inputs2['variablecontextid'] = [None if item == 'NA' else item for item in inputs2['variablecontextid']] # None
            # Get UnitsID
            get_unitsid = """SELECT * FROM ndb.variableunits WHERE variableunits %% %(units)s;"""
            cur.execute(get_unitsid, {'units': val_dict['unitcolumn'][i]})
            unitsid = cur.fetchone()[0] # This is just getting the varunitsid
            var_dict = {'variableunitsid':unitsid, 
                         'taxonid': taxonid, 
                         'variableelementid': inputs2['variableelementid'][i], 
                         'variablecontextid': inputs2['variablecontextid'][i]}
            cur.execute(get_varid, var_dict)
            varid = cur.fetchone()
            if varid != None:
                varid = int(varid[0])
            else:
                cur.execute(var_query, {'taxonid': taxonid,
                                        'variableelementid': inputs2['variableelementid'][i],
                                        'variableunitsid': unitsid,
                                        'variablecontextid': inputs2['variablecontextid'][i]}) # inputs[i]['variablecontextid']})
                varid = cur.fetchone()[0]

                cur.execute(get_varid, {'taxonid': taxonid,
                                        'variableelementid': inputs2['variableelementid'][i],
                                        'variableunitsid': unitsid,
                                        'variablecontextid': inputs2['variablecontextid'][i]})

            try:
                input_dict = {'sampleid': int(uploader['samples']['samples'][i]),
                              'variableid': int(varid),
                              'value': val_dict['value'][i]}
                cur.execute(data_query, input_dict)
                result = cur.fetchone()[0]
                results_dict['data_points'].append(result)
                results_dict['valid'].append(True)

            except Exception as e:
                logging.error(f"Samples Data is not correct. {e}")
                input_dict = {'sampleid': int(uploader['samples']['samples'][i]),
                              'variableid': None, 'value': None}
                cur.execute(data_query, input_dict)
                result = cur.fetchone()[0]
                results_dict['data_points'].append(result)
                results_dict['valid'].append(False)
    
    results_dict['valid'] = all(results_dict['valid'])
    # Return to the default isolation level
    #cur.execute("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL DEFAULT")
    return results_dict