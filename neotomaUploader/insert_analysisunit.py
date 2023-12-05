from .retrieve_dict import retrieve_dict
from .clean_column import clean_column
import numpy as np
import logging
from .pull_params import pull_params

def insert_analysisunit(cur, yml_dict, csv_template, uploader):
    """_Inserting analysis units_

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
    results_dict = {'anunits': [], 'valid': []}

    add_unit = """
    SELECT ts.insertanalysisunit(_collectionunitid := %(collunitid)s,
                                 _depth := %(depth)s,
                                 _thickness := %(thickness)s,
                                  _faciesid := %(faciesid)s,
                                  _mixed := %(mixed)s,
                                  _igsn := %(igsn)s,
                                  _notes := %(notes)s)
    """

    params = ["analysisunitname", "depth", "thickness", "faciesid", "mixed", "igsn", "notes"]
    inputs = pull_params(params, yml_dict, csv_template, 'ndb.analysisunits')
   
    for i in range(0, len(inputs['depth'])):
        if inputs['mixed'][i] == None:
            mixed_input = False
        else:
            mixed_input = inputs['mixed'][i]
        
        try:
            cur.execute(add_unit, {'collunitid': uploader['collunitid']['collunitid'],
                                    'depth': inputs['depth'][i],
                                    'thickness': inputs['thickness'][i],
                                    'faciesid': inputs['faciesid'][i],
                                    'mixed': mixed_input,
                                    'igsn': inputs['igsn'][i],
                                    'notes': inputs['notes'][i]})
            anunitid = cur.fetchone()[0]
            results_dict['anunits'].append(anunitid)
            results_dict['valid'].append(True)
        
        except Exception as e:
            logging.error(f"Analysis Unit Data is not correct. Error message: {e}")
            cur.execute(add_unit, {'collunitid': uploader['collunitid']['collunitid'],
                                    'depth': np.nan,
                                    'thickness': np.nan,
                                    'faciesid': np.nan,
                                    'mixed': np.nan,
                                    'igsn': np.nan,
                                    'notes': 'NULL'})
            anunitid = cur.fetchone()[0]
            results_dict['anunits'].append(anunitid)
            results_dict['valid'].append(False)
    
    results_dict['valid'] = all(results_dict['valid'])
    return results_dict