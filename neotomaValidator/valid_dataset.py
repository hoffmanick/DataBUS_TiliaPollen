from neotomaHelpers.pull_params import pull_params
from neotomaHelpers.retrieve_dict import retrieve_dict
def valid_dataset(cur, yml_dict, csv_file):
    """_Validating Datasets_"""
    response = {'valid': [],
                'message': []}
    
    inputs = dict()
    #params = ['datasetname', 'datasettypeid']
    ds_name = retrieve_dict(yml_dict, 'ndb.datasets.datasetname')
    inputs['datasetname'] = ds_name[0]
    ds_id = retrieve_dict(yml_dict, 'ndb.datasettypes.datasettypeid')
    inputs['datasettypeid'] = ds_id[0]

    query = "SELECT DISTINCT datasettypeid FROM ndb.datasettypes"
    cur.execute(query)
    
    all_datasets = cur.fetchall()
    all_datasets = [value[0] for value in all_datasets]

    ds_type = inputs['datasettypeid']['value'].lower()

    query = "SELECT datasettypeid FROM ndb.datasettypes WHERE LOWER(datasettype) = %(ds_type)s"
    cur.execute(query,{'ds_type': ds_type})
    datasetid = cur.fetchone()[0]

    if datasetid in all_datasets:
        response['message'].append("✔ Dataset type exists in neotoma.")
        response['valid'].append(True)
    else:
        response['message'].append(f"✗ Dataset type is not known to neotoma. Add it first")
        response['valid'].append(False)

    response['valid'] = all(response['valid'])
    
    return response