import DataBUS.neotomaHelpers as nh
from DataBUS import Response

def valid_geopolitical_units(cur, yml_dict, csv_file):
    """_Validating geopolitical units"""
    response = Response()

    params = [
        "geopoliticalunit1",
        "geopoliticalunit2",
        "geopoliticalunit3",
        "geopoliticalunit4"
    ]

    inputs = nh.clean_inputs(
        nh.pull_params(params, yml_dict, csv_file, "ndb.sitegeopolitical")
    )
    inputs = {key: None if value in ([''], None) else value[0] for key, value in inputs.items()}

    query = """SELECT geopoliticalid FROM ndb.geopoliticalunits
                WHERE LOWER(geopoliticalname) = %(geopoliticalname)s"""
    
    geo_units = {}
    for unit in inputs:
        if inputs[unit]:
            cur.execute(query, {'geopoliticalname' : inputs[unit].lower()})
            answer = cur.fetchone()
            if not answer:
                answer = None
            else:
                answer = answer[0]
        else:
            answer = None
        geo_units[unit] = answer

    result = next((geo_units[key] for key in params[::-1] if geo_units[key] is not None), None)

    if any(value is not None for value in geo_units.values()):
        response.message.append(f"✔ Site GPUID in {result}.")
        response.valid.append(True)
    else:
        response.message.append(f"? Site GPUID not available in Neotoma.")
        response.valid.append(True)

    response.validAll = all(response.valid)
    return response