import re
from .retrieve_dict import retrieve_dict

def pull_overwrite(params, yml_dict, table=None):
    """_Pull parameters overwrite value._

    Args:
        params (_list_): _A list of strings for the columns needed to generate the insert statement._
        yml_dict (_dict_): _A `dict` returned by the YAML template._
        table (_string_): _The name of the table the parameters are being drawn for._

    Returns:
        _dict_: _parameters with overwrite T/F value._
    """
    results = []
    if isinstance(table, str): 
        if re.match('.*\.$', table) == None:
            table = table + '.'
        result=dict()
        for i in params:
            valor = retrieve_dict(yml_dict, table + i)
            if len(valor) == 1:
                result[i] = valor[0]['overwrite']
            else:
                result[i] = False
        if 'geog' in params:
            result['coordlo'] =  result['geog']
            result['coordla'] = result['geog']
        return result
        
    elif isinstance(table, list):
        for item in table:
            results.append(pull_overwrite(params, yml_dict, item))
        return results