def validUnits (csv_vocab, vocab_dict):    
    """_Are the units provided valid based on defined unit names?_
    We pass in two dictionaries, that are expected to have matching keys in the dicts.
    For each key in `unitcols` (the columns in the template that contain units) we then
    have a key in `units` that contains the valid units for that column.

    Args:
        template (_list_): _The csv file content, as a pd.DataFrame._
        unitcols (_dict_): _The names of each set of columns listing units in the file, with a key linked to the `units` column._
        units (_dict_): _Acceptable units for each data column type._

    Returns:
        _list_: _A list of columns with invalid units._
    """    
    response = { 'pass': False, 'message': [] }

    for key, values in vocab_dict.items():
        # Retrieve the values in the csv file
        column_values = csv_vocab[key].tolist()
        # Check that the csv_vocab values are valid according to the YAML
        if vocab_dict[key] == ['fixed']:
            # check that all rows are the same
            valid = all(elem == column_values[0] for elem in column_values) 
        else:
            # check the elements match with the template
            valid = all(value in values for value in column_values)
        
        if valid == False:
            response['message'].append(f"✗ Column {key} contains units that do not follow the expected set.")

    if len(response['message']) == 0:
        response['pass'] = True
        response['message'].append(f"✔ All units validate.")
    return response

    # Todo: Consider using the csv file from nu.read_csv instead of a pd.DataFrame
    # Currently, the pull params function cannot be used as it has the `table` requirement
    # and needs the yml file too - we could mix the nu.VocabDict as it works with the template
    # then pull params to clean the csv file to only retrieve params we are interested in
    # For now, the easy work around is using pandas as above.