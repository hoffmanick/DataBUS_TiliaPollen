def validUnits(cur, yml_dict, df):   
    # Update the function documentation to reflect the current implementation and parameters
    """
    Validate the units in the DataFrame based on the vocabulary specified in the YAML configuration.

    Args:
        cur (psycopg2.extensions.cursor): Database cursor for executing SQL queries.
        yml_dict (dict): Dictionary containing metadata and configuration from a YAML file.
        df (pandas.DataFrame): DataFrame containing the data to be validated.

    Returns:
        dict: A dictionary with keys 'valid' (boolean indicating overall validation success),
              and 'message' (list of strings with validation messages for each column).
    """
    response = { 'valid': list(), 'message': list() }
    
    # Extract entries from the yml_dict that contain a vocab
    yml_dict = yml_dict['metadata']
    vocab_entries = [entry for entry in yml_dict if 'vocab' in entry and (entry['vocab'] is not None)]

    for entry in vocab_entries:
        column_values = df[entry['column']].tolist()
        if entry['vocab'] == ['fixed']:
            if all(elem == column_values[0] for elem in column_values):
                response['valid'].append(True)
                response['message'].append(f"✔ Column {entry['column']} contains valid units.")
            else:
                response['valid'].append(False)
                response['message'].append(f"✗ Column {entry['column']} should be unique. Multiple values found")
        else:
            if all(value in entry['vocab'] for value in column_values):
                response['valid'].append(True)
                response['message'].append(f"✔ Column {entry['column']} contains valid units.")
            else:
                response['valid'].append(False)
                response['message'].append(f"✗ Column {entry['column']} contains invalid units.")

    response['valid'] = all(response['valid'])
    return response

    # Todo: Consider using the csv file from nu.read_csv instead of a pd.DataFrame