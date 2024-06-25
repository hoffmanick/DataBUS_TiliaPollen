import neotomaHelpers as nh

def insert_sample_analyst(cur, yml_dict, csv_template, uploader):
    """
    Inserts sample analyst data into Neotoma

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_template (str): File path to the CSV template.
        uploader (dict): Dictionary containing uploader details.

    Returns:
        response (dict): A dictionary containing information about the inserted sample analysts.
            - 'contids' (list): List of dictionaries containing details of the analysts' IDs.
            - 'valid' (bool): Indicates if all insertions were successful.
    """
    response = {'contids': list(), 'valid': list(), 'message': list()}
    params = ['contactid']
    inputs = nh.pull_params(params, yml_dict, csv_template, 'ndb.sampleanalysts')

    inputs['contactid'] = list(dict.fromkeys(inputs['contactid']))
    contids = nh.get_contacts(cur, inputs['contactid'])
    response['contids'] = contids

    inserter = """
                SELECT ts.insertsampleanalyst(_sampleid := %(sampleid)s,
                                              _contactid := %(contactid)s,
                                              _analystorder := %(analystorder)s)
                """
    
    for i in range(len(uploader['samples']['samples'])):
        for contact in contids:
            try:
                cur.execute(inserter, {'sampleid': int(uploader['samples']['samples'][i]), 
                                       'contactid': int(contact['id']),
                                       'analystorder': int(contact['order'])})
                response['valid'].append(True)
                response['message'].append(f"✔  Added Sample Analyst {contact['id']} for sample {uploader['samples']['samples'][i]}.")

            except Exception as e:
                response['message'].append(f"✗ Sample Analyst data is not correct. {e}")
                response['message'].append(f"Executed temporary query.")
                cur.execute(inserter, {'sampleid': int(uploader['samples']['samples'][i]), 
                                       'contactid': None,
                                       'analystorder': None})
                response['valid'].append(False)

    response['valid'] = all(response['valid'])
    return response