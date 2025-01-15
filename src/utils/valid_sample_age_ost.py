import DataBUS.neotomaHelpers as nh
from DataBUS import SampleAge, Response


def valid_sample_age_ost(cur, yml_dict, csv_file, validator):
    """
    Inserts sample age data into a database.

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_file (str): File path to the CSV template.
        uploader (dict): Dictionary containing uploader details.

    Returns:
        response (dict): A dictionary containing information about the inserted sample ages.
            - 'sampleAge' (list): List of IDs for the inserted sample age data.
            - 'valid' (bool): Indicates if all insertions were successful.
    """
    response = Response() 

    params = ["age"]
    try:
        inputs = nh.clean_inputs(
            nh.pull_params(params, yml_dict, csv_file, "ndb.sampleages")
        ) #1950 as year zero, so 2010 is -60
        inputs['age'] = 1950 - inputs['age'][0].year
    except Exception as e:
        error_message = str(e)
        try:
            if "time data" in error_message.lower():
                event_dates = [item.get('eventDate') for item in csv_file if 'eventDate' in item]
                new_date = list(set(event_dates))
                assert len(new_date) == 1, "There should only be one date"
                new_date = new_date[0]
                if isinstance(new_date, str) and len(new_date) > 4:
                    if len(new_date) == 7 and new_date[4] == '-' and new_date[5:7].isdigit():
                        new_date = f"{new_date}-01"
                        new_date = 1950 - new_date.year
                    elif new_date[:4].isdigit():
                        new_date = int(new_date[:4])
                        new_date = 1950 - new_date
                    else:
                        new_date = None
            params.remove("age")
            inputs = nh.clean_inputs(
            nh.pull_params(params, yml_dict, csv_file, "ndb.sampleages") ) 
            inputs["age"] = new_date
            response.valid.append(True)
        except Exception as inner_e:
            response.validAll = False
            response.message.append("CU parameters cannot be properly extracted. {e}\n")
            response.message.append(str(inner_e))
            return response

    # inputs['age'] = [float(value) if value != 'NA' else None for value in inputs['age']]
    if "uncertainty" in inputs:
        inputs["uncertainty"] = [
        float(value) if value != "NA" else None for value in inputs["uncertainty"]
    ]

    for i in range(0, validator["sample"].sa_counter):
        if inputs["age"] and isinstance(inputs["age"], (int, float)):
            age_younger = inputs["age"]
            age_older = inputs["age"]
        else:
            response.message.append(
                "? Age is set to None. Ageyounger/Ageolder will be None."
            )
            age_younger = None
            age_older = None
        try:
            if inputs['age']:
                sa = SampleAge(
                    sampleid=2,  # Placeholder
                    chronologyid=2,  # Placeholder
                    age=inputs["age"],
                    ageyounger=age_younger,
                    ageolder=age_older,
                )
                response.valid.append(True)
            else:
                sa = SampleAge(
                    sampleid=2,  # Placeholder
                    chronologyid=2,  # Placeholder
                    age=None,
                    ageyounger=age_younger,
                    ageolder=age_older,
                )
                response.valid.append(True)
        except Exception as e:
            response.valid.append(False)
            response.message.append(f"✗ Samples ages cannot be created. {e}")

    response.validAll = all(response.valid)
    if response.validAll:
        response.message.append(f"✔ Sample ages can be created.")
    return response
