import DataBUS.neotomaHelpers as nh
from DataBUS import SampleAge, Response

def insert_sample_age_ost(cur, yml_dict, csv_file, uploader):
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

    for i in range(len(uploader["samples"].sampleid)):
        if not inputs["age"] and isinstance(inputs["age"], (int, float)):
            age_younger = None
            age_older = None
        else:
            response.message.append(
                "? Age is set to None. Ageyounger/Ageolder will be None."
            )
            age_younger = None
            age_older = None
        try:
            if inputs['age']:
                sa = SampleAge(
                sampleid=int(uploader["samples"].sampleid[i]),
                chronologyid=int(uploader["chronology"].chronid),
                age=inputs["age"],
                ageyounger=age_younger,
                ageolder=age_older,
            )
                response.valid.append(True)
        except Exception as e:
            sa = SampleAge()
            response.valid.append(False)
            response.message.append(f"✗ Samples ages cannot be created. {e}")
        finally:
            try:
                sa_id = sa.insert_to_db(cur)
                response.valid.append(True)
                response.message.append(
                    f"✔ Added sample age {sa_id} for sample {uploader['samples'].sampleid[i]}"
                )
            except Exception as e:
                response.message.append(f"✗ Cannot add Samples Age: {e}")
                response.valid.append(False)

    response.validAll = all(response.valid)
    if response.validAll:
        response.message.append(f"✔ Sample ages can be created.")
    return response
