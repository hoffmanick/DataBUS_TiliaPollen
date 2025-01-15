import DataBUS.neotomaHelpers as nh
from DataBUS import Chronology, ChronResponse

def insert_chronology_ost(cur, yml_dict, csv_file, uploader):
    """
    Inserts chronology data into Neotoma.

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_file (str): File path to the CSV template.
        uploader (dict): Dictionary containing uploader details.

    Returns:
        response (dict): Dictionary containing information about the inserted chronology.
        Contains keys:
            'chronology': ID of the inserted chronology.
            'valid': Boolean indicating if the insertion was successful.
    """
    response = ChronResponse()

    params = [
        "chronologyid",
        "collectionunitid",
        "contactid",
        "isdefault",
        "chronologyname",
        "dateprepared",
        "agemodel",
        "ageboundyounger",
        "ageboundolder",
        "notes",
        "recdatecreated",
        "recdatemodified",
        "age"
    ]

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
            response.message.append("Chronology parameters cannot be properly extracted. {e}\n")
            response.message.append(str(inner_e))
            return response

    if inputs['age']:
        inputs["agetype"] = "cal yr BP"
        inputs["agetypeid"] = 2
        inputs["agemodel"] = "collection date"
        inputs["chronologyname"] = "Calendar date"
        response.message.append("✔ The provided age type is correct.")
        response.valid.append(True)
    else:
        response.message.append("? No age type provided.")
        response.valid.append(True)
        inputs["agetypeid"] = None

    for k in inputs:
        if not inputs[k]:
            response.message.append(f"? {k} has no values.")
        else:
            response.message.append(f"✔ {k} looks valid.")
            response.valid.append(True)

    # if isinstance(inputs["contactid"], list):
    #     get_contact = """SELECT contactid FROM ndb.contacts WHERE LOWER(%(contactname)s) = contactname;"""
    #     cur.execute(get_contact, {"contactname": inputs["contactid"][0]})
    #     inputs["contactid"] = cur.fetchone()
    try:
        chron = Chronology(
            collectionunitid=uploader["collunitid"].cuid,
            chronologyid=inputs["chronologyid"],
            agetypeid=inputs["agetypeid"],
            contactid=inputs["contactid"],
            isdefault=inputs["isdefault"],
            chronologyname=inputs["chronologyname"],
            dateprepared=inputs["dateprepared"],
            agemodel=inputs["agemodel"],
            ageboundyounger=inputs["ageboundyounger"],
            ageboundolder=inputs["ageboundolder"],
            notes=inputs["notes"],
            recdatecreated=inputs["recdatecreated"],
            recdatemodified=inputs["recdatemodified"],
        )
        response.valid.append(True)
    except Exception as e:
        response.valid.append(False)
        response.message.append("✗  Chronology cannot be created {e}")
        chron = Chronology(collectionunitid=uploader["collunitid"].cuid)

    if isinstance(inputs["age"], (list)):
        chron.maxage = int(max(inputs["age"]))
        chron.minage = int(min(inputs["age"]))
    else:
        response.message.append("? Age is not iterable. Minage/maxage will be None.")

    try:
        chronid = chron.insert_to_db(cur)
        response.chronid = chronid
        response.valid.append(True)
        response.message.append(f"✔ Added Chronology {chronid}.")

    except Exception as e:
        response.message.append(
            f"✗  Chronology Data is not correct. Error message: {e}"
        )
        chron = Chronology(collectionunitid=uploader["collunitid"].cuid, agetypeid=1)
        chronid = chron.insert_to_db(cur)
        response.valid.append(False)
    response.validAll = all(response.valid)
    return response