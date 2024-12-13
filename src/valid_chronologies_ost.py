import DataBUS.neotomaHelpers as nh
from DataBUS import Chronology, ChronResponse


def valid_chronologies_ost(cur, yml_dict, csv_file):
    """_Validating chronologies"""
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
        inputs["ageboundyounger"] = inputs["age"]
        inputs["ageboundolder"] = inputs["age"]
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

    try:
        Chronology(
            chronologyid=inputs["chronologyid"],
            agetypeid=inputs["agetypeid"],
            contactid=inputs["contactid"],
            isdefault=1,
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
        response.message.append("✔  Chronology can be created")
    except Exception as e:
        response.valid.append(False)
        response.message.append(f"✗  Chronology cannot be created: {e}")
    response.validAll = all(response.valid)
    return response
