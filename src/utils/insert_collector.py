from DataBUS import Contact, Response
import DataBUS.neotomaHelpers as nh

def insert_collector(cur, yml_dict, csv_file, uploader):
    """
    Inserts data processors into Neotoma

    Args:
        cur (cursor object): Database cursor to execute SQL queries.
        yml_dict (dict): Dictionary containing YAML data.
        csv_file (str): File path to the CSV template.
        uploader (dict): Dictionary containing uploader details.

    Returns:
        response (dict): A dictionary containing information about the inserted data processors.
            - 'processorid' (list): List of processors' IDs.
            - 'valid' (bool): Indicates if all insertions were successful.
    """
    response = Response()
    inputs = nh.pull_params(["contactid"], yml_dict, csv_file, "ndb.collectors")

    # Use this method to preserve order.
    inputs["contactid"] = list(dict.fromkeys(inputs["contactid"]))
    if len(inputs["contactid"]) == 1 and isinstance(inputs["contactid"][0],str):
        inputs["contactid"] = inputs["contactid"][0].split(" | ")
    inputs["contactid"] = list(dict.fromkeys(inputs["contactid"]))
    contids = nh.get_contacts(cur, inputs["contactid"])
    for agent in contids:
        try:
            if agent['id']:
                contact = Contact(contactid=agent["id"])
                response.valid.append(True)
                marker = True
            else:
                response.valid.append(False)
                contact = Contact(contactid=None)
                response.message.append(f"✗ Contact {agent['name']} does not exist in Neotoma.")
                marker = False
        except Exception as e:
            contact = Contact(contactid=None)
            response.valid.append(False)
            response.message.append(f"Creating temporary insert object: {e}")
        if marker == True:
            try:
                contact.insert_collector(
                    cur, collunitid=uploader["collunitid"].cuid
                )
                response.valid.append(True)
                response.message.append(f"✔ Collector {agent['id']} inserted.")
            except Exception as e:
                response.message.append(
                    f"✗ Data collector information is not correct. {e}"
                )
                response.valid.append(False)

    response.collector = contids
    response.validAll = all(response.valid)
    return response
