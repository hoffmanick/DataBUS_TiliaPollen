def get_contacts(cur, contacts_list):
    get_contact = """SELECT * FROM ndb.contacts WHERE contactname %% %(name)s;"""
    baseid = 1
    contids = list()
    for i in contacts_list:
        cur.execute(get_contact, {'name': i})
        contids.append({'name': i, 'id': cur.fetchone()[0], 'order': baseid})
    return contids