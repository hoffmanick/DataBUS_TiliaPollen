def clean_numbers(coll_info):
    try:
        num = int(coll_info)
    except (ValueError, TypeError):
        try:
            num = float(coll_info)
        except (ValueError, TypeError):
            num = None
    return num