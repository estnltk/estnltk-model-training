from datetime import datetime


def etsa_date_string_to_date(text, suppress_exceptions=False):
    """
    Converts ETSA date string to the date

    If supress_exeptions is True then all errors are suppressed.

    TODO: To be replaced with PLSQL function inside SQL statement

    Copied from common/utils.py.
    """
    if text is None:
        return None
    elif len(text) == 8:
        date_format = "%Y%m%d"
    elif len(text) == 12:
        date_format = "%Y%m%d%H%M"
    elif len(text) == 14:
        date_format = "%Y%m%d%H%M%S"
    else:
        return None

    try:
        date = datetime.strptime(text, date_format)
    except ValueError:
        return None
    except Exception as e:
        if suppress_exceptions:
            return None
        print(e)
        raise e
    return date
