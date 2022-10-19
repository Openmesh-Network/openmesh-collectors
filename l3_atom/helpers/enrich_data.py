month_codes = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']


def enrich_raw(msg, ts):
    """Enriches raw data with additional information.

    Messages come in one at a time form the websocket, so this function
    takes and processes individual emssages one at a time.
    """
    if isinstance(msg, dict):
        msg['atom_timestamp'] = ts
    elif isinstance(msg, list):
        msg.append(ts)
    else:
        raise TypeError(
            f"enriching raw data of type {type(msg)} not supported")
    return msg


def month_code(month: int):
    """Given a month, converts it to the corresponding code for derivatives"""
    return month_codes[month - 1]
