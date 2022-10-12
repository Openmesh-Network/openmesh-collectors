import time


def enrich_raw(msg):
    """Enriches raw data with additional information.

    Messages come in one at a time form the websocket, so this function
    takes and processes individual emssages one at a time.
    """
    if isinstance(msg, dict):
        msg['atom_timestamp'] = int(time.time_ns() // 1000)
    elif isinstance(msg, list):
        msg.append(int(time.time_ns() // 1000))
    else:
        raise TypeError(
            f"enriching raw data of type {type(msg)} not supported")
    return msg
