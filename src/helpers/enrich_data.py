import time


def enrich_raw(msg):
    """Enriches raw data with additional information.

    Messages come in one at a time form the websocket, so this function
    takes and processes individual emssages one at a time.    
    """
    if isinstance(msg, dict):
        msg['receive_timestamp'] = int(time.time() * 10**3)
    elif isinstance(msg, list):
        msg.append(int(time.time() * 10**3))
    else:
        raise TypeError(f"enriching raw data of type {type(msg)} not supported")
    return msg

def enrich_lob_events(lob_events):
    pass

def enrich_market_orders(market_orders):
    pass