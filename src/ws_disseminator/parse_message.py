import json

topics  = [
    "binance",
    "bitfinex",
    "bybit",
    "coinbase",
    "deribit",
    "ftx",
    "huobi",
    "kraken",
    "kraken-futures",
    "kucoin",
    "okex",
    "phemex",
    "BTCUSD",
    "BTCUSDT"
]

suffixes = [
    "normalised",
    "raw"
]

def is_valid(message):
    try:
        message = json.loads(message)
    except ValueError as e:
        print(e)
        return 1

    if "op" not in message.keys() and "topic" not in message.keys():
        return 2

    if message['op'] not in ("subscribe", "unsubscribe"):
        return 3
    
    chunks = message['topic'].split('-')
    if not len(chunks) == 2 and not len(chunks) == 3:
        return 4
    
    if not chunks[0] in topics:
        return 5
    
    if not message['topic'].endswith("normalised") and not message['topic'].endswith("raw"):
        return 6
            
    return 0


def is_subscribe(message):
    try:
        message = json.loads(message)
    except ValueError as e:
        print(e)
        return False
    
    if "op" in message.keys():
        if message['op'] == "subscribe":
            return True
    return False

def is_unsubscribe(message):
    try:
        message = json.loads(message)
    except ValueError as e:
        print(e)
        return False
    
    if "op" in message.keys():
        if message['op'] == "unsubscribe":
            return True
    return False

def sub_type(message):
    try:
        message = json.loads(message)
    except ValueError as e:
        print(e)
        return None
    
    if "topic" in message.keys() and message["topic"].endswith("-raw"):
        return "raw"
    return "normalised"