import json


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