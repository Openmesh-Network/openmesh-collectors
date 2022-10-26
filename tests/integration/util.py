import json


def mock_process_message(ret, **kwargs):
    async def process_message(cls, msg, conn, channel):
        msg = json.loads(msg)
        key = cls.get_key(msg)
        print(key)
        ret.append(msg)
    return process_message
