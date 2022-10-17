import json


def mock_process_message(ret, **kwargs):
    async def process_message(cls, msg, conn, channel):
        msg = json.loads(msg)
        ret.append(msg)
    return process_message
