from gzip import decompress
import json
from typing import Union

async def preprocess(msg, *args):
    try:
        json.loads(msg)
    except (TypeError, UnicodeDecodeError) as e:
        # Huobi is annoying with the way they pingpong
        msg = decompress(msg)
        msg_dict = json.loads(msg)
        if 'ping' in msg_dict:
            await args[0].send(json.dumps({'pong': msg_dict['ping']}))
    return json.loads(msg)

def l3_order(
    order_id:str = "",
    price:float = -1,
    size:float = -1,
    event_type:str = ""
):
    return {
        "order_id": order_id,
        "price": price,
        "size": size,
        "event_type": event_type
    }

def l2_order(
    price:float = -1,
    size:float = -1,
    event_type:str = ""
):
    return {
        "price": price,
        "size": size,
        "event_type": event_type
    }

def trade(
    trade_id:str = "",
    price:float = -1,
    size:float = -1,
    side:int = -1,
    timestamp:float = -1,
):
    return {
        "trade_id": trade_id,
        "price": price,
        "size": size,
        "side": side,
        "timestamp": timestamp,
    }

def candle(
    exchange:str = "",
    symbol:str = "",
    start_time:float = -1,
    end_time:float = -1,
    open:float = -1,
    high:float = -1,
    low:float = -1,
    close:float = -1,
    volume:float = -1,
    interval:str = "",
    timestamp:float = -1,
    closed:bool = False
):
    return {
        "exchange": exchange,
        "symbol": symbol,
        "start_time": start_time,
        "end_time": end_time,
        "open": open,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "interval": interval,
        "timestamp": timestamp,
        "closed": closed
    }