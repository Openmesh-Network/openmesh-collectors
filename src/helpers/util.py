from gzip import decompress
import json


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

def create_lob_event(quote_no=-1,
                    event_no=-1,
                    order_id=-1,
                    original_order_id=-1,
                    side=-1,
                    price=-1,
                    size=-1,
                    lob_action=0,
                    event_timestamp=-1,
                    send_timestamp=-1,
                    receive_timestamp=-1,
                    order_type=0,
                    is_implied=-1,
                    order_executed=0,
                    execution_price=-1,
                    executed_size=-1,
                    aggressor_side=-1,
                    matching_order_id=-1,
                    old_order_id=-1,
                    trade_id=-1,
                    size_ahead=-1,
                    orders_ahead=-1):
    return {
        "quote_no": quote_no,
        "event_no": event_no,
        "order_id": order_id,
        "original_order_id": original_order_id,
        "side": side,
        "price": price,
        "size": size,
        "lob_action": lob_action,
        "event_timestamp": event_timestamp,
        "send_timestamp": send_timestamp,
        "receive_timestamp": receive_timestamp,
        "order_type": order_type,
        "is_implied": is_implied,
        "order_executed": order_executed,
        "execution_price": execution_price,
        "executed_size": executed_size,
        "aggressor_side": aggressor_side,
        "matching_order_id": matching_order_id,
        "old_order_id": old_order_id,
        "trade_id": trade_id,
        "size_ahead": size_ahead,
        "orders_ahead": orders_ahead
    }

def create_market_order(order_id=-1,
                        price=-1,
                        trade_id="",
                        timestamp=-1,
                        side=-1,
                        size=-1,
                        msg_original_type=""):
    return {
        "order_id": order_id,
        "price": price,
        "trade_id": trade_id,
        "timestamp": timestamp,
        "side": side,
        "size": size,
        "msg_original_type": msg_original_type
    }