import faust
import json

from l3_atom.helpers.read_config import get_kafka_config

config = get_kafka_config()

app = faust.App("faust_test", broker=f"aiokafka://{config['KAFKA_BOOTSTRAP_SERVERS']}")

app.conf.consumer_auto_offset_reset = 'latest'

coinbase_raw_topic = app.topic("coinbase-raw")

normalised_topics = {
    'coinbase-lob_l3-BTC.USD': app.topic("coinbase-lob_l3-BTC.USD"),
    'coinbase-lob_l3-ETH.USD': app.topic("coinbase-lob_l3-ETH.USD"),
    'coinbase-trades_l3-BTC.USD': app.topic("coinbase-trades_l3-BTC.USD"),
    'coinbase-trades_l3-ETH.USD': app.topic("coinbase-trades_l3-ETH.USD")
}

class L3Trade(faust.Record):
    exchange: str
    symbol: str
    price: float
    size: float
    taker_side: str
    trade_id: str
    maker_order_id: str
    taker_order_id: str
    timestamp: str

def _normalize_symbol(symbol):
    return symbol.replace('-', '.')

async def _trade(message):
    symbol = _normalize_symbol(message['product_id'])
    price = message['price']
    size = message['size']
    side = message['side']
    trade_id = message['trade_id']
    maker_order_id = message['maker_order_id']
    taker_order_id = message['taker_order_id']
    timestamp = message['time']
    # print(f"TRADE: {symbol} {side} {price} {size} {trade_id} {maker_order_id} {taker_order_id} {timestamp}")
    await normalised_topics[f"coinbase-trades_l3-{symbol}"].send(value=L3Trade(
        exchange='coinbase',
        symbol=symbol,
        price=price,
        size=size,
        taker_side=side,
        trade_id=trade_id,
        maker_order_id=maker_order_id,
        taker_order_id=taker_order_id,
        timestamp=timestamp
    ))

async def _open(message):
    symbol = _normalize_symbol(message['product_id'])
    price = message['price']
    size = message['remaining_size']
    side = 'ask' if message['side'] == 'sell' else 'bid'
    order_id = message['order_id']

    # print(f"NEW ORDER: {symbol} {side} {price} {size} {order_id}")
    # normalised_topics[f"coinbase-l3-lob{symbol}"].send(value=json.dumps(message))

async def _done(message):
    if 'price' not in message or not message['price']:
        return
    symbol = _normalize_symbol(message['product_id'])
    price = message['price']
    size = message['remaining_size']
    side = message['side']
    order_id = message['order_id']
    timestamp = message['time']
    # print(f"{message['reason'].upper()}: {symbol} {side} {price} {size} {order_id} {timestamp}")

async def handle_coinbase_message(msg):
    if 'type' in msg:
        if msg['type'] == 'match' or msg['type'] == 'last_match':
            await _trade(msg)
        elif msg['type'] == 'open':
            pass
        elif msg['type'] == 'done':
            pass
        elif msg['type'] == 'change':
            pass
        elif msg['type'] == 'received':
            pass
        elif msg['type'] == 'activate':
            pass
        elif msg['type'] == 'subscriptions':
            pass

@app.agent(coinbase_raw_topic)
async def coinbase(stream):
    async for msg in stream:
        await handle_coinbase_message(msg)

