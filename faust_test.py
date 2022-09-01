import faust
from yapic import json
import ssl
from l3_atom.helpers.read_config import get_kafka_config
from dateutil import parser
from schema_registry.client import SchemaRegistryClient

config = get_kafka_config()

client = SchemaRegistryClient(url=config['SCHEMA_REGISTRY_URL'])

l3_trade_schema = client.get_schema("L3 Trade")
print(l3_trade_schema)

ssl_ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)

if 'KAFKA_SASL_KEY' in config:
    app = faust.App("faust_test", broker=f"aiokafka://{config['KAFKA_BOOTSTRAP_SERVERS']}", broker_credentials=faust.SASLCredentials(username=config['KAFKA_SASL_KEY'], password=config['KAFKA_SASL_SECRET'], ssl_context=ssl_ctx, mechanism="PLAIN"))
else:
    app = faust.App("faust_test", broker=f"aiokafka://{config['KAFKA_BOOTSTRAP_SERVERS']}")

app.conf.consumer_auto_offset_reset = 'latest'

coinbase_raw_topic = app.topic("coinbase-raw")

normalised_topics = {
    'coinbase-lob_l3': app.topic("coinbase-lob_l3"),
    'coinbase-trades_l3': app.topic("coinbase-trades_l3")
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
    timestamp: float

class L3Lob(faust.Record):
    exchange: str
    symbol: str
    price: float
    size: float
    side: str
    order_id: str
    timestamp: float

def _normalize_symbol(symbol):
    return symbol.replace('-', '.')

async def _trade(message):
    symbol = _normalize_symbol(message['product_id'])
    price = float(message['price'])
    size = float(message['size'])
    side = message['side']
    trade_id = message['trade_id']
    maker_order_id = message['maker_order_id']
    taker_order_id = message['taker_order_id']
    timestamp = parser.isoparse(message['time']).timestamp()
    print(f"TRADE: {symbol} {side} {price} {size} {trade_id} {maker_order_id} {taker_order_id} {timestamp}")
    await normalised_topics["coinbase-trades_l3"].send(value=L3Trade(
        exchange='coinbase',
        symbol=symbol,
        price=price,
        size=size,
        taker_side=side,
        trade_id=trade_id,
        maker_order_id=maker_order_id,
        taker_order_id=taker_order_id,
        timestamp=timestamp
    ), key=symbol)

async def _open(message):
    symbol = _normalize_symbol(message['product_id'])
    price = float(message['price'])
    size = float(message['remaining_size'])
    side = 'ask' if message['side'] == 'sell' else 'bid'
    order_id = message['order_id']
    timestamp = parser.isoparse(message['time']).timestamp()
    await normalised_topics["coinbase-lob_l3"].send(value=L3Lob(
        exchange='coinbase',
        symbol=symbol,
        price=price,
        size=size,
        side=side,
        order_id=order_id,
        timestamp=timestamp
    ), key=symbol)
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
            await _open(msg)
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

