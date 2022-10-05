import faust
from faust.serializers import codecs
from yapic import json
import ssl
import time
from l3_atom.helpers.read_config import get_kafka_config
from dateutil import parser
from schema_registry.client import SchemaRegistryClient, schema, Auth
from schema_registry.serializers.faust import FaustSerializer
from decimal import Decimal

config = get_kafka_config()

ssl_ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)

client = SchemaRegistryClient(url=config['SCHEMA_REGISTRY_URL'], auth=Auth(username=config['SCHEMA_REGISTRY_API_KEY'], password=config['SCHEMA_REGISTRY_API_SECRET']))

l3_trades_schema = client.get_schema("L3_Trade")
l3_lob_schema = client.get_schema("L3_LOB")
ticker_schema = client.get_schema("Ticker")

l3_trades_serializer = FaustSerializer(client, 'coinbase_trades_l3', l3_trades_schema.schema)
l3_lob_serializer = FaustSerializer(client, 'coinbase_lob_l3', l3_lob_schema.schema)
ticker_serializer = FaustSerializer(client, 'coinbase_ticker', ticker_schema.schema)

codecs.register('l3_trades', l3_trades_serializer)
codecs.register('l3_lob', l3_lob_serializer)
codecs.register('ticker', ticker_serializer)

if 'KAFKA_SASL_KEY' in config:
    app = faust.App("schema-standardiser", broker=f"aiokafka://{config['KAFKA_BOOTSTRAP_SERVERS']}", broker_credentials=faust.SASLCredentials(username=config['KAFKA_SASL_KEY'], password=config['KAFKA_SASL_SECRET'], ssl_context=ssl_ctx, mechanism="PLAIN"))
else:
    app = faust.App("schema-standardiser", broker=f"aiokafka://{config['KAFKA_BOOTSTRAP_SERVERS']}")



app.conf.consumer_auto_offset_reset = 'latest'

coinbase_raw_topic = app.topic("coinbase-raw")

normalised_topics = {
    'coinbase_lob_l3': app.topic("coinbase_lob_l3"),
    'coinbase_trades_l3': app.topic("coinbase_trades_l3"),
    'coinbase_ticker': app.topic("coinbase_ticker")
}

class L3Trade(faust.Record, serializer='l3_trades'):
    exchange: str
    symbol: str
    price: Decimal
    size: Decimal
    taker_side: str
    trade_id: str
    maker_order_id: str
    taker_order_id: str
    event_timestamp: int
    atom_timestamp: int

class L3Lob(faust.Record, serializer='l3_lob'):
    exchange: str
    symbol: str
    price: Decimal
    size: Decimal
    side: str
    order_id: str
    event_timestamp: int
    atom_timestamp: int

class Ticker(faust.Record, serializer='ticker'):
    exchange: str
    symbol: str
    ask_price: Decimal
    ask_size: Decimal
    bid_price: Decimal
    bid_size: Decimal
    event_timestamp: int
    atom_timestamp: int

def _normalize_symbol(symbol):
    return symbol.replace('-', '.') if '-' in symbol else symbol

async def _trade(message):
    symbol = _normalize_symbol(message['product_id'])
    price = Decimal(message['price'])
    size = Decimal(message['size'])
    side = message['side']
    trade_id = str(message['trade_id'])
    maker_order_id = message['maker_order_id']
    taker_order_id = message['taker_order_id']
    event_timestamp = int(parser.isoparse(message['time']).timestamp() * 1000)
    atom_timestamp = message['atom_timestamp']
    await normalised_topics["coinbase_trades_l3"].send(value=L3Trade(
        exchange='coinbase',
        symbol=symbol,
        price=price,
        size=size,
        taker_side=side,
        trade_id=trade_id,
        maker_order_id=maker_order_id,
        taker_order_id=taker_order_id,
        event_timestamp=event_timestamp,
        atom_timestamp=atom_timestamp
    ), key=symbol)

async def _open(message):
    symbol = _normalize_symbol(message['product_id'])
    price = Decimal(message['price'])
    size = Decimal(message['remaining_size'])
    side = 'ask' if message['side'] == 'sell' else 'bid'
    order_id = message['order_id']
    event_timestamp = int(parser.isoparse(message['time']).timestamp() * 1000)
    atom_timestamp = message['atom_timestamp']
    await normalised_topics["coinbase_lob_l3"].send(value=L3Lob(
        exchange='coinbase',
        symbol=symbol,
        price=price,
        size=size,
        side=side,
        order_id=order_id,
        event_timestamp=event_timestamp,
        atom_timestamp=atom_timestamp
    ), key=symbol)

async def _done(message):
    if 'price' not in message or not message['price']:
        return
    symbol = _normalize_symbol(message['product_id'])
    price = Decimal(message['price'])
    size = Decimal(message['remaining_size'])
    side = message['side']
    order_id = message['order_id']
    event_timestamp = int(parser.isoparse(message['time']).timestamp() * 1000)
    atom_timestamp = message['atom_timestamp']
    await normalised_topics["coinbase_lob_l3"].send(value=L3Lob(
        exchange='coinbase',
        symbol=symbol,
        price=price,
        size=size,
        side=side,
        order_id=order_id,
        event_timestamp=event_timestamp,
        atom_timestamp=atom_timestamp
    ), key=symbol)

async def _change(message):
    if 'price' not in message or not message['price']:
        return
    symbol = _normalize_symbol(message['product_id'])
    price = Decimal(message['price'])
    size = Decimal(message['remaining_size'])
    side = message['side']
    order_id = message['order_id']
    event_timestamp = int(parser.isoparse(message['time']).timestamp() * 1000)
    atom_timestamp = message['atom_timestamp']
    await normalised_topics["coinbase_lob_l3"].send(value=L3Lob(
        exchange='coinbase',
        symbol=symbol,
        price=price,
        size=size,
        side=side,
        order_id=order_id,
        event_timestamp=event_timestamp,
        atom_timestamp=atom_timestamp
    ), key=symbol)

async def _ticker(message):
    symbol = _normalize_symbol(message['product_id'])
    ask_price = Decimal(message['best_ask'])
    bid_price = Decimal(message['best_bid'])
    event_timestamp = int(parser.isoparse(message['time']).timestamp() * 1000)
    atom_timestamp = message['atom_timestamp']
    await normalised_topics["coinbase_ticker"].send(value=Ticker(
        exchange='coinbase',
        symbol=symbol,
        ask_price=ask_price,
        ask_size=-1,
        bid_price=bid_price,
        bid_size=-1,
        event_timestamp=event_timestamp,
        atom_timestamp=atom_timestamp
    ), key=symbol)

async def handle_coinbase_message(msg):
    if 'type' in msg:
        if msg['type'] == 'match' or msg['type'] == 'last_match':
            await _trade(msg)
        elif msg['type'] == 'open':
            await _open(msg)
        elif msg['type'] == 'done':
            await _done(msg)
        elif msg['type'] == 'change':
            await _change(msg)
        elif msg['type'] == 'batch_ticker' or msg['type'] == 'ticker':
            await _ticker(msg)
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

