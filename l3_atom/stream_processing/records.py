# Defines schemas for records in the stream processing pipeline and prepares for Avro serialization
import faust
from decimal import Decimal

class BaseRecord(faust.Record):
    exchange: str
    symbol: str
    event_timestamp: int
    atom_timestamp: int

class Trade(BaseRecord, serializer='trades'):
    price: Decimal
    size: Decimal
    taker_side: str
    trade_id: str

class Lob(BaseRecord, serializer='lob'):
    price: Decimal
    size: Decimal
    side: str

class Candle(BaseRecord, serializer='candle'):
    start: int
    end: int
    interval: str
    trades: int
    closed: bool
    o: Decimal
    h: Decimal
    l: Decimal
    c: Decimal
    v: Decimal

class TradeL3(BaseRecord, serializer='trades_l3'):
    price: Decimal
    size: Decimal
    taker_side: str
    trade_id: str
    maker_order_id: str
    taker_order_id: str

class LobL3(BaseRecord, serializer='lob_l3'):
    price: Decimal
    size: Decimal
    side: str
    order_id: str

class Ticker(BaseRecord, serializer='ticker'):
    ask_price: Decimal
    ask_size: Decimal
    bid_price: Decimal
    bid_size: Decimal