# Defines schemas for records in the stream processing pipeline and prepares for Avro serialization
import faust
from decimal import Decimal

class TradeL3(faust.Record, serializer='trades_l3'):
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

class LobL3(faust.Record, serializer='lob_l3'):
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