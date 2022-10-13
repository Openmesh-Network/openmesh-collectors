from typing import Literal
import faust
from decimal import Decimal


class BaseRecord(faust.Record):
    """
    Base record for all records.
    
    :param exchange: The exchange the message is sourced from
    :type exchange: str
    :param symbol: The symbol the message is for
    :type symbol: str
    :param event_timestamp: The timestamp of the event (in milliseconds)
    :type event_timestamp: int
    :param atom_timestamp: The timestamp of when the message was received (in microseconds)
    :type atom_timestamp: int
    """
    exchange: str
    symbol: str
    event_timestamp: int
    atom_timestamp: int


class Trade(BaseRecord, serializer='trades'):
    """
    Record for a trade without order IDs.

    :param price: The price of the trade
    :type price: Decimal
    :param size: The size of the trade
    :type size: Decimal
    :param taker_side: The side of the taker, either 'buy' or 'sell'
    :type taker_side: Literal['buy', 'sell']
    :param trade_id: The ID of the trade
    :type trade_id: str
    """
    price: Decimal
    size: Decimal
    taker_side: Literal['buy', 'sell']
    trade_id: str


class Lob(BaseRecord, serializer='lob'):
    """
    Record for a level 2 order book update.

    :param price: The price of the order
    :type price: Decimal
    :param size: The size of the order
    :type size: Decimal
    :param side: The side of the order, either 'buy' or 'sell'
    :type side: Literal['buy', 'sell']
    """
    price: Decimal
    size: Decimal
    side: Literal['buy', 'sell']


class Candle(BaseRecord, serializer='candle'):
    """
    Record for a candlestick event.

    :param start: The start time of the candle
    :type start: int
    :param end: The end time of the candle
    :type end: int
    :param interval: The interval of the candle
    :type interval: str
    :param trades: The number of trades in the candle
    :type trades: int
    :param closed: Whether the candle is closed
    :type closed: bool
    :param o: The open price of the candle
    :type o: Decimal
    :param h: The high price of the candle
    :type h: Decimal
    :param l: The low price of the candle
    :type l: Decimal
    :param c: The close price of the candle
    :type c: Decimal
    :param v: The volume of the candle
    :type v: Decimal
    """
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


class TradeL3(Trade, serializer='trades_l3'):
    """
    Record for a trade with IDs.

    :param maker_order_id: The ID of the market maker order
    :type maker_order_id: str
    :param taker_order_id: The ID of the taker order
    :type taker_order_id: str
    """
    maker_order_id: str
    taker_order_id: str


class LobL3(Lob, serializer='lob_l3'):
    """
    Record for a level 3 order book update (with IDs).

    :param order_id: The ID of the order
    :type order_id: str
    """
    order_id: str


class Ticker(BaseRecord, serializer='ticker'):
    """
    Record for a ticker update.

    :param bid_price: The best bid price
    :type bid_price: Decimal
    :param bid_size: The best bid size
    :type bid_size: Decimal
    :param ask_price: The best ask price
    :type ask_price: Decimal
    :param ask_size: The best ask size
    :type ask_size: Decimal
    """
    ask_price: Decimal
    ask_size: Decimal
    bid_price: Decimal
    bid_size: Decimal


class FundingRate(BaseRecord, serializer='funding_rate'):
    """
    Record for a funding rate update.

    :param mark_price: The mark price
    :type mark_price: Decimal
    :param funding_rate: The funding rate
    :type funding_rate: Decimal
    :param next_funding_time: The timestamp of the next funding
    :type next_funding_time: int
    :param predicted_rate: The predicted funding rate
    :type predicted_rate: Decimal
    """
    mark_price: Decimal
    funding_rate: Decimal
    next_funding_time: int
    predicted_rate: Decimal


class OpenInterest(BaseRecord, serializer='open_interest'):
    """
    Record for an open interest update.

    :param open_interest: The open interest
    """
    open_interest: Decimal


record_mapping = {
    'lob_l3': LobL3,
    'trades_l3': TradeL3,
    'ticker': Ticker,
    'lob': Lob,
    'trades': Trade,
    'candle': Candle,
    'funding_rate': FundingRate,
    'open_interest': OpenInterest
}
