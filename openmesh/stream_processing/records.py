from typing import Literal, Optional
import faust
from decimal import Decimal
from openmesh.on_chain.ethereum import EthereumLog


class BaseCEXRecord(faust.Record):
    """
    Base record for all other records.

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


class Trade(BaseCEXRecord, serializer='trades'):
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


class Lob(BaseCEXRecord, serializer='lob'):
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


class Candle(BaseCEXRecord, serializer='candle'):
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
    closed: Optional[bool]
    o: Decimal
    h: Decimal
    l: Decimal
    c: Decimal
    v: Decimal


class TradeL3(Trade, serializer='trades_l3'):
    """
    Record for a trade with Order IDs.

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


class Ticker(BaseCEXRecord, serializer='ticker'):
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


class FundingRate(BaseCEXRecord, serializer='funding_rate'):
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
    funding_rate: Decimal
    mark_price: Decimal = Decimal(-1)
    next_funding_time: int = -1
    predicted_rate: Decimal = Decimal(-1)


class OpenInterest(BaseCEXRecord, serializer='open_interest'):
    """
    Record for an open interest update.

    :param open_interest: The open interest
    """
    open_interest: Decimal


class BaseChainRecord(faust.Record):
    """
    Base record for all on-chain messages.

    :param blockTimestamp: The timestamp of the block (in milliseconds)
    :type blockTimestamp: int
    :param atomTimestamp: The timestamp of when the message was received (in microseconds)
    :type atomTimestamp: int
    """
    blockTimestamp: int
    atomTimestamp: int


class BaseDexRecord(BaseChainRecord):
    """
    Base record for all DEX events

    :param exchange: The name of the exchange
    :type exchange: str
    :param pairAddr: The address of the pair
    :type pairAddr: str
    :param transactionHash: The hash of the transaction
    :type transactionHash: str
    :param logIndex: The index of the log in the transaction
    :type logIndex: int
    :param blockNumber: The number of the block
    :type blockNumber: int
    :param blockHash: The hash of the block
    :type blockHash: str
    """
    exchange: str
    pairAddr: str
    transactionHash: str
    logIndex: int
    blockNumber: int
    blockHash: str


class NFTTrade(BaseChainRecord, serializer='nft_trades'):
    exchange: str
    itemName: str
    itemId: str
    itemPermalink: str
    amountBought: int
    salePrice: Decimal
    maker: str = None
    taker: str = None
    transactionHash: str = None
    logIndex: int = None
    blockNumber: int = None
    blockHash: str = None


class DexTrade(BaseDexRecord, serializer='dex_trades'):
    """
    Record for a DEX trade.

    :param tokenBought: The token bought
    :type tokenBought: str
    :param tokenSold: The token sold
    :type tokenSold: str
    :param tokenBoughtAddr: The address of the token bought
    :type tokenBoughtAddr: str
    :param tokenSoldAddr: The address of the token sold
    :type tokenSoldAddr: str
    :param amountBought: The amount of the token bought
    :type amountBought: Decimal
    :param amountSold: The amount of the token sold
    :type amountSold: Decimal
    :param maker: The address of the maker
    :type maker: str
    :param taker: The address of the taker
    :type taker: str
    """
    tokenBought: str
    tokenSold: str
    tokenBoughtAddr: str
    tokenSoldAddr: str
    amountBought: Decimal
    amountSold: Decimal
    maker: str = None
    taker: str = None


class DexLiquidity(BaseDexRecord, serializer='dex_liquidity'):
    """
    Record for DEX liquidity events.

    :param token0: The first token in the pair
    :type token0: str
    :param token1: The second token in the pair
    :type token1: str
    :param token0Addr: The address of the first token in the pair
    :type token0Addr: str
    :param token1Addr: The address of the second token in the pair
    :type token1Addr: str
    :param amount0: The amount of the first token in the pair used in the liquidity event
    :type amount0: Decimal
    :param amount1: The amount of the second token in the pair used in the liquidity event
    :type amount1: Decimal
    :param owner: The address of the owner of the liquidity position
    :type owner: str
    """
    eventType: Literal['add', 'remove']
    token0: str
    token1: str
    token0Addr: str
    token1Addr: str
    amount0: Decimal
    amount1: Decimal
    owner: str = None


class EthereumLogRecord(EthereumLog, faust.Record, serializer='ethereum_logs'):
    """
    Record for an Ethereum log.
    """
    pass


record_mapping = {
    'lob_l3': LobL3,
    'trades_l3': TradeL3,
    'ticker': Ticker,
    'lob': Lob,
    'trades': Trade,
    'candle': Candle,
    'funding_rate': FundingRate,
    'open_interest': OpenInterest,
    'ethereum_logs': EthereumLogRecord,
    'dex_trades': DexTrade,
    'dex_liquidity': DexLiquidity
}
