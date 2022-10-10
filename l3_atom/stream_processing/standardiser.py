from l3_atom.orderbook_exchange import OrderBookExchange
from l3_atom.stream_processing.records import *

class Standardiser:
    raw_topic:str = NotImplemented
    feeds:list = NotImplemented
    exchange:OrderBookExchange = NotImplemented

    def __init__(self) -> None:
        self.id = self.exchange.name
        self.normalised_topics = {
            f"{feed}": f"{self.id}_{feed}" for feed in self.feeds
        }
        self.raw_topic = f'{self.id}_raw'
        self.exchange = self.exchange()

    def normalise_symbol(self, exch_symbol:str) -> str:
        return self.exchange.get_normalised_symbol(exch_symbol)

    async def send_to_topic(self, feed, **kwargs):
        val = self.feed_to_record[feed](**kwargs, exchange=self.id)
        val.validate()
        await self.normalised_topics[feed].send(
            value=val,
            key=kwargs['symbol']
        )

    async def handle_message(self, msg:dict):
        raise NotImplementedError

    async def process(self, stream):
        async for message in stream:
            await self.handle_message(message)
