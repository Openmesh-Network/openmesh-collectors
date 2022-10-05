from l3_atom.orderbook_exchange import OrderbookExchange

class Standardiser:
    raw_topic:str = NotImplemented
    feeds:list = NotImplemented
    exchange:OrderbookExchange = NotImplemented

    def __init__(self) -> None:
        self.id = self.exchange.id
        self.normalised_topics = {
            f"{feed}": self.app.topic(f"{self.id}_{feed}") for feed in self.feeds
        }

    def normalise_symbol(self, exch_symbol:str) -> str:
        return self.exchange.get_normalised_symbol(exch_symbol)

    async def handle_message(self, msg:dict):
        raise NotImplementedError

    async def process(self, stream):
        async for message in stream:
            self.handle_message(message)
