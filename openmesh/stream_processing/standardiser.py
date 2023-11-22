from openmesh.data_source import DataSource
from openmesh.stream_processing.records import record_mapping


class Standardiser:
    """
    Base class for schema standardisers

    :param raw_topic: The raw topic to consume from
    :type raw_topic: str
    :param feeds: The feeds to standardise
    :type feeds: list
    :param exchange: The exchange to standardise for. Creates a connection to the exchange
    :type exchange: OrderBookExchange
    :param feed_to_record: Mapping of feed to record class
    :type feed_to_record: dict
    """
    raw_topic: str = 'raw'
    exchange: DataSource = NotImplemented
    feed_to_record: dict = record_mapping

    def __init__(self) -> None:
        self.id = self.exchange.name
        self.exchange_started = False
        self.feeds = [*self.exchange.ws_channels.keys(), *
                      self.exchange.rest_channels.keys()]
        self.normalised_topics = {
            feed: None for feed in self.feeds
        }

    def start_exchange(self):
        self.exchange = self.exchange()
        self.exchange_started = True

    def normalise_symbol(self, exch_symbol: str) -> str:
        """Get the normalised symbol from the exchange symbol"""
        return self.exchange.get_normalised_symbol(exch_symbol).normalised

    async def send_to_topic(self, feed: str, exchange=None, key_field='symbol', **kwargs):
        """
        Given a feed and arguments, send to the correct topic

        :param feed: The feed to send to
        :type feed: str
        :param kwargs: The arguments to use in the relevant Record
        :type kwargs: dict
        """
        source = exchange if exchange else self.id
        val = self.feed_to_record[feed](**kwargs, exchange=source)
        val.validate()
        await self.normalised_topics[feed].send(
            value=val,
            key=f"{source}_{kwargs[key_field]}"
        )

    async def handle_message(self, msg: dict):
        """
        Method to handle incoming messages. Overriden by subclasses.

        :param msg: The message to handle
        :type msg: dict
        """
        raise NotImplementedError
