from l3_atom.stream_processing.standardisers import standardisers
import logging
from typing import AsyncIterable

RAW_TOPIC = 'raw'

handlers = {
    e.exchange.name: e() for e in standardisers
}

async def process(stream: AsyncIterable) -> AsyncIterable:
        """
        Indefinite iterator over the stream of messages coming in over the raw topic. Continuously iterates and processes each incoming message.

        :param stream: The stream of messages to process
        :type stream: AsyncIterable
        :return: Iterator over the stream of raw processed messages
        :rtype: AsyncIterable
        """
        async for key, message in stream.items():
            key = key.decode()
            exchange = key.split('_')[0]
            standardiser = handlers[exchange]
            if not standardiser.exchange_started:
                standardiser.start_exchange()
            await standardiser.handle_message(message)
            yield message

def initialise_agents(app):
    """Initialises the Faust agent"""
    logging.info(f"Initialising raw consumer")
    for standardiser in handlers.values():
        for k, topic in standardiser.normalised_topics.items():
            standardiser.normalised_topics[k] = app.topic(topic)
    app.agent(RAW_TOPIC)(process)

