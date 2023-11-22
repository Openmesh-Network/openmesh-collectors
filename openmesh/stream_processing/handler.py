from openmesh.stream_processing.standardisers import standardisers
import logging
from typing import AsyncIterable
from .records import EthereumLogRecord
import traceback

RAW_CEX_TOPIC = 'raw'
RAW_CHAIN_TOPIC = 'ethereum_logs'

handlers = {
    e.exchange.name: e() for e in standardisers
}


async def process_cex(stream: AsyncIterable) -> AsyncIterable:
    """
    Indefinite iterator over the stream of messages coming in over the raw topic. Continuously iterates and processes each incoming message.

    :param stream: The stream of messages to process
    :type stream: AsyncIterable
    :return: Iterator over the stream of raw processed messages
    :rtype: AsyncIterable
    """
    async for key, message in stream.items():
        try:
            if not key:
                continue
            key = key.decode()
            exchange = key.split('_', maxsplit=1)[0]
            standardiser = handlers[exchange]
            if not standardiser.exchange_started:
                standardiser.start_exchange()
            await standardiser.handle_message(message)
            yield message
        except Exception:
            traceback.print_exc()
            logging.error(f"Message: {message}")
            continue


async def process_chain(stream: AsyncIterable) -> AsyncIterable:
    async for _, message in stream.items():
        try:
            standardiser = handlers['ethereum']
            if not standardiser.exchange_started:
                standardiser.start_exchange()
            await standardiser.handle_message(message)
            yield message
        except Exception:
            traceback.print_exc()
            logging.error(f"Message: {message}")
            continue


def initialise_agents(app):
    """Initialises the Faust agent"""
    logging.info("Initialising raw consumer")
    for standardiser in handlers.values():
        for topic in standardiser.normalised_topics:
            standardiser.normalised_topics[topic] = app.topic(topic)
    app.agent(RAW_CEX_TOPIC)(process_cex)
    ethereum_logs_topic = app.topic(
        RAW_CHAIN_TOPIC, value_type=EthereumLogRecord)
    app.agent(ethereum_logs_topic)(process_chain)
