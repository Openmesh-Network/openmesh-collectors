import aiokafka
import asyncio
from yapic import json
from l3_atom.sink_connector.sink_connector import SinkConnector, SinkMessageHandler
from l3_atom.helpers.read_config import get_kafka_config
import logging

class Kafka(SinkMessageHandler):
    def __init__(self, exchange:str):
        super().__init__(exchange)
        conf = get_kafka_config()
        self.bootstrap = conf['KAFKA_BOOTSTRAP_SERVERS']
        self.kafka_producer = None
        self.topic = f"{exchange}-raw"

class KafkaConnector(Kafka, SinkConnector):
    async def producer(self):
        if not self.kafka_producer:
            loop = asyncio.get_event_loop()
            self.kafka_producer = aiokafka.AIOKafkaProducer(loop=loop, bootstrap_servers=self.bootstrap)
            await self.kafka_producer.start()
        while self.started:
            async with self.read_from_pipe() as messages:
                for message in messages:
                    await self.kafka_producer.send_and_wait(self.topic, message.encode())
        await self.kafka_producer.stop()