import aiokafka
import asyncio
from yapic import json
from l3_atom.sink_connector.sink_connector import SinkConnector, SinkMessageHandler
from l3_atom.helpers.read_config import get_kafka_config
import logging
import ssl

ssl_ctx = ssl.create_default_context()

class Kafka(SinkMessageHandler):
    def __init__(self, exchange:str):
        super().__init__(exchange)
        conf = get_kafka_config()
        self.bootstrap = conf['KAFKA_BOOTSTRAP_SERVERS']
        self.sasl_username = conf['KAFKA_SASL_KEY'] if 'KAFKA_SASL_KEY' in conf else None
        self.sasl_password = conf['KAFKA_SASL_SECRET'] if 'KAFKA_SASL_SECRET' in conf else None
        self.kafka_producer = None
        self.topic = f"{exchange}-raw"

class KafkaConnector(Kafka, SinkConnector):
    async def producer(self):
        if not self.kafka_producer:
            loop = asyncio.get_event_loop()
            if self.sasl_username and self.sasl_password:
                self.kafka_producer = aiokafka.AIOKafkaProducer(loop=loop, bootstrap_servers=self.bootstrap, sasl_mechanism="PLAIN", sasl_plain_username=self.sasl_username, sasl_plain_password=self.sasl_password, security_protocol="SASL_SSL", ssl_context=ssl_ctx, linger_ms=0, acks=1)
            else:
                self.kafka_producer = aiokafka.AIOKafkaProducer(loop=loop, bootstrap_servers=self.bootstrap, linger_ms=0, acks=1)
            await self.kafka_producer.start()
        while self.started:
            async with self.read_from_pipe() as messages:
                for message in messages:
                    await self.kafka_producer.send(self.topic, message.encode())
        await self.kafka_producer.stop()