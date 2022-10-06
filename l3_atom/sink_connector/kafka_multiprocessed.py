import aiokafka
import asyncio
from yapic import json
from l3_atom.sink_connector.sink_connector import SinkConnector, SinkMessageHandler
from l3_atom.helpers.read_config import get_kafka_config
from l3_atom.helpers.enrich_data import enrich_raw
import logging
import ssl
import uuid

ssl_ctx = ssl.create_default_context()

class Kafka(SinkMessageHandler):
    def __init__(self, exchange:str, key_field:str):
        super().__init__(exchange)
        conf = get_kafka_config()
        self.bootstrap = conf['KAFKA_BOOTSTRAP_SERVERS']
        self.sasl_username = conf['KAFKA_SASL_KEY'] if 'KAFKA_SASL_KEY' in conf else None
        self.sasl_password = conf['KAFKA_SASL_SECRET'] if 'KAFKA_SASL_SECRET' in conf else None
        self.kafka_producer = None
        self.topic = f"{exchange}_raw"
        self.key_field = key_field

class KafkaConnector(Kafka, SinkConnector):
    async def producer(self):
        if not self.kafka_producer:
            loop = asyncio.get_event_loop()
            if self.sasl_username and self.sasl_password:
                self.kafka_producer = aiokafka.AIOKafkaProducer(loop=loop, bootstrap_servers=self.bootstrap, sasl_mechanism="PLAIN", sasl_plain_username=self.sasl_username, sasl_plain_password=self.sasl_password, security_protocol="SASL_SSL", ssl_context=ssl_ctx, linger_ms=0, acks=1, client_id=f"{self.exchange}-raw-producer-{str(uuid.uuid4())[:8]}")
            else:
                self.kafka_producer = aiokafka.AIOKafkaProducer(loop=loop, bootstrap_servers=self.bootstrap, linger_ms=0, acks=1)
            await self.kafka_producer.start()
        while self.started:
            async with self.read_from_pipe() as messages:
                for message in messages:
                    msg = enrich_raw(json.loads(message))
                    key = msg[self.key_field] if self.key_field in msg else None
                    await self.kafka_producer.send(self.topic, json.dumps(msg).encode(), key=key.encode() if key else None)
        await self.kafka_producer.stop()