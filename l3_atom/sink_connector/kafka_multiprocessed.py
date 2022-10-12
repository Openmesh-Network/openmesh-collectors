import aiokafka
import asyncio
from yapic import json
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from l3_atom.sink_connector.sink_connector import SinkMessageHandler
from l3_atom.helpers.read_config import get_kafka_config
from l3_atom.helpers.enrich_data import enrich_raw
import ssl
import uuid
import logging

ssl_ctx = ssl.create_default_context()


class Kafka(SinkMessageHandler):
    """
    Class to handle the metadata for Kafka

    :param exchange: The exchange to handle
    :type exchange: str
    :param key_field: The field to use as the key
    :type key_field: str
    """

    def __init__(self, exchange: str, key_field: str):
        super().__init__(exchange)
        conf = get_kafka_config()
        self.bootstrap = conf['KAFKA_BOOTSTRAP_SERVERS']
        self.sasl_username = conf['KAFKA_SASL_KEY'] if 'KAFKA_SASL_KEY' in conf else None
        self.sasl_password = conf['KAFKA_SASL_SECRET'] if 'KAFKA_SASL_SECRET' in conf else None
        self.schema_url = conf['SCHEMA_REGISTRY_URL']
        self.schema_username = conf['SCHEMA_REGISTRY_API_KEY'] if 'SCHEMA_REGISTRY_API_KEY' in conf else None
        self.schema_password = conf['SCHEMA_REGISTRY_API_SECRET'] if 'SCHEMA_REGISTRY_API_SECRET' in conf else None
        self.kafka_producer = None
        self.admin_client = None
        self.schema_client = None
        self.topic = f"{exchange}_raw"
        self.key_field = key_field


class KafkaConnector(Kafka):
    """Class to handle the backend connection to Kafka"""
    async def producer(self):
        """Handles the production of messages to Kafka. Runs indefinitely until the termination sentinel is sent over the pipe"""
        if not self.kafka_producer:
            await self._producer_init()
        while self.started:
            async with self.read_from_pipe() as messages:
                for message in messages:
                    msg = enrich_raw(json.loads(message))
                    key = msg[self.key_field] if self.key_field in msg else None
                    await self.kafka_producer.send(self.topic, json.dumps(msg).encode(), key=key.encode() if key else None)
        await self.kafka_producer.stop()

    async def _producer_init(self):
        """Initializes the Kafka producer"""
        loop = asyncio.get_event_loop()
        if self.sasl_username and self.sasl_password:
            self.kafka_producer = aiokafka.AIOKafkaProducer(
                loop=loop, bootstrap_servers=self.bootstrap, sasl_mechanism="PLAIN", sasl_plain_username=self.sasl_username,
                sasl_plain_password=self.sasl_password, security_protocol="SASL_SSL", ssl_context=ssl_ctx, linger_ms=0, acks=1, client_id=f"{self.exchange}-raw-producer-{str(uuid.uuid4())[:8]}")
        else:
            self.kafka_producer = aiokafka.AIOKafkaProducer(
                loop=loop, bootstrap_servers=self.bootstrap, linger_ms=0, acks=1)
        await self.kafka_producer.start()

    def _admin_init(self):
        """Initializes the Kafka admin client"""
        if self.sasl_username and self.sasl_password:
            self.admin_client = AdminClient({
                "bootstrap.servers": self.bootstrap,
                "sasl.mechanism": "PLAIN",
                "sasl.username": self.sasl_username,
                "sasl.password": self.sasl_password,
                "security.protocol": "SASL_SSL",
            })
        else:
            self.admin_client = AdminClient({
                "bootstrap.servers": self.bootstrap,
            })

    def _schema_init(self):
        """Initializes the Schema Registry client"""
        if self.schema_username and self.schema_password:
            self.schema_client = SchemaRegistryClient({
                "url": self.schema_url,
                "basic.auth.credentials.source": "USER_INFO",
                "basic.auth.user.info": f"{self.schema_username}:{self.schema_password}",
            })
        else:
            self.schema_client = SchemaRegistryClient({
                "url": self.schema_url
            })

    def create_exchange_topics(self, feeds: list):
        """
        Creates the topics and populates the schemas for the exchange's feeds

        :param feeds: The feeds to create topics for
        :type feeds: list[str]
        """
        if not self.admin_client:
            self._admin_init()
        if not self.schema_client:
            self._schema_init()
        topic_metadata = self.admin_client.list_topics(timeout=5)
        topics = []
        for feed in feeds:
            topic = f'{self.exchange}_{feed}'
            if topic not in topic_metadata.topics:
                logging.info(f"{self.exchange}: Creating topic {topic}")
                topics.append(
                    NewTopic(topic, num_partitions=6, replication_factor=1))
            else:
                logging.info(f"{self.exchange}: Topic {topic} already exists")

            if self.schema_client.lookup_schema(f'{topic}-value', "latest"):
                logging.info(
                    f"{self.exchange}: Schema for {topic} already exists")
            else:
                logging.info(f"{self.exchange}: Creating schema for {topic}")
                feed_schema = self.schema_client.get_latest_version(
                    feed).schema.schema_str
                self.schema_client.register_schema(
                    f'{topic}-value', feed_schema)

        if topics:
            self.admin_client.create_topics(topics)
