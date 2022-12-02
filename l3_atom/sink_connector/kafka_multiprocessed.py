import aiokafka
import asyncio
from yapic import json
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from l3_atom.sink_connector.sink_connector import SinkMessageHandler
from l3_atom.helpers.read_config import get_kafka_config
import ssl
import uuid
import logging
from io import BytesIO
from fastavro import schemaless_writer
from struct import pack

ssl_ctx = ssl.create_default_context()

CONFLUENT_MAGIC_BYTE = 0

class Kafka(SinkMessageHandler):
    """
    Class to handle the metadata for Kafka

    :param exchange: The exchange to handle
    :type exchange: str
    :param key_field: The field to use as the key
    :type key_field: str
    """

    def __init__(self, exchange):
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
        self.topic = "raw"

        # { <feed>: (<schema>, <schema id in registry>) }
        self.schema_map: dict = {}


class KafkaConnector(Kafka):
    """Class to handle the backend connection to Kafka"""
    async def producer(self):
        """Handles the production of messages to Kafka. Runs indefinitely until the termination sentinel is sent over the pipe"""
        if not self.kafka_producer:
            await self._producer_init()
        while self.started:
            async with self.read_from_pipe() as messages:
                for message in messages:
                    msg = json.loads(message)
                    key = self.exchange_ref.get_key(msg)
                    msg = self.serialize(msg)
                    await self.kafka_producer.send(self.topic, msg, key=key if key else None)
        await self.kafka_producer.stop()

    def serialize(self, msg: dict):
        """Preprocess message before sending to Kafka"""
        return json.dumps(msg).encode()

    async def _producer_init(self):
        """Initializes the Kafka producer"""
        loop = asyncio.get_event_loop()
        if self.sasl_username and self.sasl_password:
            self.kafka_producer = aiokafka.AIOKafkaProducer(
                loop=loop, bootstrap_servers=self.bootstrap, sasl_mechanism="PLAIN", sasl_plain_username=self.sasl_username,
                sasl_plain_password=self.sasl_password, security_protocol="SASL_SSL", ssl_context=ssl_ctx, linger_ms=0, acks=1, client_id=f"{self.exchange}-raw-producer-{str(uuid.uuid4())[:8]}", connections_max_idle_ms=None)
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
                "basic.auth.user.info": f"{self.schema_username}:{self.schema_password}",
            })
        else:
            self.schema_client = SchemaRegistryClient({
                "url": self.schema_url
            })

    def create_exchange_topics(self, feeds: list, prefix=None, include_raw=True):
        """
        Creates the topics and populates the schemas for the exchange's feeds. Also stores those schemas in memory.

        :param feeds: The feeds to create topics for
        :type feeds: list[str]
        """
        if not self.admin_client:
            self._admin_init()
        if not self.schema_client:
            self._schema_init()
        topics = []
        topic_metadata = self.admin_client.list_topics(timeout=5)
        if include_raw and "raw" not in topic_metadata.topics:
            topics.append(NewTopic(f"{prefix}raw", 100, 3))
        schemas = self.schema_client.get_subjects()
        for feed in feeds:
            feed = prefix + feed if prefix else feed
            feed_schema = self.schema_client.get_latest_version(
                feed)
            self.schema_map[feed] = feed_schema
            if feed not in topic_metadata.topics:
                logging.info(f"{self.exchange}: Creating topic {feed}")
                topics.append(
                    NewTopic(feed, num_partitions=50, replication_factor=3))
            else:
                logging.info(f"{self.exchange}: Topic {feed} already exists")

            if f'{feed}-value' in schemas:
                logging.info(
                    f"{self.exchange}: Schema for {feed} already exists")
            elif feed.endswith('raw'):
                logging.info(
                    f"{self.exchange}: Schema for {feed} is not required")
            else:
                logging.info(f"{self.exchange}: Creating schema for {feed}")
                self.schema_client.register_schema(
                    f'{feed}-value', feed_schema.schema)

        if topics:
            futures = self.admin_client.create_topics(topics)
            for topic, future in futures.items():
                try:
                    future.result()
                    logging.info(f"{self.exchange}: Created topic {topic}")
                except Exception as e:
                    logging.error(
                        f"{self.exchange}: Failed to create topic {topic}: {e}")

    def create_chain_topics(self, feeds, name):
        self.create_exchange_topics(
            feeds, prefix=f"{name}_", include_raw=False)


class AvroKafkaConnector(KafkaConnector):

    def serialize(self, msg: dict):
        msg_schema, schema_id = self.schema_map[msg['feed']]
        res = BytesIO()
        res.write(pack('>bI', CONFLUENT_MAGIC_BYTE, schema_id))
        schemaless_writer(res, msg_schema, msg)
        return res.getvalue()

