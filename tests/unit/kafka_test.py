import pytest
import json
from unittest.mock import Mock, AsyncMock
from multiprocessing import Pipe

import openmesh.sink_connector.kafka_multiprocessed as kafka_multiprocessed
from openmesh.sink_connector.kafka_multiprocessed import KafkaConnector
from openmesh.data_source import DataFeed


class MockTopics:
    def __init__(self, topics):
        self.topics = topics


def new_config():
    return {
        'KAFKA_BOOTSTRAP_SERVERS': None,
        'SCHEMA_REGISTRY_URL': None
    }


class MockExchange(DataFeed):
    name = 'test'
    sym_field = 0
    type_field = 0


@pytest.fixture()
def mock_kafka():
    kafka_multiprocessed.get_kafka_config = new_config
    kafka_multiprocessed.AdminClient = Mock()
    kafka_multiprocessed.SchemaRegistryClient = Mock()
    kafka_multiprocessed.NewTopic = Mock()
    kafka = KafkaConnector(MockExchange)
    kafka.pipe = Pipe(duplex=False)

    kafka._admin_init()
    kafka.admin_client = Mock()
    kafka.admin_client.list_topics = Mock()
    kafka.admin_client.list_topics.return_value = MockTopics(
        ['raw', 'lob'])
    kafka.admin_client.create_topics.return_value = {}
    kafka._schema_init()
    kafka.schema_client = Mock()
    kafka.schema_client.get_subjects = Mock()
    kafka.schema_client.get_subjects.return_value = ['lob-value']
    kafka.kafka_producer = AsyncMock()
    return kafka


def mock_process_message(ret, **kwargs):
    async def process_message(cls, msg, conn, channel):
        msg = json.loads(msg)
        ret.append(msg)
    return process_message


@pytest.mark.asyncio()
async def test_topic_and_schema(mock_kafka):
    mock_kafka.create_exchange_topics(['trades', 'lob'])
    mock_kafka.admin_client.create_topics.assert_called_with(
        [kafka_multiprocessed.NewTopic('trades', num_partitions=6, replication_factor=1)])


@pytest.mark.asyncio()
async def test_producer(mock_kafka):
    mock_kafka.started = True
    for msg in ['[1]', '[2]', '[3]', '[4]', 'END']:
        await mock_kafka.write(msg)
    ret = mock_kafka.producer()
    await ret
    args = mock_kafka.kafka_producer.send.call_args
    assert args[0][0] == 'raw'
    assert json.loads(args[0][1].decode())[0] == 4
    mock_kafka.kafka_producer.stop.assert_called()
