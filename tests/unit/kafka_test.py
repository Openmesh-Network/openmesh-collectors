import pytest
import json
import asyncio
from unittest.mock import Mock, AsyncMock
from multiprocessing import Pipe

import l3_atom.sink_connector.kafka_multiprocessed as kafka_multiprocessed
from l3_atom.sink_connector.kafka_multiprocessed import KafkaConnector


class MockTopics:
    def __init__(self, topics):
        self.topics = topics


def new_config():
    return {
        'KAFKA_BOOTSTRAP_SERVERS': None,
        'SCHEMA_REGISTRY_URL': None
    }


@pytest.fixture()
def mock_kafka():
    kafka_multiprocessed.get_kafka_config = new_config
    kafka_multiprocessed.AdminClient = Mock()
    kafka_multiprocessed.SchemaRegistryClient = Mock()
    kafka_multiprocessed.NewTopic = Mock()
    kafka = KafkaConnector('test', 'test')
    kafka.pipe = Pipe(duplex=False)

    kafka._admin_init()
    kafka.admin_client = Mock()
    kafka.admin_client.list_topics = Mock()
    kafka.admin_client.list_topics.return_value = MockTopics(
        ['test_raw', 'test_lob'])
    kafka._schema_init()
    kafka.schema_client = Mock()
    kafka.schema_client.get_subjects = Mock()
    kafka.schema_client.get_subjects.return_value = ['test_lob-value']
    kafka.kafka_producer = AsyncMock()
    return kafka


def mock_process_message(ret, **kwargs):
    async def process_message(cls, msg, conn, channel):
        msg = json.loads(msg)
        ret.append(msg)
    return process_message


@pytest.mark.asyncio()
async def test_topic_and_schema(mock_kafka):
    mock_kafka.create_exchange_topics(['raw', 'trades', 'lob'])
    mock_kafka.admin_client.create_topics.assert_called_with(
        [kafka_multiprocessed.NewTopic('test_trades', num_partitions=6, replication_factor=1)])


@pytest.mark.asyncio()
async def test_producer(mock_kafka):
    mock_kafka.started = True
    for msg in ['[1]', '[2]', '[3]', '[4]', 'END']:
        await mock_kafka.write(msg)
    ret = asyncio.create_task(mock_kafka.producer())
    await ret
    args = mock_kafka.kafka_producer.send.call_args
    assert args[0][0] == 'test_raw'
    assert json.loads(args[0][1].decode())[0] == 4
    mock_kafka.kafka_producer.stop.assert_called()
