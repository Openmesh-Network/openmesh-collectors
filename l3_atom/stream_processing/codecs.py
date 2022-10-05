from schema_registry.client import SchemaRegistryClient, Auth
from schema_registry.serializers.faust import FaustSerializer
from faust.serializers import codecs
from l3_atom.helpers.read_config import get_kafka_config

def initialise():
    config = get_kafka_config()

    client = SchemaRegistryClient(url=config['SCHEMA_REGISTRY_URL'], auth=Auth(username=config['SCHEMA_REGISTRY_API_KEY'], password=config['SCHEMA_REGISTRY_API_SECRET']))

    l3_trades_schema = client.get_schema("L3_Trade")
    l3_lob_schema = client.get_schema("L3_LOB")
    ticker_schema = client.get_schema("Ticker")

    l3_trades_serializer = FaustSerializer(client, 'trades_l3', l3_trades_schema.schema)
    l3_lob_serializer = FaustSerializer(client, 'lob_l3', l3_lob_schema.schema)
    ticker_serializer = FaustSerializer(client, 'ticker', ticker_schema.schema)

    codecs.register('trades_l3', l3_trades_serializer)
    codecs.register('trades_l3', l3_lob_serializer)
    codecs.register('ticker', ticker_serializer)
