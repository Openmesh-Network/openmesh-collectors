import faust
from openmesh.helpers.read_config import get_kafka_config
from openmesh.stream_processing import codecs, handler
import ssl


def init():
    """Initialises the Faust Stream Processing app"""
    config = get_kafka_config()

    ssl_ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)

    brokers = config['KAFKA_BOOTSTRAP_SERVERS'].split(',')

    if 'KAFKA_SASL_KEY' in config:
        app = faust.App("schema-standardiser", broker=f"aiokafka://{';'.join(brokers)}", broker_credentials=faust.SASLCredentials(
            username=config['KAFKA_SASL_KEY'], password=config['KAFKA_SASL_SECRET'], ssl_context=ssl_ctx, mechanism="PLAIN"))
    else:
        app = faust.App("schema-standardiser",
                        broker=f"aiokafka://{';'.join(brokers)}")

    app.conf.consumer_auto_offset_reset = 'latest'
    app.conf.producer_acks = 1

    app.conf.producer_connections_max_idle_ms = None
    app.conf.consumer_connections_max_idle_ms = None

    codecs.initialise()
    handler.initialise_agents(app)

    return app
