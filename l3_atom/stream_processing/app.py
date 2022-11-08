import faust
from l3_atom.helpers.read_config import get_kafka_config
from l3_atom.stream_processing import codecs, handler
import ssl


def init():
    """Initialises the Faust Stream Processing app"""
    config = get_kafka_config()

    ssl_ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)

    if 'KAFKA_SASL_KEY' in config:
        app = faust.App("schema-standardiser", broker=f"aiokafka://{config['KAFKA_BOOTSTRAP_SERVERS']}", broker_credentials=faust.SASLCredentials(
            username=config['KAFKA_SASL_KEY'], password=config['KAFKA_SASL_SECRET'], ssl_context=ssl_ctx, mechanism="PLAIN"))
    else:
        app = faust.App("schema-standardiser",
                        broker=f"aiokafka://{config['KAFKA_BOOTSTRAP_SERVERS']}")

    app.conf.consumer_auto_offset_reset = 'latest'
    app.conf.producer_acks = 1

    app.conf.producer_connections_max_idle_ms = None
    app.conf.consumer_connections_max_idle_ms = None

    codecs.initialise()
    handler.initialise_agents(app)

    return app
