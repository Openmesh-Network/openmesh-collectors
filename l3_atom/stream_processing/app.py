import faust
from l3_atom.helpers.read_config import get_kafka_config
from l3_atom.stream_processing import codecs

config = get_kafka_config()

if 'KAFKA_SASL_KEY' in config:
    app = faust.App("schema-standardiser", broker=f"aiokafka://{config['KAFKA_BOOTSTRAP_SERVERS']}", broker_credentials=faust.SASLCredentials(username=config['KAFKA_SASL_KEY'], password=config['KAFKA_SASL_SECRET'], ssl_context=ssl_ctx, mechanism="PLAIN"))
else:
    app = faust.App("schema-standardiser", broker=f"aiokafka://{config['KAFKA_BOOTSTRAP_SERVERS']}")

app.conf.consumer_auto_offset_reset = 'latest'

codecs.initialise()