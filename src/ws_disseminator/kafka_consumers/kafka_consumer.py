from confluent_kafka import Consumer, KafkaError, KafkaException 
import sys
import asyncio
import threading

class KafkaConsumer():
    def __init__(self, topic):
        suffix = ""
        if topic not in ("BTCUSDT", "BTCUSD"):
            suffix = "-raw"
        self.topic = "test-" + topic + suffix
        self.conf = {
            'bootstrap.servers': 'SSL://kafka-16054d72-gda-3ad8.aivencloud.com:18921',
            'security.protocol' : 'SSL', 
            'client.id': 'kafka-python-consumer',
            'ssl.certificate.location': 'jay.cert',
            'ssl.key.location': 'jay.key',
            'ssl.ca.location': 'ca-aiven-cert.pem',
            'group.id': 'jay-test-group',
            'auto.offset.reset': 'earliest'
        }
        self.consumer = Consumer(self.conf)
        self.consumer.subscribe([self.topic])
        self.queue = asyncio.Queue()
    
    def consume(self):
        try:
            return self._consume()
        except RuntimeError as e:
            print(e)
            # print("kafka consumer closed")

    def _consume(self):
        msg = self.consumer.poll(timeout=10e-9)
        if msg is None: 
            return

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                (msg.topic(), msg.partition(), msg.offset()))
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            return msg.value()
    
    def shutdown(self):
        self.consumer.close()


def main():
    consumer = KafkaConsumer("bybit")
    while True:
        msg = consumer.consume()
        if msg:
            print(msg)


if __name__ == "__main__":
    main()