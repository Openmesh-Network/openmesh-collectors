from confluent_kafka import Consumer, KafkaError, KafkaException
import sys
from queue import Queue

class ExchangeDataConsumer():
    def __init__(self, topic):
        self.topic = topic
        self.conf = {
            'bootstrap.servers': 'SSL://kafka-16054d72-gda-3ad8.aivencloud.com:18921',
            'security.protocol' : 'SSL', 
            'client.id': 'binance-python-consumer',
            'ssl.certificate.location': 'jay.cert',
            'ssl.key.location': 'jay.key',
            'ssl.ca.location': 'ca-aiven-cert.pem',
            'group.id': 'binance-test-group',
            'auto.offset.reset': 'earliest'
        }
        self.consumer = Consumer(self.conf)
        self.consumer.subscribe([f"test-binance-raw"])
        print(f"Subscribed to topic test-binance-raw")

    def consume(self):
        msg = self.consumer.poll()
        if msg is None: 
            print(msg)
            print("no message")
            return

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                (msg.topic(), msg.partition(), msg.offset()))
            elif msg.error():
                raise KafkaException(msg.error())
        
        else:
            #print("Message received: %s: %s" % (msg.key().decode(), msg.value().decode()))
            return msg.value()
        #self.consumer.close()

def main():
    conf = {'bootstrap.servers': 'localhost:9092', 'group.id': 'mygroup', 'client.id': 'kafka-python-consumer'}
    consumer = Consumer(conf)

    consumer.subscribe(["BTC-USD"])


if __name__ == "__main__":
    main()