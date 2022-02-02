from confluent_kafka import Consumer, KafkaError, KafkaException
import sys
from queue import Queue

class ExchangeDataConsumer():
    def __init__(self, topic):
        self.topic = topic
        self.conf = {'bootstrap.servers': 'localhost:19092,localhost:29092,localhost:39092', 'group.id': 'mygroup', 'client.id': 'kafka-coinbase-consumer'}
        self.consumer = Consumer(self.conf)
        self.consumer.subscribe([self.topic])

    def consume(self):
        msg = self.consumer.poll(1.0)
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