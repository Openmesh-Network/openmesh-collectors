from confluent_kafka import Consumer, KafkaError, KafkaException
import sys
from queue import Queue

class ExchangeDataConsumer():
    def __init__(self, topic):
        self.topic = topic
        self.conf = {'bootstrap.servers': 'localhost:9092', 'group.id': 'mygroup', 'client.id': 'kafka-python-consumer'}
        self.consumer = Consumer(self.conf)
        self.consumer.subscribe([self.topic])
        self.msg_queue = Queue()

    def consume(self):
        msg = self.consumer.poll(1.0)
        if msg is None: return

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                (msg.topic(), msg.partition(), msg.offset()))
            elif msg.error():
                raise KafkaException(msg.error())
        
        else:
            print("Message received: %s: %s" % (msg.key().decode(), msg.value().decode()))
            self.msg_queue.put(msg.value())
        #self.consumer.close()

    def get_msg(self):
        return self.msg_queue.get()

def main():
    conf = {'bootstrap.servers': 'localhost:9092', 'group.id': 'mygroup', 'client.id': 'kafka-python-consumer'}
    consumer = Consumer(conf)

    consumer.subscribe(["BTC-USD"])


if __name__ == "__main__":
    main()