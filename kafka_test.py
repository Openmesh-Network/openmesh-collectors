from confluent_kafka import Producer, Consumer
import json
import sys

def main():

    conf = {
            'bootstrap.servers': 'SSL://kafka-16054d72-gda-3ad8.aivencloud.com:18921',
            'security.protocol' : 'SSL', 
            'client.id': 'kafka-python-producer',
            'ssl.certificate.location': './jay.cert',
            'ssl.key.location': './jay.key',
            'ssl.ca.location': './ca-aiven-cert.pem',
        }
    producer = Producer(conf)

    topic = "jay-test-topic"

    def acked(err, msg):
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            #delivered_records += 1
            print("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))

    print("\n\n-----PRODUCER TEST-----\n")

    for n in range(3):
        record_key = "jay"
        record_value = json.dumps({'count': n, 'message': 'test'})
        print("Producing record: {}\t{}".format(record_key, record_value))
        producer.produce(topic, key=record_key, value=record_value, on_delivery=acked)
        producer.poll(0)

    producer.flush()

    print("Messages were produced to topic {}!".format(topic))

    print("\n\n-----CONSUMER TEST-----\n")

    conf['group.id'] = 'jay-test-group'
    conf['auto.offset.reset'] = 'earliest'

    consumer = Consumer(conf)
    consumer.subscribe([topic])

    total_count = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()
                record_value = msg.value()
                data = json.loads(record_value)
                count = data['count']
                total_count += count
                print("Consumed record with key {} and value {}, and updated total count to {}"
                      .format(record_key, record_value, total_count))
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Quitting")
        sys.exit(0)