from confluent_kafka import Consumer

# Apache Kafka のパラメータ設定
HOST = '192.168.3.40:9093'
TOPIC = 'test'

# Apache Kafka の接続設定
conf = {
    'bootstrap.servers': HOST
    , 'client.id': 'python-example-consumer'
    , 'group.id': 'foo'
    , 'auto.offset.reset': 'smallest'
}

# Apache Kafka から受信
kafka = Consumer(conf)
try:
    kafka.subscribe([TOPIC])
    msg_count = 0
    while True:
        msg = kafka.poll(timeout = 1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        print('Received message: {}'.format(msg.value().decode('utf-8')))
finally:
    kafka.close()
