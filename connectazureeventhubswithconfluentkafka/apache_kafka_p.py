from confluent_kafka import Producer

# Apache Kafka のパラメータ設定
HOST = '192.168.3.40:9093'
TOPIC = 'test'

# Apache Kafka の接続設定
conf = {
    'bootstrap.servers': HOST
    , 'client.id': 'python-example-producer'
}

# Apache Kafka へ送信
kafka = Producer(conf)
message = 'hello'
try:
    kafka.produce(topic = TOPIC, value = message)
finally:
    kafka.flush()
