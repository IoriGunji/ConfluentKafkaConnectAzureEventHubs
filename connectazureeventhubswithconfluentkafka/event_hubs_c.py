from confluent_kafka import Consumer
import certifi

# Azure Event Hubs のパラメータ設定
HOST = 'vantiq-eventhubs.servicebus.windows.net:9093'
TOPIC = 'test'
CONNECTION_STRING = 'Endpoint=sb://vantiq-eventhubs.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=AjwFYYFwgZ71KveJoAIVZcmgEREpNmvBb+AEhISwGKo='
GROUP = 'foo'

# Azure Event Hubs の接続設定
conf = {
    'bootstrap.servers': HOST
    , 'client.id': 'python-example-consumer'
    , 'security.protocol': 'SASL_SSL'
    , 'sasl.mechanism': 'PLAIN'
    , 'sasl.username': '$ConnectionString'
    , 'sasl.password': CONNECTION_STRING
    , 'ssl.ca.location': certifi.where()
    , 'group.id': GROUP
    , 'auto.offset.reset': 'smallest'
}

# Azure Event Hubs から受信
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
