from confluent_kafka import Producer
import certifi

# Azure Event Hubs のパラメータ設定
HOST = 'vantiq-eventhubs.servicebus.windows.net:9093'
TOPIC = 'test'
CONNECTION_STRING = 'Endpoint=sb://vantiq-eventhubs.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=AjwFYYFwgZ71KveJoAIVZcmgEREpNmvBb+AEhISwGKo='


# Azure Event Hubs の接続設定
conf = {
    'bootstrap.servers': HOST
    , 'client.id': 'python-example-producer'
    , 'security.protocol': 'SASL_SSL'
    , 'sasl.mechanism': 'PLAIN'
    , 'sasl.username': '$ConnectionString'
    , 'sasl.password': CONNECTION_STRING
    , 'ssl.ca.location': certifi.where()
}

# Azure Event Hubs へ送信
kafka = Producer(conf)
message = 'hello'
try:
    kafka.produce(topic = TOPIC, value = message)
finally:
    kafka.flush()
