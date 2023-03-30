#!/usr/bin/env python
#
# Copyright (c) Microsoft Corporation. All rights reserved.
# Copyright 2016 Confluent Inc.
# Licensed under the MIT License.
# Licensed under the Apache License, Version 2.0
#
# Original Confluent sample modified for use with Azure Event Hubs for Apache Kafka Ecosystems

from confluent_kafka import Consumer, KafkaException, KafkaError
import sys
import getopt
import json
import logging
from pprint import pformat
import certifi


# def stats_cb(stats_json_str):
#     stats_json = json.loads(stats_json_str)
#     print('\nKAFKA Stats: {}\n'.format(pformat(stats_json)))


# def print_usage_and_exit(program_name):
#     sys.stderr.write('Usage: %s [options..] <consumer-group> <topic1> <topic2> ..\n' % program_name)
#     options = '''
#  Options:
#   -T <intvl>   Enable client statistics at specified interval (ms)
# '''
#     sys.stderr.write(options)
#     sys.exit(1)


group = '$Default'
topics = ['test']
conf = {
    'bootstrap.servers': 'vantiq-eventhubs.servicebus.windows.net:9093',
    'security.protocol': 'SASL_SSL',
    'ssl.ca.location': certifi.where(),
    'sasl.mechanism': 'PLAIN',
    'sasl.username': '$ConnectionString',
    'sasl.password': 'Endpoint=sb://vantiq-eventhubs.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=AjwFYYFwgZ71KveJoAIVZcmgEREpNmvBb+AEhISwGKo=',
    'group.id': group,
    'client.id': 'python-example-consumer',
    'request.timeout.ms': 60000,
    'session.timeout.ms': 60000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
}

# conf['stats_cb'] = stats_cb
# conf['statistics.interval.ms'] = int(100)

# Create logger for consumer (logs will be emitted when poll() is called)
# logger = logging.getLogger('consumer')
# logger.setLevel(logging.DEBUG)
# handler = logging.StreamHandler()
# handler.setFormatter(logging.Formatter('%(asctime)-15s %(levelname)-8s %(message)s'))
# logger.addHandler(handler)

# Create Consumer instance
# Hint: try debug='fetch' to generate some log messages
kafka = Consumer(conf)

# def print_assignment(consumer, partitions):
#     print('Assignment:', partitions)

# Subscribe to topics

try:
    kafka.subscribe(topics)
    while True:
        msg = kafka.poll(timeout=100.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                    (msg.topic(), msg.partition(), msg.offset()))
            elif msg.error():
                raise KafkaException(msg.error())
        else:
            print(msg.value())
finally:
    # Close down consumer to commit final offsets.
    kafka.close()

# Read messages from Kafka, print to stdout
# try:
#     while True:
#         msg = kafka.poll(timeout=100.0)
#         if msg is None:
#             continue
#         if msg.error():
#             # Error or event
#             if msg.error().code() == KafkaError._PARTITION_EOF:
#                 # End of partition event
#                 sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
#                                  (msg.topic(), msg.partition(), msg.offset()))
#             else:
#                 # Error
#                 raise KafkaException(msg.error())
#         else:
#             # Proper message
#             print(msg.value())

# except KeyboardInterrupt:
#     sys.stderr.write('%% Aborted by user\n')

# finally:
#     # Close down consumer to commit final offsets.
#     kafka.close()


