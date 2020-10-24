import asyncio
import logging
import sys
import os
import unittest
import uuid

from kafka import KafkaProducer

# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

kafka_host = os.getenv('KAFKA_HOST', '127.0.0.1')


def produce_to_kafka(topic, message, key=None, number=1):
    producer = KafkaProducer(bootstrap_servers=f'{kafka_host}:9092')
    [producer.send(topic, message, key=key) for _ in range(number)]
    producer.close()


test_consumer_settings = {
    "session.timeout.ms": "6000"
}


test_topic_settings = {
    'auto.offset.reset':  'smallest'
}


class IntegrationTestCase(unittest.TestCase):

    brokers = f"{kafka_host}:9092"

    def setUp(self):
        self.test_message = b'some_message_bytes'
        self.test_topic = 'test_' + str(uuid.uuid4())
        self.test_key = b'test_key'

        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        self.loop.close()
