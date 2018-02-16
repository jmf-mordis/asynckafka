import socket
import unittest

import asyncio

import time
from contextlib import closing

from kafka import KafkaProducer

from asynckafka import exceptions
from asynckafka.consumers.rd_kafka_consumer cimport RdKafkaConsumer
from asynckafka.consumers.rd_kafka_consumer import RdKafkaConsumer
from asynckafka.consumers.consumers cimport Consumer, StreamConsumer
from asynckafka.includes cimport c_rd_kafka as crdk

import os
from subprocess import call


import logging
import sys
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


def produce_one_message(topic, message):
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    producer.send(topic, message)
    producer.close()

test_consumer_settings = {
    "session.timeout.ms": "6000"
}

test_topic_settings = {}


class IntegrationTestCase(unittest.TestCase):

    docker_compose_file = os.getcwd() + '/docker-compose.yml'

    @classmethod
    def setUpClass(cls):
        return_code = call(
            ["docker-compose", "-f", cls.docker_compose_file, "up", "-d"]
        )
        if return_code != 0:
            cls.fail("Error launching docker-compose")

    @classmethod
    def tearDownClass(cls):
        return_code = call(
            ["docker-compose", "-f", cls.docker_compose_file, "kill"]
        )
        if return_code != 0:
            cls.fail("Error stopping docker-compose")
        return_core = call(
            ["docker-compose", "-f", cls.docker_compose_file, "down"]
        )
        if return_code != 0:
            cls.fail("Error stopping docker-compose")

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

    def tearDown(self):
        self.loop.close()


class TestIntegrationRdKafkaConsumer(IntegrationTestCase):

    def tests_start_and_stop(self):
        rd_kafka_consumer = RdKafkaConsumer(
            brokers='127.0.0.1',
            consumer_settings=test_consumer_settings,
            topic_settings=test_topic_settings,
        )
        rd_kafka_consumer.add_topic("my_topic")
        rd_kafka_consumer.start()
        rd_kafka_consumer.stop()


class TestIntegrationConsumer(IntegrationTestCase):

    def tests_receive_one_message(self):
        confirm_message = asyncio.Future(loop=self.loop)

        async def message_handler(message):
            confirm_message.set_result(message)

        consumer = Consumer(
            brokers="127.0.0.1:9092",
            consumer_settings=test_consumer_settings,
            topic_settings=test_topic_settings,
            loop=self.loop,
        )
        consumer.add_message_handler('my_topic', message_handler)
        consumer.start()

        crdk.rd_kafka_assign(
            consumer._rdk_consumer.consumer,
            consumer._rdk_consumer.topic_partition_list
        )

        message = b'some_message_bytes'
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        producer.send('my_topic', message)
        producer.close()

        coro = asyncio.wait_for(confirm_message, timeout=30, loop=self.loop)

        self.loop.run_until_complete(coro)

        self.assertEqual(confirm_message.result(), message)
        consumer.stop()


class TestIntegrationStreamConsumer(IntegrationTestCase):

    def tests_receive_one_message(self):
        confirm_message = asyncio.Future(loop=self.loop)

        async def consume_messages():
            async for message in stream_consumer:
                confirm_message.set_result(message)

        stream_consumer = StreamConsumer(
            brokers="127.0.0.1:9092",
            topic='my_test_topic',
            consumer_settings=test_consumer_settings,
            topic_settings=test_topic_settings,
            loop=self.loop,
        )
        stream_consumer.start()
        crdk.rd_kafka_assign(
            stream_consumer._rd_kafka.consumer,
            stream_consumer._rd_kafka.topic_partition_list
        )

        message = b'some_message_bytes'
        produce_one_message('my_test_topic', message)


        asyncio.ensure_future(consume_messages(), loop=self.loop)
        coro = asyncio.wait_for(confirm_message, timeout=30, loop=self.loop)
        self.loop.run_until_complete(coro)

        self.assertEqual(confirm_message.result(), message)
        stream_consumer.stop()
