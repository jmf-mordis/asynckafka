import asyncio
import unittest
from multiprocessing import Event

import os
from kafka import KafkaConsumer

from asynckafka import exceptions
from asynckafka.producer.producer import Producer
from tests.integration_tests.test_utils import IntegrationTestCase


class TestsIntegrationProducer(IntegrationTestCase):

    def setUp(self):
        super().setUp()
        self.producer = Producer(
            brokers="127.0.0.1",
            loop=self.loop
        )

    def tearDown(self):
        if self.producer.is_started():
            self.producer.stop()
        super().tearDown()

    def test_producer_start_stop(self):
        self.producer.start()
        self.loop.run_until_complete(asyncio.sleep(0))
        self.producer.stop()

    @unittest.skipIf(os.environ.get("SHORT"), "Skipping long tests")
    def test_produce_one_message(self):
        consumer = KafkaConsumer(self.test_topic)

        self.producer.start()
        coro = self.producer.produce(self.test_topic, self.test_message)
        self.loop.run_until_complete(coro)

        msg = next(consumer)

        self.assertEqual(msg.value, self.test_message)

    @unittest.skipIf(os.environ.get("SHORT"), "Skipping long tests")
    def test_produce_one_message_with_key(self):
        consumer = KafkaConsumer(self.test_topic)

        self.producer.start()
        coro = self.producer.produce(self.test_topic, self.test_message,
                                     self.test_key)
        self.loop.run_until_complete(coro)

        msg = next(consumer)

        self.assertEqual(msg.value, self.test_message)
        self.assertEqual(msg.key, self.test_key)

    @unittest.skipIf(os.environ.get("SHORT"), "Skipping long tests")
    def test_produce_thousand_of_messages(self):
        consumer = KafkaConsumer(self.test_topic)
        self.producer.start()
        n_messages = 1000

        async def produce_messages():
            for _ in range(n_messages):
                await self.producer.produce(self.test_topic, self.test_message)
        self.loop.run_until_complete(produce_messages())

        msgs = [next(consumer) for _ in range(n_messages)]
        self.assertTrue(all([msg.value == self.test_message for msg in msgs]))

    def test_two_starts_raise_producer_error(self):
        self.producer.start()
        self.loop.run_until_complete(asyncio.sleep(0))
        with self.assertRaises(exceptions.ProducerError):
            self.producer.start()

    def test_stops_raise_producer_error(self):
        self.producer.start()
        self.loop.run_until_complete(asyncio.sleep(0))
        self.producer.stop()
        with self.assertRaises(exceptions.ProducerError):
            self.producer.stop()

    def test_stop_without_start_raise_producer_error(self):
        with self.assertRaises(exceptions.ProducerError):
            self.producer.stop()

    def test_error_callback(self):
        error_event = Event()

        async def error_callback(kafka_error):
            error_event.set()

        # Should be a wrong port
        self.producer = Producer(
            brokers="127.0.0.1:6000",
            error_callback=error_callback
        )

        async def wait_for_event():
            while True:
                await asyncio.sleep(0.5)
                if error_event.is_set():
                    break

        self.producer.start()
        coro = asyncio.wait_for(wait_for_event(), timeout=10)
        self.loop.run_until_complete(coro)
