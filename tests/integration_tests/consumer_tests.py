import asyncio
import logging
import sys
from multiprocessing import Event


from asynckafka import exceptions
from asynckafka.consumer.consumer import Consumer
from tests.integration_tests.test_utils import IntegrationTestCase, \
    test_consumer_settings, test_topic_settings, produce_to_kafka

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


class TestIntegrationConsumer(IntegrationTestCase):

    def setUp(self):
        super().setUp()
        self.stream_consumer = Consumer(
            brokers=self.brokers,
            topics=[self.test_topic],
            consumer_settings=test_consumer_settings,
            topic_settings=test_topic_settings,
            loop=self.loop,
        )

    def tearDown(self):
        if self.stream_consumer.is_consuming():
            self.stream_consumer.stop()
        super().tearDown()

    def test_consume_one_message(self):
        confirm_message = asyncio.Future(loop=self.loop)

        async def consume_messages():
            async for message in self.stream_consumer:
                confirm_message.set_result(message)

        self.stream_consumer.start()

        produce_to_kafka(self.test_topic, self.test_message)

        asyncio.ensure_future(consume_messages(), loop=self.loop)
        coro = asyncio.wait_for(confirm_message, timeout=10, loop=self.loop)
        self.loop.run_until_complete(coro)

        consumed_message = confirm_message.result()
        self.assertEqual(
            consumed_message.payload,
            self.test_message
        )
        self.assertEqual(
            consumed_message.topic,
            self.test_topic
        )
        self.assertTrue(isinstance(consumed_message.key, bytes))
        self.assertTrue(isinstance(consumed_message.offset, int))

    def test_consume_one_thousand_of_messages(self):
        n_messages = 1000
        consumed_messages = asyncio.Queue(maxsize=n_messages, loop=self.loop)

        async def consume_messages():
            async for message in self.stream_consumer:
                consumed_messages.put_nowait(message)

        self.stream_consumer.start()

        produce_to_kafka(self.test_topic, self.test_message, number=1000)

        asyncio.ensure_future(consume_messages(), loop=self.loop)

        async def wait_for_messages():
            while True:
                await asyncio.sleep(0.1)
                if consumed_messages.qsize() == n_messages:
                    break

        coro = asyncio.wait_for(
            wait_for_messages(),
            timeout=30,
            loop=self.loop
        )
        self.loop.run_until_complete(coro)

        for _ in range(n_messages):
            self.assertEqual(
                consumed_messages.get_nowait().payload,
                self.test_message
            )

    def test_two_starts_raise_consumer_error(self):
        self.stream_consumer.start()
        with self.assertRaises(exceptions.ConsumerError):
            self.stream_consumer.start()

    def test_stops_raise_consumer_error(self):
        self.stream_consumer.start()
        self.stream_consumer.stop()
        with self.assertRaises(exceptions.ConsumerError):
            self.stream_consumer.stop()

    def test_stop_without_start_raise_consumer_error(self):
        with self.assertRaises(exceptions.ConsumerError):
            self.stream_consumer.stop()

    def test_error_callback(self):
        error_event = Event()

        async def error_callback(kafka_error):
            error_event.set()

        self.stream_consumer = Consumer(
            brokers="127.0.0.1:60000",
            topics=[self.test_topic],
            consumer_settings=test_consumer_settings,
            topic_settings=test_topic_settings,
            error_callback=error_callback,
            loop=self.loop,
        )

        async def wait_for_event():
            while True:
                await asyncio.sleep(0.5)
                if error_event.is_set():
                    break

        self.stream_consumer.start()
        coro = asyncio.wait_for(wait_for_event(), timeout=10)
        self.loop.run_until_complete(coro)

