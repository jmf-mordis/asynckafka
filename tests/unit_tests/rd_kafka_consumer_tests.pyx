import unittest

from asynckafka import exceptions
from asynckafka.consumer.rd_kafka_consumer cimport RdKafkaConsumer, \
    consumer_states


cdef RdKafkaConsumer consumer_factory():
    return RdKafkaConsumer(
        brokers='127.0.0.1',
        group_id='my_test_group_id',
        consumer_config={},
        topic_config={},
    )


class TestsUnitRdKafkaConsumer(unittest.TestCase):

    def test_instance_without_settings(self):
        rd_kafka_consumer = consumer_factory()
        self.assertTrue(isinstance(rd_kafka_consumer, RdKafkaConsumer))

    def test_add_topic(self):
        rd_kafka_consumer = consumer_factory()
        rd_kafka_consumer.add_topic('my_topic')
        self.assertIn(b'my_topic', rd_kafka_consumer.topics)

    def test_init_add_group_to_settings(self):
        rd_kafka_consumer = consumer_factory()
        self.assertDictEqual(
            rd_kafka_consumer.consumer_config,
            {b'group.id': b'my_test_group_id'},
        )
        self.assertDictEqual(
            rd_kafka_consumer.topic_config,
            {b'offset.store.method': b'broker'}
        )

    def test_init_no_add_group_to_settings(self):
        rd_kafka_consumer = RdKafkaConsumer(
            brokers='127.0.0.1',
            group_id=None,
            consumer_config={},
            topic_config={},
        )
        self.assertDictEqual(
            rd_kafka_consumer.consumer_config,
            {b'group.id': b'default_consumer_group'}
        )
        self.assertDictEqual(
            rd_kafka_consumer.topic_config,
            {b'offset.store.method': b'broker'}
        )

    def test_init_rd_kafka_configs(self):
        rd_kafka_consumer = consumer_factory()
        rd_kafka_consumer.add_topic('my_topic')
        rd_kafka_consumer._init_rd_kafka_configs()

    def test_start_without_topic(self):
        rd_kafka_consumer = consumer_factory()
        with self.assertRaises(exceptions.ConsumerError):
            rd_kafka_consumer.start()
        self.assertEqual(rd_kafka_consumer.status, consumer_states.NOT_STARTED)

    def test_start_stop(self):
        rd_kafka_consumer = consumer_factory()
        rd_kafka_consumer.add_topic('my_topic')
        self.assertEqual(rd_kafka_consumer.status, consumer_states.NOT_STARTED)
        rd_kafka_consumer.start()
        self.assertEqual(rd_kafka_consumer.status, consumer_states.STARTED)
        rd_kafka_consumer.stop()
        self.assertEqual(rd_kafka_consumer.status, consumer_states.STOPPED)
