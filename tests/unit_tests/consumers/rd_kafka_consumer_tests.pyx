import logging
import unittest

from asynckafka import exceptions
from asynckafka.consumers.rd_kafka_consumer cimport RdKafkaConsumer, \
    consumer_states
from asynckafka.includes cimport c_rd_kafka as crdk


cdef RdKafkaConsumer consumer_factory():
    return RdKafkaConsumer(
        brokers='127.0.0.1',
        group_id='my_test_group_id',
        consumer_settings={},
        topic_settings={},
    )


class TestsUnitRdKafkaConsumer(unittest.TestCase):

    def test_instance_without_settings(self):
        rd_kafka_consumer = consumer_factory()
        self.assertTrue(isinstance(rd_kafka_consumer, RdKafkaConsumer))

    def test_encode_settings(self):
        input = {
            'key_1': 'value',
            'key_2': 'value'
        }
        expected_output = {
            b'key_1': b'value',
            b'key_2': b'value'
        }
        output = RdKafkaConsumer._encode_settings(input)
        self.assertEqual(expected_output, output, "Incorrect encoding")

    def test_parse_settings(self):
        input = {
            'key_1': 'my_value',
            '_': 'my_value',
            '_key_3_': 'my_value'
        }
        expected_output = {
            'key.1': 'my_value',
            '.': 'my_value',
            '.key.3.': 'my_value'
        }
        output = RdKafkaConsumer._parse_settings(input)
        self.assertEqual(expected_output, output, "Incorrect parsing")

    def test_parse_and_encode_settings(self):
        input = {
            'key_1': 'my_value',
            '_': 'my_value',
            '_key_3_': 'my_value'
        }
        expected_output = {
            b'key.1': b'my_value',
            b'.': b'my_value',
            b'.key.3.': b'my_value'
        }
        output = RdKafkaConsumer._parse_and_encode_settings(input)
        self.assertEqual(expected_output, output, "Incorrect parsing")

    def test_parse_rd_kafka_conf_response_ok(self):
        with self.assertLogs("asynckafka", logging.DEBUG):
            RdKafkaConsumer._parse_rd_kafka_conf_response(
                crdk.RD_KAFKA_CONF_OK,
                b'key',
                b'value'
            )

    def test_parse_rd_kafka_conf_response_conf_invalid(self):
        with self.assertLogs("asynckafka", logging.ERROR):
            with self.assertRaises(exceptions.InvalidSetting):
                RdKafkaConsumer._parse_rd_kafka_conf_response(
                    crdk.RD_KAFKA_CONF_INVALID,
                    b'setting_key',
                    b'setting_value'
                )

    def test_parse_rd_kafka_conf_response_conf_unknown(self):
        with self.assertLogs("asynckafka", logging.ERROR):
            with self.assertRaises(exceptions.UnknownSetting):
                RdKafkaConsumer._parse_rd_kafka_conf_response(
                    crdk.RD_KAFKA_CONF_UNKNOWN,
                    b'setting_key',
                    b'setting_value'
                )

    def test_add_topic(self):
        rd_kafka_consumer = consumer_factory()
        rd_kafka_consumer.add_topic('my_topic')
        self.assertIn(b'my_topic', rd_kafka_consumer.topics)

    def test_init_add_group_to_settings(self):
        rd_kafka_consumer = consumer_factory()
        self.assertDictEqual(
            rd_kafka_consumer.consumer_settings,
            {b'group.id': b'my_test_group_id'},
        )
        self.assertDictEqual(
            rd_kafka_consumer.topic_settings,
            {b'offset.store.method': b'broker'}
        )

    def test_init_no_add_group_to_settings(self):
        rd_kafka_consumer = RdKafkaConsumer(
            brokers='127.0.0.1',
            group_id=None,
            consumer_settings={},
            topic_settings={},
        )
        self.assertDictEqual(rd_kafka_consumer.consumer_settings, {})
        self.assertDictEqual(rd_kafka_consumer.topic_settings, {})

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

    def test_cb_logger(self):
        raise NotImplemented

    def test_cb_rebalance(self):
        raise NotImplemented

