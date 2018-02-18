import logging
import unittest

from asynckafka.consumers.consumer_thread cimport ConsumerThread
from asynckafka.consumers.rd_kafka_consumer cimport RdKafkaConsumer
from asynckafka.includes cimport c_rd_kafka as crdk


cdef RdKafkaConsumer rd_kafka_consumer_factory():
    rd_kafka_consumer = RdKafkaConsumer(
        brokers='127.0.0.1',
        group_id='my_test_group_id',
        consumer_settings={},
        topic_settings={},
    )
    rd_kafka_consumer.add_topic("my_test_topic")
    return rd_kafka_consumer

cdef ConsumerThread consumer_thread_factory():
    return ConsumerThread(
        rd_kafka_consumer_factory(),
        debug=True
    )

class TestsUnitConsumerThread(unittest.TestCase):

    def test_consumer_thread_instance(self):
        consumer_thread_factory()

    def test_is_in_debug(self):
        consumer_thread = consumer_thread_factory()
        self.assertTrue(consumer_thread.is_in_debug())

    def test_change_debug(self):
        consumer_thread = consumer_thread_factory()
        consumer_thread.set_debug(False)
        self.assertFalse(consumer_thread.is_in_debug())

    def test_increase_consumption_limiter(self):
        consumer_thread = consumer_thread_factory()
        self.assertEqual(consumer_thread.consumption_limiter, 0)
        consumer_thread._increase_consumption_limiter()
        self.assertEqual(consumer_thread.consumption_limiter, 1)

    def test_decrease_consumption_limiter(self):
        consumer_thread = consumer_thread_factory()
        consumer_thread.consumption_limiter = 1000
        consumer_thread.decrease_consumption_limiter()
        self.assertEqual(consumer_thread.consumption_limiter, 999)

    def test_cb_consume_message_partition_eof(self):
        cdef crdk.rd_kafka_message_t test_rk_message
        consumer_thread = consumer_thread_factory()
        test_rk_message = crdk.rd_kafka_message_t(
            err=crdk.RD_KAFKA_RESP_ERR__PARTITION_EOF)
        with self.assertLogs("asynckafka", logging.DEBUG):
            consumer_thread._cb_consume_message(&test_rk_message)

    def test_cb_consume_message_error_with_rkt(self):
        raise NotImplemented

    def test_cb_consume_message_error_without_rkt(self):
        cdef crdk.rd_kafka_message_t test_rk_message
        consumer_thread = consumer_thread_factory()
        test_rk_message = crdk.rd_kafka_message_t(
            err=crdk.RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN,
            rkt=NULL
        )
        with self.assertLogs("asynckafka", logging.ERROR):
            consumer_thread._cb_consume_message(&test_rk_message)

    def test_cb_consume_message(self):
        cdef crdk.rd_kafka_message_t test_rk_message
        cdef crdk.rd_kafka_message_t *poped_rk_message
        cdef long memory_address
        consumer_thread = consumer_thread_factory()
        consumer_thread.set_debug(False)

        payload = b"my_message"
        cdef char *payload_ptr = payload

        test_rk_message = crdk.rd_kafka_message_t(
            payload=payload_ptr,
            len=len(payload),
            err=crdk.RD_KAFKA_RESP_ERR_NO_ERROR,
            rkt=NULL
        )
        self.assertEqual(len(consumer_thread.thread_communication_list), 0)
        consumer_thread._cb_consume_message(&test_rk_message)
        self.assertEqual(len(consumer_thread.thread_communication_list), 1)

        memory_address = consumer_thread.thread_communication_list.pop()
        poped_rk_message = <crdk.rd_kafka_message_t*>memory_address
        self.assertEqual(poped_rk_message.len, test_rk_message.len)



