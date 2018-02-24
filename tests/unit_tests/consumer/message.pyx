import logging
import unittest

from asynckafka.includes cimport c_rd_kafka as crdk


class TestsUnitConsumerThread(unittest.TestCase):

    def test_cb_consume_message_error_with_rkt(self):
        raise NotImplemented

    def test_error_message_without_rkt(self):
        cdef crdk.rd_kafka_message_t test_rk_message
        consumer_thread = consumer_thread_factory()
        test_rk_message = crdk.rd_kafka_message_t(
            err=crdk.RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN,
            rkt=NULL
        )
        with self.assertLogs("asynckafka", logging.ERROR):
            consumer_thread._cb_consume_message(&test_rk_message)
