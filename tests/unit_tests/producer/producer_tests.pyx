import unittest

from asynckafka.producer.producer cimport Producer


class TestsUnitRdKafkaProducer(unittest.TestCase):

    def setUp(self):
        self.producer = Producer(
            brokers="127.0.0.1",
            topic="my_test_topic",
            debug=True,
        )

    def test_producer_instance(self):
        pass

    def test_is_in_debug(self):
        self.assertTrue(self.producer.is_in_debug())

    def test_change_debug(self):
        self.producer.set_debug(False)
        self.assertFalse(self.producer.is_in_debug())
