import unittest

from asynckafka import exceptions
from asynckafka.consumers.rd_kafka_consumer cimport RdKafkaConsumer
from asynckafka.consumers.rd_kafka_consumer import RdKafkaConsumer
from asynckafka.includes cimport c_rd_kafka as crdk

import os
from subprocess import call



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


class TestIntegrationRdKafkaConsumer(IntegrationTestCase):

    def tests_start_and_stop(self):
        rd_kafka_consumer = RdKafkaConsumer(
            brokers='127.0.0.1',
            group_id='my_test_group_id',
            consumer_settings={},
            topic_settings={},
        )
        rd_kafka_consumer.add_topic("my_topic")
        rd_kafka_consumer.start()
        rd_kafka_consumer.stop()
