import logging
import unittest

from asynckafka import exceptions
from asynckafka import utils
from asynckafka.includes cimport c_rd_kafka as crdk


class TestsUnitUtils(unittest.TestCase):

    def test_encode_settings(self):
        input = {
            'key_1': 'value',
            'key_2': 'value'
        }
        expected_output = {
            b'key_1': b'value',
            b'key_2': b'value'
        }
        output = utils._encode_settings(input)
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
        output = utils._parse_settings(input)
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
        output = utils.parse_and_encode_settings(input)
        self.assertEqual(expected_output, output, "Incorrect parsing")

    def test_parse_rd_kafka_conf_response_ok(self):
        with self.assertLogs("asynckafka", logging.DEBUG):
            utils.parse_rd_kafka_conf_response(
                crdk.RD_KAFKA_CONF_OK,
                b'key',
                b'value'
            )

    def test_parse_rd_kafka_conf_response_conf_invalid(self):
        with self.assertLogs("asynckafka", logging.ERROR):
            with self.assertRaises(exceptions.InvalidSetting):
                utils.parse_rd_kafka_conf_response(
                    crdk.RD_KAFKA_CONF_INVALID,
                    b'setting_key',
                    b'setting_value'
                )

    def test_parse_rd_kafka_conf_response_conf_unknown(self):
        with self.assertLogs("asynckafka", logging.ERROR):
            with self.assertRaises(exceptions.UnknownSetting):
                utils.parse_rd_kafka_conf_response(
                    crdk.RD_KAFKA_CONF_UNKNOWN,
                    b'setting_key',
                    b'setting_value'
                )
