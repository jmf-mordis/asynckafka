from tests.integration_tests.consumer_tests import \
    TestIntegrationConsumer
from tests.integration_tests.producer_tests import TestsIntegrationProducer
include "./unit_tests/utils_tests.pyx"
include "./unit_tests/rd_kafka_consumer_tests.pyx"
include "./unit_tests/develop_tests.pyx"
import logging
logger = logging.getLogger("asynckafka")
logger.setLevel("CRITICAL")
logger = logging.getLogger("asyncio")
logger.setLevel("CRITICAL")
