import logging
import sys
import os

import asyncio

from asynckafka.consumer.consumer cimport Consumer
from tests.integration_tests.test_utils import IntegrationTestCase, \
    test_consumer_settings, test_topic_settings, produce_to_kafka
from asynckafka.includes cimport c_rd_kafka as crdk
from asynckafka.consumer.topic_partition cimport \
    current_partition_assignment

# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


class TestsUnitDevelop(IntegrationTestCase):

    @unittest.skipIf(os.environ.get("DEVELOP") != True, "Skipping long tests")
    def test_assignment(self):
        cdef Consumer consumer = Consumer(
            brokers=self.brokers,
            topics=[self.test_topic],
            rdk_consumer_config=test_consumer_settings,
            rdk_topic_config=test_topic_settings,
            loop=self.loop,
        )
        confirm_message = asyncio.Future(loop=self.loop)

        async def consume_messages():
            async for message in consumer:
                print("consumed message")
                confirm_message.set_result(message)

        consumer.start()
        produce_to_kafka(self.test_topic, self.test_message)

        asyncio.ensure_future(consume_messages(), loop=self.loop)
        coro = asyncio.wait_for(confirm_message, timeout=10, loop=self.loop)
        self.loop.run_until_complete(coro)

        cdef crdk.rd_kafka_topic_partition_list_t *partitions = new_partition()
        err = crdk.rd_kafka_assignment(
            consumer.rdk_consumer.consumer,
            &partitions
        )
        if err:
            error_str = crdk.rd_kafka_err2str(err)
            self.fail("error returning assignment")
        print_partition_list_info(partitions)
        err = crdk.rd_kafka_subscription(
            consumer.rdk_consumer.consumer,
            &consumer.rdk_consumer.topic_partition_list
        )
        if err:
            error_str = crdk.rd_kafka_err2str(err)
            self.fail("error returning assignment")
        print_partition_list_info(consumer.rdk_consumer.topic_partition_list)

        topic_partition_list = current_partition_assignment(
            consumer.rdk_consumer.consumer)
        print(f"""
        TOPIC PARTITION FACTORY: 
        {topic_partition_list}
        """)

        consumer.stop()


cdef crdk.rd_kafka_topic_partition_list_t* new_partition():
    cdef crdk.rd_kafka_topic_partition_list_t partition_list = \
        crdk.rd_kafka_topic_partition_list_t()
    return &partition_list


cdef print_partition_list_info(
        crdk.rd_kafka_topic_partition_list_t *topic_partition_list):
    print(f"Partition list cnt: {topic_partition_list.cnt}")
    print(f"Partition list size: {topic_partition_list.size}")

    for index in range(topic_partition_list.cnt):
        element = topic_partition_list.elems[index]
        print(f"Partition topic: {element.topic}")
        print(f"Partition offset: {element.offset}")
        print(f"Partition partition: {element.partition}")
        metadata_ptr = <char*> element.metadata
        print(f"Partition metadata: {metadata_ptr[:element.metadata_size]}")
