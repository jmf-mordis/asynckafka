import asyncio
import logging

from asynckafka import exceptions, utils
from asynckafka.callbacks import register_error_callback, \
    unregister_error_callback
from asynckafka.settings import CONSUMER_RD_KAFKA_POLL_PERIOD_SECONDS
from asynckafka.consumer.message cimport message_factory
from asynckafka.consumer.rd_kafka_consumer cimport RdKafkaConsumer
from asynckafka.includes cimport c_rd_kafka as crdk
from asynckafka.settings cimport debug


logger = logging.getLogger('asynckafka')


cdef class Consumer:
    """
    TODO DOC

    Example:
        Consumer works as a async iterator::

            consumer = Consumer(['my_topic'])
            consumer.start()

            async for message in consumer:
                print(message.payload)

    Args:
        brokers (str): Brokers separated with ",", example:
            "192.168.1.1:9092,192.168.1.2:9092".
        topics (list): Topics to consume.
        group_id (str): Consumer group identifier.
        rdk_consumer_config (dict): Rdkafka consumer settings.
        rdk_topic_config (dict): Rdkafka topic settings.
        error_callback (Coroutine[asyncio.exceptions.KafkaError]): Coroutine
            with one argument (KafkaError). It is scheduled in the loop when
            there is an error, for example, if the broker is down.
        loop (asyncio.AbstractEventLoop): Asyncio event loop.
    """
    def __init__(self, topics, brokers=None, group_id=None,
                 rdk_consumer_config=None, rdk_topic_config=None,
                 error_callback=None, loop=None):
        # TODO add auto_partition_assigment as parameter
        self.rdk_consumer = RdKafkaConsumer(
            brokers=brokers, group_id=group_id, topic_config=rdk_topic_config,
            consumer_config=rdk_consumer_config
        )
        assert isinstance(topics, list), "Topics should be a list"
        assert len(topics) >= 1, "It is necessary at least one topic"
        self.topics = [topic.encode() for topic in topics]
        [self.rdk_consumer.add_topic(topic) for topic in topics]
        self.loop = loop if loop else asyncio.get_event_loop()
        self.consumer_state = consumer_states.NOT_CONSUMING
        self.poll_rd_kafka_task = None
        self.error_callback = error_callback


    def seek(self, topic_partition, timeout=500):
      """
      Seek the topic_partition to specified offset.

      Example:
        topic_partition.offset = seek_offset
        consumer.seek(topic_partition)

      Raises:
          asynckafka.exceptions.ConsumerError: Request timeout.
      """
      self.rdk_consumer.seek(topic_partition, timeout)

    def is_consuming(self):
        """
        Method for check the consumer state.

        Returns:
            bool: True if the consumer is consuming false if not.
        """
        return self.consumer_state == consumer_states.CONSUMING

    def is_stopped(self):
        """
        Method for check the consumer state.

        Returns:
            bool: True if the consumer is stopped false if not.
        """
        return not self.is_consuming()

    def start(self):
        """
        Start the consumer. It is necessary call this method before start to
        consume messages.

        Raises:
            asynckafka.exceptions.ConsumerError: Error in the initialization of
                the consumer client.
            asynckafka.exceptions.InvalidSetting: Invalid setting in
                consumer_settings or topic_settings.
            asynckafka.exceptions.UnknownSetting: Unknown setting in
                consumer_settings or topic_settings.
        """
        if self.is_consuming():
            logger.error("Tried to start a consumer that it is already "
                         "running")
            raise exceptions.ConsumerError("Consumer is already running")
        else:
            logger.info("Called consumer start")
            self.rdk_consumer.start()
            logger.info("Creating rd kafka poll task")
            self.poll_rd_kafka_task = asyncio.ensure_future(
                utils.periodic_rd_kafka_poll(
                    <long> self.rdk_consumer.consumer,
                    self.loop
                ), loop=self.loop
            )
            self._post_start()
            if self.error_callback:
                register_error_callback(
                    self, self.rdk_consumer.get_name())
            self.consumer_state = consumer_states.CONSUMING
            logger.info('Consumer started')

    def assign_topic_offset(self, topic, partition, offset):
      """
      Assign topic/partition with specified offset.
      """
      self.rdk_consumer.assign_topic_offset(topic, partition, offset)

    def _post_start(self):
        """
        Internal method to be overwritten by other consumers that use this one
        as Base.
        """
        pass

    def stop(self):
        """
        Stop the consumer. It is advisable to call this method before
        closing the python interpreter. It are going to stop the
        asynciterator, asyncio tasks opened by this client and free the
        memory used by the consumer.

        Raises:
            asynckafka.exceptions.ConsumerError: Error in the shut down of
                the consumer client.
            asynckafka.exceptions.InvalidSetting: Invalid setting in
                consumer_settings or topic_settings.
            asynckafka.exceptions.UnknownSetting: Unknown setting in
                consumer_settings or topic_settings.
        """
        if not self.is_consuming():
            logger.error("Tried to stop a consumer that it is already "
                         "stopped")
            raise exceptions.ConsumerError("Consumer isn't running")
        else:
            logger.info("Stopping asynckafka consumer")
            self._pre_stop()
            if self.error_callback:
                unregister_error_callback(self.rdk_consumer.get_name())
            logger.info("Closing rd kafka poll task")
            self.poll_rd_kafka_task.cancel()
            self.rdk_consumer.stop()
            logger.info("Stopped correctly asynckafka consumer")
            self.consumer_state = consumer_states.NOT_CONSUMING

    def _pre_stop(self):
        """
        Internal method to be overwritten by other consumers that use this one
        as Base.
        """
        pass

    def assignment(self):
        """
        Partition assignment of the consumer.

        Returns:
            [asynckafka.consumer.topic_partition.TopicPartition]: List with
                the current partition assignment of the consumer.
        """
        return self.rdk_consumer.assignment()

    async def commit_partitions(self, partitions):
        """
        Commit topic partitions.

        Args:
            [asynckafka.consumer.topic_partition.TopicPartition]: topic
                partitions to commit.
        """
        pass

    async def offset_store(self):
        pass

    def __aiter__(self):
        """
        Allows the consumer to work as a async iterator.
        """
        if self.is_consuming():
            return self
        else:
            raise exceptions.ConsumerError("Consumer isn't consuming")

    async def __anext__(self):
        """
        Polls rdkafka consumer and build the message object.

        Returns:
            asynckafka.consumer.message.Message: Message consumed.
        """
        cdef crdk.rd_kafka_message_t *rk_message
        try:
            while self.consumer_state == consumer_states.CONSUMING:
                    rk_message = crdk.rd_kafka_consumer_poll(
                        self.rdk_consumer.consumer, 0)
                    if rk_message:
                        message = message_factory(rk_message)
                        if not message.error:
                            return message
                    else:
                        if debug: logger.debug("Poll timeout without messages")
                        await asyncio.sleep(
                            CONSUMER_RD_KAFKA_POLL_PERIOD_SECONDS,
                            loop=self.loop
                        )
        except asyncio.CancelledError:
            logger.info("Poll consumer thread task canceled")
            raise
        except Exception:
            logger.error(
                "Unexpected exception consuming messages from thread",
                exc_info=True
            )
        raise StopAsyncIteration
