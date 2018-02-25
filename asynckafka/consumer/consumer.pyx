import asyncio
import logging

from asynckafka import exceptions, utils
from asynckafka.settings import CONSUMER_RD_KAFKA_POLL_PERIOD_SECONDS
from asynckafka.consumer.message cimport message_factory
from asynckafka.consumer.rd_kafka_consumer cimport RdKafkaConsumer
from asynckafka.includes cimport c_rd_kafka as crdk
from asynckafka.settings cimport debug

logger = logging.getLogger('asynckafka')


cdef class Consumer:
    """
    TODO doc
    """
    def __init__(self, brokers, topics, group_id=None, consumer_settings=None,
                 topic_settings=None, loop=None):
        """
        Args:
            brokers (str): Brokers separated with ",", example:
            "192.168.1.1:9092,192.168.1.2:9092".
            topics (list): Topics to consume.
            group_id (str): Consumer group identifier.
            consumer_settings (dict): Consumer rdkafka configuration.
            topic_settings (dict): Topic rdkafka settings.
            loop (asyncio.AbstractEventLoop): Asyncio event loop.
        """
        self.rdk_consumer = RdKafkaConsumer(
            brokers=brokers, group_id=group_id, topic_settings=topic_settings,
            consumer_settings=consumer_settings
        )
        assert isinstance(topics, list), "Topics should be a list"
        self.topics = [topic.encode() for topic in topics]
        [self.rdk_consumer.add_topic(topic) for topic in topics]
        self.loop = loop if loop else asyncio.get_event_loop()
        self.consumer_state = consumer_states.NOT_CONSUMING
        self.poll_rd_kafka_task = None

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
            self.consumer_state = consumer_states.CONSUMING
            logger.info('Consumer started')

    def _post_start(self):
        """
        Internal method to be overwritten by other consumers that use this one
        as Base.
        Returns:
        """
        pass

    def stop(self):
        """
        Stop the consumer. Tt is advisable to call this method before
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
            logger.info("Closing rd kafka poll task")
            self.poll_rd_kafka_task.cancel()
            self.rdk_consumer.stop()
            logger.info("Stopped correctly asynckafka consumer")
            self.consumer_state = consumer_states.NOT_CONSUMING

    def _pre_stop(self):
        """
        Internal method to be overwritten by other consumers that use this one
        as Base.
        Returns:
        """
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
