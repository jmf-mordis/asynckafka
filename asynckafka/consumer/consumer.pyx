import asyncio
import logging


from asynckafka.consumer.rd_kafka_consumer cimport RdKafkaConsumer
from asynckafka.includes cimport c_rd_kafka as crdk
from asynckafka.consumer.message cimport message_factory, Message
from asynckafka.settings cimport debug
from asynckafka import exceptions, utils

logger = logging.getLogger('asynckafka')


cdef class StreamConsumer:

    def __init__(self, brokers, topics, group_id=None, consumer_settings=None,
                 topic_settings=None, loop=None, debug=False):
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
        return self.consumer_state == consumer_states.CONSUMING

    def is_stopped(self):
        return not self.is_consuming()

    def start(self):
        if self.is_consuming():
            logger.error("Tried to start a consumer that it is already "
                         "running")
            raise exceptions.ConsumerError("Consumer is already running")
        else:
            self.rdk_consumer.start()
            self.poll_rd_kafka_task = asyncio.ensure_future(
                utils.periodic_rd_kafka_poll(
                    <long> self.rdk_consumer.consumer,
                    self.loop
                ), loop=self.loop
            )
            self._start()
            self.consumer_state = consumer_states.CONSUMING
            logger.debug('Consumer started')

    def _start(self):
        pass

    def stop(self):
        if not self.is_consuming():
            logger.error("Tried to stop a consumer that it is already "
                         "stopped")
            raise exceptions.ConsumerError("Consumer isn't running")
        else:
            logger.info("Stopping asynckafka consumer")
            self._stop()
            logger.info("Closing rd kafka poll task")
            self.poll_rd_kafka_task.cancel()
            self.rdk_consumer.stop()
            logger.info("Stopped correctly asynckafka consumer")
            self.consumer_state = consumer_states.NOT_CONSUMING

    def _stop(self):
        pass

    def __aiter__(self):
        if self.is_consuming():
            return self
        else:
            raise exceptions.ConsumerError("Consumer isn't consuming")

    async def __anext__(self) -> Message:
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
                        await asyncio.sleep(0.1, loop=self.loop)
        except asyncio.CancelledError:
            logger.info("Poll consumer thread task canceled")
        except Exception:
            logger.error(
                "Unexpected exception consuming messages from thread",
                exc_info=True
            )
        raise StopAsyncIteration
