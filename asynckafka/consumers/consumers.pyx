import asyncio
import logging


from asynckafka.consumers.rd_kafka_consumer cimport RdKafkaConsumer
from asynckafka.consumers.consumer_thread cimport ConsumerThread
from asynckafka.includes cimport c_rd_kafka as crdk
from asynckafka import exceptions

logger = logging.getLogger('asynckafka')


cdef class Consumer:

    def __cinit__(self, brokers, group_id=None, consumer_settings=None,
                  topic_settings=None, message_handlers=None, loop=None,
                  spawn_tasks=True, debug=False):
        self._rdk_consumer = RdKafkaConsumer(
            brokers=brokers, group_id=group_id,
            consumer_settings=consumer_settings,
            topic_settings=topic_settings
        )
        self._consumer_thread = ConsumerThread(self._rdk_consumer, debug=debug)

        message_handlers = message_handlers if message_handlers else {}
        self.message_handlers = {
            key.encode(): value
            for key, value in  message_handlers.items()
        }
        self.loop = loop if loop else asyncio.get_event_loop()
        self._poll_consumer_thread_task = None
        self._debug = 1 if debug else 0
        self._spawn_tasks = spawn_tasks

    async def _poll_consumer_thread(self):
        cdef long message_memory_address
        while True:
            try:
                try:
                    message_memory_address = \
                        self._consumer_thread.thread_communication_list.pop()
                except IndexError:
                    await asyncio.sleep(0.01, loop=self.loop)
                else:
                    if self._spawn_tasks:
                        self._open_asyncio_task(message_memory_address)
                    else:
                        try:
                            await self._call_message_handler(
                                message_memory_address)
                        finally:
                            rk_message = <crdk.rd_kafka_message_t*> \
                                message_memory_address
                            self._consumer_thread.\
                                decrease_consumption_limiter()
                            crdk.rd_kafka_message_destroy(rk_message)
            except asyncio.CancelledError:
                logger.info("Poll consumer thread task canceled correctly")
                return
            except Exception:
                logger.critical(
                    "Unexpected exception consuming messages from thread",
                    exc_info=True
                )
                raise

    cdef _open_asyncio_task(self, long message_memory_address):
        rkmessage = <crdk.rd_kafka_message_t*> message_memory_address
        payload_ptr = <char*>rkmessage.payload
        payload_len = rkmessage.len
        topic = crdk.rd_kafka_topic_name(rkmessage.rkt)

        message_handler_coroutine = \
            self.message_handlers[topic](payload_ptr[:payload_len])
        message_handler_task = asyncio.ensure_future(
            message_handler_coroutine, loop=self.loop)
        message_handler_task.add_done_callback(
            self._cb_coroutine_counter_decrease)
        crdk.rd_kafka_message_destroy(rkmessage)

    def _cb_coroutine_counter_decrease(self, _):
        self._consumer_thread.decrease_consumption_limiter()

    async def _call_message_handler(self, long message_memory_address):
        rkmessage = <crdk.rd_kafka_message_t*> message_memory_address
        payload_ptr = <char*>rkmessage.payload
        payload_len = rkmessage.len
        topic = crdk.rd_kafka_topic_name(rkmessage.rkt)
        try:
            await self.message_handlers[topic](payload_ptr[:payload_len])
        except Exception:
            logger.error(f"Not captured exception in message handler "
                         f"of topic {str(topic)}", exc_info=True)

    def start(self):
        logger.debug("Starting asynckafka consumer")
        if not self.message_handlers:
            logger.error("Asynckafka consumer can't be started without "
                         "message handlers.")
            raise exceptions.ConsumerError(
                "At least one message handler is needed before start the "
                "consumer."
            )
        self._rdk_consumer.start()
        self._consumer_thread.start()
        self._poll_consumer_thread_task = asyncio.ensure_future(
            self._poll_consumer_thread(), loop=self.loop
        )

    def stop(self):
        logger.info("Stopping asynckafka consumer")
        logger.info("Closing poll consumer thread task")
        self._poll_consumer_thread_task.cancel()
        self._consumer_thread.stop()
        self._rdk_consumer.stop()
        logger.info("Stopped correctly asynckafka consumer")

    def add_message_handler(self, topic, message_handler):
        logger.info(f"Added message handler for topic {topic}")
        encoded_topic = topic.encode()
        self._rdk_consumer.add_topic(topic)
        self.message_handlers[encoded_topic] = message_handler


cdef class StreamConsumer:

    def __cinit__(self, brokers, topic, group_id=None, consumer_settings=None,
                  topic_settings=None, loop=None, debug=False):
        self._rd_kafka = RdKafkaConsumer(
            brokers=brokers, group_id=group_id, topic_settings=topic_settings,
            consumer_settings=consumer_settings
        )
        self._topic = topic.encode()
        self._rd_kafka.add_topic(topic)
        self._consumer_thread = ConsumerThread(self._rd_kafka, debug=debug)
        self._loop = loop if loop else asyncio.get_event_loop()
        self._stop = False
        self._debug = 1 if debug else 0

    def start(self):
        self._rd_kafka.start()
        self._consumer_thread.start()

    def stop(self):
        self._stop = True
        self._consumer_thread.stop()
        self._rd_kafka.stop()

    def __aiter__(self):
        return self

    async def __anext__(self):
        cdef long message_memory_address
        while not self._stop:
            try:
                try:
                    message_memory_address = \
                        self._consumer_thread.thread_communication_list.pop()
                except IndexError:
                    await asyncio.sleep(0.01, loop=self._loop)
                else:
                    rk_message = \
                        <crdk.rd_kafka_message_t*> message_memory_address
                    payload_ptr = <char*>rk_message.payload
                    payload_len = rk_message.len
                    payload = payload_ptr[:payload_len]
                    message_memory_view = memoryview(payload)
                    self._destroy_message(message_memory_address)
                    return message_memory_view
            except Exception:
                error_str = "Unexpected exception consuming messages from " \
                            "thread in async iterator consumer"
                logger.error(error_str, exc_info=True)
                raise
        raise StopAsyncIteration

    cdef _destroy_message(self, long message_memory_address):
        rk_message = <crdk.rd_kafka_message_t*> message_memory_address
        crdk.rd_kafka_message_destroy(rk_message)
        self._consumer_thread.decrease_consumption_limiter()

