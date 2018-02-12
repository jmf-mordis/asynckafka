import asyncio
import logging


from asynckafka.consumers.rd_kafka_consumer cimport RdKafkaConsumer
from asynckafka.consumers.consumer_thread cimport ConsumerThread
from asynckafka.consumers.consumer_aiter import ConsumerAsyncIterator
from asynckafka.includes cimport c_rd_kafka as crdk
from asynckafka import exceptions

logger = logging.getLogger('asynckafka')


cdef class Consumer:

    def __cinit__(self, brokers, group_id=None, consumer_settings=None,
                  topic_settings=None, message_handlers=None, loop=None,
                  spawn_tasks=True, debug=False):
        self._rdk_consumer = RdKafkaConsumer(
            brokers=brokers, group_id=group_id, consumer_settings=consumer_settings,
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
                            rkmessage = <crdk.rd_kafka_message_t*> \
                                message_memory_address
                            self._consumer_thread.\
                                decrease_consumption_limiter()
                            crdk.rd_kafka_message_destroy(rkmessage)
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
        self._rdk_consumer.add_topic(encoded_topic)
        self.message_handlers[encoded_topic] = message_handler


cdef class StreamConsumer:
    cdef RdKafkaConsumer _rd_kafka
    cdef ConsumerThread _consumer_thread

    cdef object _loop
    cdef object _open_tasks_task
    cdef object _spawn_tasks
    cdef bint _debug

    cdef object _async_iterator

    def __cinit__(self, brokers, topic, group_id=None, consumer_settings=None,
                  topic_settings=None, loop=None, debug=False):
        self._rd_kafka = RdKafkaConsumer(
            brokers=brokers, group_id=group_id, consumer_settings=consumer_settings,
            topic_settings=topic_settings
        )
        self._rd_kafka.add_topic(topic.encode())
        self._consumer_thread = ConsumerThread(self._rd_kafka, debug=debug)
        self._loop = loop if loop else asyncio.get_event_loop()
        self._async_iterator = ConsumerAsyncIterator(
            self._consumer_thread.thread_communication_list,
            self._get_message,
            self._destroy_message,
            self._loop
        )
        self._debug = 1 if debug else 0

    def start(self):
        self._rd_kafka.start()
        self._consumer_thread.start()

    def stop(self):
        self._async_iterator.stop()
        self._consumer_thread.stop()
        self._rd_kafka.stop()

    def __aiter__(self):
        return self._async_iterator.get_aiter()

    cpdef bytes _get_message(self, long message_memory_address):
        rkmessage = <crdk.rd_kafka_message_t*> message_memory_address
        payload_ptr = <char*>rkmessage.payload
        payload_len = rkmessage.len
        return payload_ptr[:payload_len]

    cpdef _destroy_message(self, long message_memory_address):
        rkmessage = <crdk.rd_kafka_message_t*> message_memory_address
        crdk.rd_kafka_message_destroy(rkmessage)
        self._consumer_thread.decrease_consumption_limiter()

