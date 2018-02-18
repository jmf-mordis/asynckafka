import logging
import time
from threading import Thread, Event

from asynckafka.includes cimport c_rd_kafka as crdk
from asynckafka import settings

logger = logging.getLogger('asynckafka')


cdef class ConsumerThread:

    def __init__(self, RdKafkaConsumer rd_kafka_struct, debug=False):
        self._rd_kafka = rd_kafka_struct
        self.thread = Thread(target=self._main_poll_rdkafka)
        self.thread_communication_list = []
        self.consumption_limiter = 0
        self.stop_event = Event()
        self._debug = 1 if debug else 0

    cpdef _main_poll_rdkafka(self):
        cdef crdk.rd_kafka_message_t *rkmessage
        cdef crdk.rd_kafka_t *kafka_consumer
        logger.info(f"Opened consumer thread.")
        try:
            while not self.stop_event.is_set():
                rkmessage = crdk.rd_kafka_consumer_poll(
                    self._rd_kafka.consumer, 10)
                if rkmessage:
                    self._cb_consume_message(rkmessage)
                else:
                    if self._debug:
                        logger.debug(
                            "thread consumer, poll timeout without messages")
            else:
                logger.info(f"Closing consumer thread.")
        except Exception:
            logger.error(f"Unexpected exception in consumer thread. "
                         "Closing thread.", exc_info=True)

    cdef inline _cb_consume_message(
            self, crdk.rd_kafka_message_t *rk_message):
        if rk_message.err:
            if rk_message.err == crdk.RD_KAFKA_RESP_ERR__PARTITION_EOF:
                if self._debug: logger.info("Partition EOF")
            elif rk_message.rkt:
                err_message_str = str(crdk.rd_kafka_message_errstr(rk_message))
                topic = str(crdk.rd_kafka_topic_name(rk_message.rkt))
                logger.error(
                    f"Consumer error in kafka topic {topic}, "
                    f"partition {rk_message.partition}, "
                    f"offset {rk_message.offset} "
                    f"error info: {err_message_str}"
                )
            else:
                err_str = crdk.rd_kafka_err2str(rk_message.err)
                err_message_str = crdk.rd_kafka_message_errstr(rk_message)
                logger.error(
                    f"Consumer error {err_str} {err_message_str}"
                )
        else:
            if self._debug:
                payload_ptr = <char*>rk_message.payload
                payload_len = rk_message.len
                topic = crdk.rd_kafka_topic_name(rk_message.rkt)
                logger.debug(
                    f"Consumed message in thread of topic {topic} "
                    f"with payload: {payload_ptr[:payload_len]}"
                )
            self._send_message_to_asyncio(rk_message)

    cdef inline _send_message_to_asyncio(
            self, crdk.rd_kafka_message_t *rkmessage):
        memory_address = <long> rkmessage
        while True:
            if self.consumption_limiter < settings.DEFAULT_MAX_COROUTINES:
                self._increase_consumption_limiter()
                self.thread_communication_list.append(memory_address)
                if self._debug: logger.debug(
                    "Sent memory address of message from consumer "
                    "thread to asyncio thread"
                )
                return
            else:
                if self.stop_event.is_set():
                    return
                logger.debug("Consumer limit reached in consumer thread")
                time.sleep(0.1)

    cdef inline _increase_consumption_limiter(self):
        self.consumption_limiter += 1

    cdef inline decrease_consumption_limiter(self):
        self.consumption_limiter -= 1

    def start(self):
        self.thread.start()

    def stop(self):
        logger.info('Stopping consumer thread')
        self.stop_event.set()
        logger.info('Triggered thread stop event, waiting for the thread to '
                    'close.')
        self.thread.join(timeout=5)
        logger.info('Thread closed, destroying all allocated messages.')
        self._destroy_remaining_messages()
        logger.info('Destroyed all the remaining messages consumed by the '
                    'thread.')

    def is_in_debug(self):
        return True if self._debug else False

    def set_debug(self, debug: bool):
        self._debug = 1 if debug else 0

    cdef _destroy_remaining_messages(self):
        for _ in range(len(self.thread_communication_list)):
            message_memory_address = self.thread_communication_list.pop()
            self.decrease_consumption_limiter()
            rkmessage = <crdk.rd_kafka_message_t*> message_memory_address
            crdk.rd_kafka_message_destroy(rkmessage)
