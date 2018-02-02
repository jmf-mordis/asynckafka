cimport rdkafka
import asyncio
import logging

import functools
from libc cimport stdio

from libc.stdint cimport int32_t, int64_t
from threading import Thread, Event, Semaphore

from asynckafka import exceptions

from asynckafka import settings


logger = logging.getLogger('asynckafka')


cdef void cb_logger(const rdkafka.rd_kafka_t *rk, int level, const char *fac,
                    const char *buf):
    if level in {1, 2}:
        logger.critical(f"{fac}:{buf}")
    elif level == 3:
        logger.error(f"{fac}:{buf}")
    elif level in {4, 5}:
        logger.info(f"{fac}:{buf}")
    elif level in {6, 7} :
        logger.debug(f"{fac}:{buf}")
    else:
        logger.critical(f"Unexpected logger level {level}")
        logger.critical(f"{fac}:{buf}")


cdef log_partition_list(rdkafka.rd_kafka_topic_partition_list_t *partitions):
    string = "List of partitions: "
    for i in range(partitions.cnt):
        topic = partitions.elems[i].topic
        partition = partitions.elems[i].partition
        offset = partitions.elems[i].offset
        string += f"\nTopic: {topic}, Partition: {partition},  Offset: {offset}"
    logger.debug(string)


cdef void cb_rebalance(
        rdkafka.rd_kafka_t *rk, rdkafka.rd_kafka_resp_err_t err,
        rdkafka.rd_kafka_topic_partition_list_t *partitions, void *opaque):
    logger.debug("Consumer group rebalance")
    if err == rdkafka.RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
        logger.debug("New partitions assigned")
        log_partition_list(partitions)
        rdkafka.rd_kafka_assign(rk, partitions)
    elif err == rdkafka.RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
        logger.debug("Revoked Partitions")
        log_partition_list(partitions)
        rdkafka.rd_kafka_assign(rk, NULL)
    else:
        err_str = rdkafka.rd_kafka_err2str(err)
        logger.error(
            f"Error in rebalance callback, "
            f"Revoked partitions {err_str}"
        )
        rdkafka.rd_kafka_assign(rk, NULL)


cdef class Consumer:

    cdef rdkafka.rd_kafka_t *kafka_consumer
    cdef rdkafka.rd_kafka_topic_t *kafka_topic
    cdef char errstr[512]
    cdef rdkafka.rd_kafka_conf_t *conf
    cdef rdkafka.rd_kafka_topic_conf_t *topic_conf
    cdef rdkafka.rd_kafka_topic_partition_list_t *topic_list

    cdef bytes brokers
    cdef bytes topic
    cdef dict consumer_settings
    cdef dict topic_settings

    cdef object message_handler
    cdef object loop
    cdef object thread_coroutine_semaphore
    cdef object thread_stop_event
    cdef object _thread

    def __cinit__(self, brokers, topic, consumer_settings,
                  message_handler=None, loop=None, topic_settings=None):
        self.brokers = brokers.encode()
        self.topic = topic.encode()

        consumer_settings = self._parse_settings(consumer_settings)
        self.consumer_settings = {
            key.encode(): value.encode()
            for key, value in consumer_settings.items()
        }

        topic_settings = topic_settings if topic_settings else {}
        if 'group.id' in consumer_settings:
            topic_settings["offset.store.method"] = "broker"
        topic_settings = self._parse_settings(topic_settings)
        self.topic_settings = {
            key.encode(): value.encode()
            for key, value in topic_settings.items()
        }

        self.message_handler = self.wrap_message_handler(message_handler) \
            if message_handler else None
        self.loop = loop if loop else asyncio.get_event_loop()
        self.thread_coroutine_semaphore = Semaphore(
            value=settings.DEFAULT_MAX_COROUTINES
        )
        self.thread_stop_event = Event()
        self._thread = Thread(
            target=self._thread_consume_messages,
        )

        self._init_config()
        self._consumer_settings_to_rdkafka()
        self._init_consumer_group()
        self._init_consumer()
        self._init_topic()
        self._init_subscription()

    def wrap_message_handler(self, message_handler):
        # TODO fix memory copy
        @functools.wraps(message_handler)
        async def wrapper_message_handler(payload):
            try:
                await message_handler(payload)
            finally:
                self.thread_coroutine_semaphore.release()
        return wrapper_message_handler

    cpdef _init_config(self):
        self.conf = rdkafka.rd_kafka_conf_new()
        rdkafka.rd_kafka_conf_set_log_cb(self.conf, cb_logger)
        self.topic_conf = rdkafka.rd_kafka_topic_conf_new()

    cpdef _consumer_settings_to_rdkafka(self):
        for key, value in self.consumer_settings.items():
            conf_resp = rdkafka.rd_kafka_conf_set(
                self.conf, key, value, self.errstr, sizeof(self.errstr)
            )
            self._parse_conf_response(conf_resp, key, value)
        for key, value in self.topic_settings.items():
            conf_resp = rdkafka.rd_kafka_topic_conf_set(
                self.topic_conf, key, value, self.errstr, sizeof(self.errstr)
            )
            self._parse_conf_response(conf_resp, key, value)
        # Set default topic config for pattern-matched topics. */
        rdkafka.rd_kafka_conf_set_default_topic_conf(self.conf, self.topic_conf)

    def _parse_settings(self, config: dict) -> dict:
        return {key.replace("_", "."): value for key, value in config.items()}

    def _parse_conf_response(self, conf_respose, key, value):
        if conf_respose == rdkafka.RD_KAFKA_CONF_OK:
            logger.debug(f"Correctly configured {key} with value {value}")
        elif conf_respose == rdkafka.RD_KAFKA_CONF_INVALID:
            err_str = f"Invalid {key} setting with value: {value}"
            logger.error(err_str)
            raise exceptions.InvalidSetting(err_str)
        elif conf_respose == rdkafka.RD_KAFKA_CONF_UNKNOWN:
            err_str = f"Unknown {value} setting with value {value}"
            logger.error(err_str)
            raise exceptions.UnknownSetting(err_str)

    cdef _init_consumer_group(self):
        # Callback called on partition assignment changes */
        rdkafka.rd_kafka_conf_set_rebalance_cb(self.conf, cb_rebalance)

    def _init_consumer(self):
        self.kafka_consumer = rdkafka.rd_kafka_new(
            rdkafka.RD_KAFKA_CONSUMER, self.conf, self.errstr, sizeof(self.errstr))
        if self.kafka_consumer == NULL:
            err_str = "Unexpected error creating kafka consumer"
            logger.error(err_str)
            raise exceptions.ConsumerError(err_str)
        logger.debug("Initialized kafka consumer")

        cdef char *brokers_ptr = self.brokers
        resp = rdkafka.rd_kafka_brokers_add(self.kafka_consumer, brokers_ptr)
        if resp == 0:
            err_str = f"Invalid kafka brokers: {self.brokers}"
            logger.error(err_str)
            raise exceptions.InvalidBrokers(err_str)
        logger.debug("Added brokers to kafka consumer")

        err_poll = rdkafka.rd_kafka_poll_set_consumer(self.kafka_consumer)
        if err_poll:
            err_str_poll = rdkafka.rd_kafka_err2str(err_poll)
            logger.error(err_str_poll)
            raise exceptions.ConsumerError(err_str_poll)

    def _init_topic(self):
        self.topic_list = rdkafka.rd_kafka_topic_partition_list_new(1)
        cdef int32_t partition = -1
        cdef char *topic_ptr = self.topic
        rdkafka.rd_kafka_topic_partition_list_add(
            self.topic_list, topic_ptr, partition)

    def _init_subscription(self):
        err = rdkafka.rd_kafka_subscribe(self.kafka_consumer, self.topic_list)
        if err:
            error_str = rdkafka.rd_kafka_err2str(err)
            logger.error(f"Error subscribing to topic: {error_str}")
            raise exceptions.SubscriptionError(error_str)
        logger.debug("Subscribed to topic ")

    cpdef _thread_consume_messages(self):
        cdef rdkafka.rd_kafka_message_t *rkmessage
        cdef rdkafka.rd_kafka_t *kafka_consumer
        logger.info(f"Opened consumer thread of topic {self.topic}")
        try:
            while not self.thread_stop_event.is_set():
                rkmessage = rdkafka.rd_kafka_consumer_poll(
                    self.kafka_consumer, 1000)
                if rkmessage:
                    self._thread_cb_msg_consume(rkmessage)
                    rdkafka.rd_kafka_message_destroy(rkmessage) # segmentation fault
                else:
                    logger.debug("thread consumer, poll timeout without messages")
            else:
                logger.info(f"Closing consumer thread of topic {self.topic}")
                return
        except Exception:
            logger.error(f"Unexpected exception in consumer thread of topic "
                         f"{self.topic}. Closing thread . ", exc_info=True)

    cdef _thread_cb_msg_consume(self, rdkafka.rd_kafka_message_t *rkmessage):
        if rkmessage.err:
            if rkmessage.err == rdkafka.RD_KAFKA_RESP_ERR__PARTITION_EOF:
                logger.debug("Partition EOF")
            elif rkmessage.rkt:
                err_message_str = rdkafka.rd_kafka_message_errstr(rkmessage)
                logger.error(
                    f"Consumer error in kafka topic {self.topic}, "
                    f"partition {rkmessage.partition}, "
                    f"offset {rkmessage.offset} "
                    f"error info: {err_message_str}"
                )
            else:
                err_str = rdkafka.rd_kafka_err2str(rkmessage.err)
                err_message_str = rdkafka.rd_kafka_message_errstr(rkmessage)
                logger.error(
                    f"Consumer error in kafka topic {self.topic}, "
                    f"error info: {err_str} {err_message_str}"
                )
        else:
            payload_ptr = <char*>rkmessage.payload
            payload_len = rkmessage.len
            logger.debug(
                f"Consumed message in thread of topic {self.topic} "
                f"with payload: {payload_ptr[:payload_len]}"
            )
            self._thread_open_asyncio_task(payload_ptr, payload_len)

    cdef _thread_open_asyncio_task(self, char *payload_ptr, size_t payload_len):
        while not self.thread_stop_event.is_set():
            try:
                self.thread_coroutine_semaphore.acquire(blocking=True, timeout=1)
            except TimeoutError:
                logger.warning(
                    "Maximum of coroutines reached, there aren't new slots in"
                    "the last second. "
                )
            else:
                coro = self.message_handler(payload_ptr[:payload_len])
                logger.debug("Opened new message_handler coroutine in the loop "
                             "of the main thread")
                asyncio.run_coroutine_threadsafe(coro, loop=self.loop)
                return

    def start(self) -> None:
        if not self.message_handler:
            raise exceptions.ConsumerError(
                "Message handler is needed before start the consumer")
        self._thread.start()

    def stop(self) -> None:
        self._rdkafka_consumer.thread_stop_event.set()
        try:
            self._thread.join(timeout=10)
        except TimeoutError:
            logger.error("Unexpected error closing consumer thread")
            raise
