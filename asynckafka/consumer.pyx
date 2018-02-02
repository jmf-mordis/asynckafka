from typing import Callable, Coroutine

cimport rdkafka
import asyncio

import functools
from libc cimport stdio

from libc.stdint cimport int32_t, int64_t
from threading import Thread, Event, Semaphore

from asynckafka.exceptions import InvalidSetting, UnknownSetting

from asynckafka import settings

cdef int wait_eof = 0


# Kafka logger callback (optional)
cdef void cb_logger(const rdkafka.rd_kafka_t *rk, int level, const char *fac, const char *buf):
    print("logger callback. TODO: call to python logging")


cdef void cb_rebalance(
        rdkafka.rd_kafka_t *rk, rdkafka.rd_kafka_resp_err_t err,
        rdkafka.rd_kafka_topic_partition_list_t *partitions, void *opaque):
    print("%% Consumer group rebalanced: ")
    if err == rdkafka.RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
        print("Partition assigned: ")
        rdkafka.rd_kafka_assign(rk, partitions)
    elif err == rdkafka.RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
        print("Partition revoked: ")
        rdkafka.rd_kafka_assign(rk, NULL)
    else:
        print("Error: ", rdkafka.rd_kafka_err2str(err))
        rdkafka.rd_kafka_assign(rk, NULL)


class Consumer:

    def __init__(self, brokers, topic, message_handler=None, error_callback=None,
                 loop=None, consumer_settings=None, topic_settings=None):
        self._consumer_settings = self._parse_settings(consumer_settings) \
            if consumer_settings else {}
        self._topic_settings = self._parse_settings(topic_settings) \
            if topic_settings else {}

        self._thread_coroutine_manager = Semaphore(
            value=settings.DEFAULT_MAX_COROUTINES)

        @functools.wraps(message_handler)
        async def wrapper_message_handler(payload):
            try:
                await message_handler(payload)
            finally:
                self._thread_coroutine_manager.release()

        self.loop = loop if loop else asyncio.get_event_loop()
        self.message_handler = message_handler if message_handler else None
        self._thread = None

        self._rdkafka_consumer = RdkafkaConsumer(
            brokers=brokers, topic=topic, consumer_settings=self._consumer_settings,
            topic_settings=self._topic_settings, message_handler=wrapper_message_handler,
            loop=loop, coroutine_manager= self._thread_coroutine_manager
        )

    @staticmethod
    def _parse_settings(config: dict) -> dict:
        return {key.replace("_", "."): value for key, value in config.items()}

    def start(self) -> None:
        if not self.message_handler:
            raise Exception("Message handler is needed before the start")
        self._open_thread()

    def stop(self) -> None:
        self._thread_stop_event.set()
        try:
            self._thread.join(timeout=10)
        except TimeoutError:
            print("Error closing thread")
            raise

    def _open_thread(self) -> None:

        self._thread = Thread(
            target=self._rdkafka_consumer.thread_consume_messages,
        )
        self._thread.start()


cdef class RdkafkaConsumer:

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

    cdef tuple message_handler
    cdef tuple loop
    cdef tuple coroutine_manager
    cdef tuple stop_event

    def __cinit__(self, brokers, topic, consumer_settings, topic_settings,
                  message_handler, loop, coroutine_manager):
        self.brokers = brokers.encode()
        self.topic = topic.encode()
        self.consumer_settings = {
            key.encode(): value.encode()
            for key, value in consumer_settings.items()
        }
        if 'group.id' in consumer_settings:
            topic_settings["offset.store.method"] = "broker"
        self.topic_settings = {
            key.encode(): value.encode()
            for key, value in topic_settings.items()
        }

        self.message_handler = tuple([message_handler])
        self.loop = tuple([loop])
        self.thread_coroutine_semaphore = tuple([coroutine_manager])
        self.thread_stop_event = tuple([Event()])
        self.thread_stopped_event = tuple([Event()])

        self._init_config()
        self._consumer_settings_to_rdkafka()
        self._init_consumer_group()
        self._init_consumer()
        self._init_topic()
        self._init_subscription()

    cpdef _init_config(self):
        self.conf = rdkafka.rd_kafka_conf_new()
        rdkafka.rd_kafka_conf_set_log_cb(self.conf, cb_logger)
        #/* Topic configuration */
        self.topic_conf = rdkafka.rd_kafka_topic_conf_new()

    cpdef _consumer_settings_to_rdkafka(self):
        for key, value in self.consumer_settings.items():
            conf_resp = rdkafka.rd_kafka_conf_set(
                self.conf, key, value, self.errstr, sizeof(self.errstr)
            )
            self.parse_conf_response(conf_resp, key, value)

    cdef parse_conf_response(self, conf_respose, key, value):
        if conf_respose == rdkafka.RD_KAFKA_CONF_OK:
            print(f"{key} correcly configured with {value}")
        elif conf_respose == rdkafka.RD_KAFKA_CONF_INVALID:
            raise InvalidSetting(f"Invalid {key} setting. Value: {value}")
        elif conf_respose == rdkafka.RD_KAFKA_CONF_UNKNOWN:
            raise UnknownSetting(f"Unknown {value} setting. Value {value}")

    cdef _init_consumer_group(self):
        for key, value in self.topic_settings.items():
            conf_resp = rdkafka.rd_kafka_topic_conf_set(
                self.topic_conf, key, value, self.errstr, sizeof(self.errstr)
            )
            self.parse_conf_response(conf_resp, key, value)
        # Set default topic config for pattern-matched topics. */
        rdkafka.rd_kafka_conf_set_default_topic_conf(self.conf, self.topic_conf)
        # Callback called on partition assignment changes */
        rdkafka.rd_kafka_conf_set_rebalance_cb(self.conf, cb_rebalance)

    def _init_consumer(self):
        self.kafka_consumer = rdkafka.rd_kafka_new(
            rdkafka.RD_KAFKA_CONSUMER, self.conf, self.errstr, sizeof(self.errstr))
        if self.kafka_consumer == NULL:
            print("null kafka consumer pointer")
            exit(1)     # TODO launch exception
        print("Initialized kafka consumer")

        cdef char *brokers_ptr = self.brokers
        resp = rdkafka.rd_kafka_brokers_add(self.kafka_consumer, brokers_ptr)
        if resp == 0:
            print("No valid brokers")
            # TODO launch exception
            exit(1)     # TODO launch exception
        print("Added brokers")

        err_poll = rdkafka.rd_kafka_poll_set_consumer(self.kafka_consumer)
        if err_poll:
            print(rdkafka.rd_kafka_err2str(err_poll))
            exit(1)     # TODO launch exception

    def _init_topic(self):
        print("Initializing topics")
        self.topic_list = rdkafka.rd_kafka_topic_partition_list_new(1)
        cdef int32_t partition = -1
        cdef char *topic_ptr = self.topic
        rdkafka.rd_kafka_topic_partition_list_add(
            self.topic_list, topic_ptr, partition)

    def _init_subscription(self):
        print("Initializing subscription")
        err = rdkafka.rd_kafka_subscribe(self.kafka_consumer, self.topic_list)
        if err:
            print(rdkafka.rd_kafka_err2str(err))
            exit(1)     # TODO launch exception

    cpdef thread_consume_messages(self):
        cdef rdkafka.rd_kafka_message_t *rkmessage
        cdef rdkafka.rd_kafka_t *kafka_consumer
        print("In consumer thread")
        while not self.thread_stop_event[0].is_set():
            rkmessage = rdkafka.rd_kafka_consumer_poll(self.kafka_consumer, 1000)
            print("timeout of consumer poll")
            if rkmessage:
                self.thread_cb_msg_consume(rkmessage)
                # rdkafka.rd_kafka_message_destroy(rkmessage) segmentation fault
        else:
            print("closing thread")

    cdef thread_cb_msg_consume(self, rdkafka.rd_kafka_message_t *rkmessage):
        cdef char *payload
        stdio.printf("consumed message in thread")
        if rkmessage.err:
            if rkmessage.err == rdkafka.RD_KAFKA_RESP_ERR__PARTITION_EOF:
                print("Partition EOF")
            elif rkmessage.rkt:
                print("Consume error for topic blabla")
            else:
                print("Consuming error")
                print(rdkafka.rd_kafka_err2str(rkmessage.err))
                print(rdkafka.rd_kafka_message_errstr(rkmessage))
        else:
            payload = <char*>rkmessage.payload
            topic = rdkafka.rd_kafka_topic_name(rkmessage.rkt)
            print("Message payload: ", payload)
            print("Topic: ", topic)
            self.thread_open_asyncio_task(payload)


    cdef thread_open_asyncio_task(self, payload):
        while not self.thread_stop_event[0].is_set():
            try:
                self.thread_coroutine_semaphore[0].acquire(blocking=True, timeout=1)
            except TimeoutError:
                print("Coro semaphore is full.")
            else:
                coro = self.message_handler[0](payload)
                asyncio.run_coroutine_threadsafe(coro, loop=self.loop[0])
                return

