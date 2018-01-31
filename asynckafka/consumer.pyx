from typing import Callable, Coroutine

cimport rdkafka
import asyncio
from libc cimport stdio

from libc.stdint cimport int32_t, int64_t
from threading import Thread, Event
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

    required_init_kwargs = {'topic'}

    def __init__(self, topic, message_handler=None, error_callback=None,
                 loop=None, **kwargs):
        self._config = self._config_replace_low_bars_with_points(kwargs)
        self._config['topic'] = topic
        self._rdkafka_consumer = RdkafkaConsumer(**kwargs)
        self.loop = loop if loop else asyncio.get_event_loop()
        self.message_handler = message_handler if message_handler else None
        self._thread = None
        self._thread_stop_event = Event()

    @staticmethod
    def _config_replace_low_bars_with_points(config: dict) -> dict:
        return {key.replace("_", "."): value for key, value in config.items()}

    def start(self) -> None:
        if not self.message_handler:
            raise Exception("Message handler is needed before the start")
        self._open_thread()

    def close(self) -> None:
        self._thread_stop_event.set()

    def _open_thread(self) -> None:
        self._thread = Thread(
            target=self.cython_consumer.consume_messages_in_thread,
            args=(self.message_handler, self.loop)
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
    cdef bytes group_id

    def __cinit__(self, brokers, topic, group_id=None):
        self.brokers = brokers.encode()
        self.topic = topic.encode()
        self.group_id = group_id.encode() if group_id else None
        self._init_config()
        self._init_consumer_group()
        self._init_consumer()
        self._init_topic()
        self._init_subscription()

    cpdef _init_config(self):
        self.conf = rdkafka.rd_kafka_conf_new()
        rdkafka.rd_kafka_conf_set_log_cb(self.conf, cb_logger)
        #/* Topic configuration */
        self.topic_conf = rdkafka.rd_kafka_topic_conf_new()

    cdef _init_consumer_group(self):
        cdef char *group_id = self.group_id
        conf_resp = rdkafka.rd_kafka_conf_set(
            self.conf, "group.id", group_id, self.errstr, sizeof(self.errstr)
        )
        if conf_resp != rdkafka.RD_KAFKA_CONF_OK:
            print("Wrong response in consumer group")
            exit(1)     # TODO launch exception
        topic_conf_resp = rdkafka.rd_kafka_topic_conf_set(
            self.topic_conf, "offset.store.method", "broker", self.errstr,
            sizeof(self.errstr)
        )
        if topic_conf_resp != rdkafka.RD_KAFKA_CONF_OK:
            print("Wrong response in consumer group")
            exit(1)     # TODO launch exception

        # Set default topic config for pattern-matched topics. */
        rdkafka.rd_kafka_conf_set_default_topic_conf(self.conf, self.topic_conf)

        # Callback called on partition assignment changes */
        rdkafka.rd_kafka_conf_set_rebalance_cb(self.conf, cb_rebalance)

    def _init_consumer(self):
        self.kafka_consumer = rdkafka.rd_kafka_new(rdkafka.RD_KAFKA_CONSUMER, self.conf, self.errstr, sizeof(self.errstr))
        if self.kafka_consumer == NULL:
            print("null kafka consumer pointer")
            exit(1)     # TODO launch exception
        print("Initialized kafka consumer")

        cdef char *brokers = self.brokers
        resp = rdkafka.rd_kafka_brokers_add(self.kafka_consumer, brokers)
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
        cdef char *topic = self.topic
        rdkafka.rd_kafka_topic_partition_list_add(self.topic_list, topic, partition)

    def _init_subscription(self):
        print("Initializing subscription")
        err = rdkafka.rd_kafka_subscribe(self.kafka_consumer, self.topic_list)
        if err:
            print(rdkafka.rd_kafka_err2str(err))
            exit(1)     # TODO launch exception

    cpdef consume_messages_in_thread(self, message_handler, loop):
        cdef rdkafka.rd_kafka_message_t *rkmessage
        cdef rdkafka.rd_kafka_t *kafka_consumer
        stdio.printf("in main thread\n")
        while True:
            rkmessage = rdkafka.rd_kafka_consumer_poll(self.kafka_consumer, 1000)
            stdio.printf("asdfdsfs\n")
            if rkmessage:
                self.cb_msg_consume(rkmessage, message_handler, loop)
                # rdkafka.rd_kafka_message_destroy(rkmessage) segmentation fault

    cdef void cb_msg_consume(self, rdkafka.rd_kafka_message_t *rkmessage, message_handler,  loop):
        cdef char *payload
        stdio.printf("consumed message in cthread")
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
            print("Message payload: ", payload)
            coro = message_handler(payload)
            asyncio.run_coroutine_threadsafe(coro, loop=loop)

