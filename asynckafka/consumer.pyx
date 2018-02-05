import asyncio
import logging
import time
from libc.stdint cimport int32_t
from threading import Thread, Event

cimport rdkafka
from asynckafka import exceptions
from asynckafka import settings
from asynckafka.consumer_asynciter import ConsumerAsyncIterator

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
        string += f"\n" \
                  f"Topic: {topic}, " \
                  f"Partition: {partition}, " \
                  f"Offset: {offset}"
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


cdef class RdKafkaStructs:
    cdef rdkafka.rd_kafka_t *consumer
    cdef rdkafka.rd_kafka_conf_t *conf
    cdef rdkafka.rd_kafka_topic_conf_t *topic_conf
    cdef rdkafka.rd_kafka_topic_partition_list_t *topic_partition_list
    cdef char errstr[512]

    cdef bytes brokers
    cdef list topics
    cdef dict consumer_settings
    cdef dict topic_settings

    def __cinit__(self, brokers, group_id, consumer_settings,
                  topic_settings):
        self.topics = []
        self.brokers = brokers.encode()

        consumer_settings = consumer_settings if consumer_settings else {}
        if group_id:
            consumer_settings['group.id'] = group_id
        self.consumer_settings = self._parse_and_encode_settings(consumer_settings)

        topic_settings = topic_settings if topic_settings else {}
        if 'group.id' in consumer_settings:
            topic_settings["offset.store.method"] = "broker"
        self.topic_settings = self._parse_and_encode_settings(topic_settings)

    def _parse_settings(self, config: dict) -> dict:
        return {key.replace("_", "."): value for key, value in config.items()}

    def _encode_settings(self, settings: dict) -> dict:
        return {key.encode(): value.encode() for key, value in settings.items()}

    def _parse_and_encode_settings(self, settings: dict) -> dict:
        parsed_settings = self._parse_settings(settings)
        return self._encode_settings(parsed_settings)

    def start_rd_kafka_consumer(self):
        self._init_rd_kafka_configs()
        self._init_rd_kafka_consumer_group()
        self._init_rd_kafka_consumer()
        self._init_rd_kafka_topic_partition_lists()
        self._init_rd_kafka_subscription()

    def add_topic(self, topic):
        self.topics.append(topic)

    cpdef _init_rd_kafka_configs(self):
        self.conf = rdkafka.rd_kafka_conf_new()
        rdkafka.rd_kafka_conf_set_log_cb(self.conf, cb_logger)
        self.topic_conf = rdkafka.rd_kafka_topic_conf_new()
        for key, value in self.consumer_settings.items():
            conf_resp = rdkafka.rd_kafka_conf_set(
                self.conf, key, value, self.errstr, sizeof(self.errstr)
            )
            self._parse_rd_kafka_conf_response(conf_resp, key, value)
        for key, value in self.topic_settings.items():
            conf_resp = rdkafka.rd_kafka_topic_conf_set(
                self.topic_conf,
                key, value,
                self.errstr,
                sizeof(self.errstr)
            )
            self._parse_rd_kafka_conf_response(conf_resp, key, value)
        rdkafka.rd_kafka_conf_set_default_topic_conf(
            self.conf, self.topic_conf)

    @staticmethod
    def _parse_rd_kafka_conf_response(conf_respose, key, value):
        if conf_respose == rdkafka.RD_KAFKA_CONF_OK:
            logger.debug(f"Correctly configured rdkafka {key} with value "
                         f"{value}")
        elif conf_respose == rdkafka.RD_KAFKA_CONF_INVALID:
            err_str = f"Invalid {key} setting with value: {value}"
            logger.error(err_str)
            raise exceptions.InvalidSetting(err_str)
        elif conf_respose == rdkafka.RD_KAFKA_CONF_UNKNOWN:
            err_str = f"Unknown {value} setting with value {value}"
            logger.error(err_str)
            raise exceptions.UnknownSetting(err_str)

    cdef _init_rd_kafka_consumer_group(self):
        if b'group.id' in self.consumer_settings:
            rdkafka.rd_kafka_conf_set_rebalance_cb(
                self.conf,
                cb_rebalance
            )

    def _init_rd_kafka_consumer(self):
        self.consumer = rdkafka.rd_kafka_new(
            rdkafka.RD_KAFKA_CONSUMER,
            self.conf,
            self.errstr,
            sizeof(self.errstr)
        )
        if self.consumer == NULL:
            err_str = "Unexpected error creating kafka consumer"
            logger.error(err_str)
            raise exceptions.ConsumerError(err_str)
        logger.debug("Initialized kafka consumer")

        cdef char *brokers_ptr = self.brokers
        resp = rdkafka.rd_kafka_brokers_add(self.consumer, brokers_ptr)
        if resp == 0:
            err_str = f"Invalid kafka brokers: {self.brokers}"
            logger.error(err_str)
            raise exceptions.InvalidBrokers(err_str)
        logger.debug("Added brokers to kafka consumer")

        err_poll = rdkafka.rd_kafka_poll_set_consumer(self.consumer)
        if err_poll:
            err_str_poll = rdkafka.rd_kafka_err2str(err_poll)
            logger.error(err_str_poll)
            raise exceptions.ConsumerError(err_str_poll)

    def _init_rd_kafka_topic_partition_lists(self):
        cdef int32_t partition
        cdef char *topic_ptr
        self.topic_partition_list = \
            rdkafka.rd_kafka_topic_partition_list_new(len(self.topics))
        for topic in self.topics:
            partition = -1
            topic_ptr = topic
            rdkafka.rd_kafka_topic_partition_list_add(
                self.topic_partition_list, topic_ptr, partition)

    def _init_rd_kafka_subscription(self):
        err = rdkafka.rd_kafka_subscribe(
            self.consumer,
            self.topic_partition_list
        )
        if err:
            error_str = rdkafka.rd_kafka_err2str(err)
            logger.error(f"Error subscribing to topic: {error_str}")
            raise exceptions.SubscriptionError(error_str)
        logger.debug("Subscribed to topic ")


cdef class ConsumerThread:
    cdef RdKafkaStructs _rd_kafka

    cdef object thread
    cdef object stop_event

    cdef public list thread_communication_list
    cdef public unsigned long consumption_limiter

    cdef char _debug

    def __init__(self, RdKafkaStructs rd_kafka_struct, debug=False):
        self._rd_kafka = rd_kafka_struct
        self.thread = Thread(target=self._main())
        self.thread_communication_list = []
        self.stop_event = Event()
        self._debug = 1 if debug else 0

    cpdef _main(self):
        cdef rdkafka.rd_kafka_message_t *rkmessage
        cdef rdkafka.rd_kafka_t *kafka_consumer
        logger.info(f"Opened consumer thread.")
        try:
            while not self.stop_event.is_set():
                rkmessage = rdkafka.rd_kafka_consumer_poll(
                    self._rd_kafka.consumer, 100)
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

    cdef _cb_consume_message(
            self, rdkafka.rd_kafka_message_t *rk_message):
        if rk_message.err:
            if rk_message.err == rdkafka.RD_KAFKA_RESP_ERR__PARTITION_EOF:
                if self._debug: logger.debug("Partition EOF")
            elif rk_message.rkt:
                err_message_str = rdkafka.rd_kafka_message_errstr(rk_message)
                topic = rdkafka.rd_kafka_topic_name(rk_message.rkt)
                logger.error(
                    f"Consumer error in kafka topic {topic}, "
                    f"partition {rk_message.partition}, "
                    f"offset {rk_message.offset} "
                    f"error info: {err_message_str}"
                )
            else:
                err_str = rdkafka.rd_kafka_err2str(rk_message.err)
                err_message_str = rdkafka.rd_kafka_message_errstr(rk_message)
                logger.error(
                    f"Consumer error {err_str} {err_message_str}"
                )
        else:
            if self._debug:
                payload_ptr = <char*>rk_message.payload
                payload_len = rk_message.len
                topic = rdkafka.rd_kafka_topic_name(rk_message.rkt)
                logger.debug(
                    f"Consumed message in thread of topic {topic} "
                    f"with payload: {payload_ptr[:payload_len]}"
                )
            self._send_message_to_asyncio(rk_message)

    cdef _send_message_to_asyncio(self, rdkafka.rd_kafka_message_t *rkmessage):
        memory_address = <long> rkmessage
        while True:
            if self.consumption_limiter < settings.DEFAULT_MAX_COROUTINES:
                self.increase_consumption_limiter()
                self.thread_communication_list.append(memory_address)
                if self._debug: logger.debug(
                    "Sent memory address of message from consumer "
                    "thread to asyncio thread"
                )
                return
            else:
                if self.stop_event.is_set():
                    return
                time.sleep(0.1)

    cdef increase_consumption_limiter(self):
        self.consumption_limiter += 1

    cdef decrease_consumption_limiter(self):
        self.consumption_limiter -= 1

    def start(self):
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        self.thread.join(timeout=5)


cdef class Consumer:
    cdef RdKafkaStructs _rd_kafka
    cdef ConsumerThread _consumer_thread

    cdef object message_handlers
    cdef object loop
    cdef object _open_tasks_task
    cdef object _spawn_tasks
    cdef bint _debug

    def __cinit__(self, brokers, group_id=None, consumer_settings=None,
                  topic_settings=None, message_handlers=None, loop=None,
                  spawn_tasks=True, debug=False):
        self._rd_kafka = RdKafkaStructs(
            brokers=brokers, group_id=group_id, consumer_settings=consumer_settings,
            topic_settings=topic_settings
        )
        self._consumer_thread = ConsumerThread(self._rd_kafka, debug=debug)

        message_handlers = message_handlers if message_handlers else {}
        self.message_handlers = {
            key.encode(): value
            for key, value in  message_handlers.items()
        }
        self.loop = loop if loop else asyncio.get_event_loop()
        self._open_tasks_task = None
        self._debug = 1 if debug else 0
        self._spawn_tasks = spawn_tasks

    async def _read_from_consumer_thread(self):
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
                        await self._call_message_handler(message_memory_address)
            except Exception:
                logger.error(
                    "Unexpected exception consuming messages from thread",
                    exc_info=True
                )

    cdef _open_asyncio_task(self, long message_memory_address):
        rkmessage = <rdkafka.rd_kafka_message_t*> message_memory_address
        payload_ptr = <char*>rkmessage.payload
        payload_len = rkmessage.len
        topic = rdkafka.rd_kafka_topic_name(rkmessage.rkt)

        message_handler_coroutine = \
            self.message_handlers[topic](payload_ptr[:payload_len])
        message_handler_task = asyncio.ensure_future(
            message_handler_coroutine, loop=self.loop)
        message_handler_task.add_done_callback(self._cb_coroutine_counter_decrease)
        rdkafka.rd_kafka_message_destroy(rkmessage)

    def _cb_coroutine_counter_decrease(self, _):
        self._consumer_thread.decrease_consumption_limiter()

    async def _call_message_handler(self, long message_memory_address):
        rkmessage = <rdkafka.rd_kafka_message_t*> message_memory_address
        payload_ptr = <char*>rkmessage.payload
        payload_len = rkmessage.len
        topic = rdkafka.rd_kafka_topic_name(rkmessage.rkt)
        try:
            await self.message_handlers[topic](payload_ptr[:payload_len])
        except Exception:
            logger.error(f"Not captured exception in message handler "
                         f"of topic {str(topic)}", exc_info=True)
        finally:
            self._consumer_thread.decrease_consumption_limiter()
            rdkafka.rd_kafka_message_destroy(rkmessage)

    def start(self):
        logger.debug("Starting asynckafka consumer")
        if not self.message_handlers:
            raise exceptions.ConsumerError(
                "At least one message handler is needed before start the "
                "consumer"
            )
        self._rd_kafka.start_rd_kafka_consumer()
        self._consumer_thread.start()
        self._open_tasks_task = asyncio.ensure_future(
            self._read_from_consumer_thread(), loop=self.loop
        )

    def stop(self):
        self._consumer_thread.stop()
        # TODO wait for consume all messages
        self._open_tasks_task.cancel()

    def add_message_handler(self, topic, message_handler):
        logger.debug(f"Added message handler for topic {topic}")
        encoded_topic = topic.encode()
        self._rd_kafka.add_topic(encoded_topic)
        self.message_handlers[encoded_topic] = message_handler


cdef class AsyncIterConsumer:
    cdef RdKafkaStructs _rd_kafka
    cdef ConsumerThread _consumer_thread

    cdef object message_handlers
    cdef object loop
    cdef object _open_tasks_task
    cdef object _spawn_tasks
    cdef bint _debug

    cdef object async_iterator

    def __cinit__(self, brokers, topic, group_id=None, consumer_settings=None,
                  topic_settings=None, loop=None, debug=False):
        self._rd_kafka = RdKafkaStructs(
            brokers=brokers, group_id=group_id, consumer_settings=consumer_settings,
            topic_settings=topic_settings
        )
        self._rd_kafka.add_topic(topic)
        self._consumer_thread = ConsumerThread(self._rd_kafka, debug=debug)
        self.loop = loop if loop else asyncio.get_event_loop()
        self.async_iterator = ConsumerAsyncIterator(
            self._consumer_thread.thread_communication_list,
            self.get_message,
            self.destroy_message,
            self.loop
        )
        self._debug = 1 if debug else 0

    def start(self):
        self._rd_kafka.start_rd_kafka_consumer()
        self._consumer_thread.start()

    def stop(self):
        self._rd_kafka.start_rd_kafka_consumer()
        self._consumer_thread.stop()
        self.async_iterator.stop()

    def __aiter__(self):
        return self.async_iterator

    cpdef bytes get_message(self, long message_memory_address):
        rkmessage = <rdkafka.rd_kafka_message_t*> message_memory_address
        payload_ptr = <char*>rkmessage.payload
        payload_len = rkmessage.len
        return payload_ptr[:payload_len]

    cpdef destroy_message(self, long message_memory_address):
        rkmessage = <rdkafka.rd_kafka_message_t*> message_memory_address
        rdkafka.rd_kafka_message_destroy(rkmessage)
        self._consumer_thread.decrease_consumption_limiter()

