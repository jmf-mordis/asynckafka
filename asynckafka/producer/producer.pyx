import logging

import asyncio

from asynckafka import exceptions
from asynckafka.callbacks cimport cb_logger, cb_error
from asynckafka.includes cimport c_rd_kafka as crdk
from asynckafka import utils


logger = logging.getLogger("asynckafka")


cdef class Producer:
    def __init__(self, brokers: str, topic: str, producer_settings=None,
                  debug=False, loop=None):
        self.brokers = brokers.encode()
        self.topic = topic.encode()

        producer_settings = producer_settings if producer_settings else {}
        producer_settings['bootstrap.servers'] = brokers
        self.producer_settings = utils.parse_and_encode_settings(
            producer_settings
        )

        self.set_debug(debug)
        self.producer_state = producer_states.STOPPED
        self.loop = loop if loop else asyncio.get_event_loop()

    def _init_rd_kafka_configs(self):
        self._rd_kafka_conf = crdk.rd_kafka_conf_new()
        crdk.rd_kafka_conf_set_log_cb(self._rd_kafka_conf, cb_logger)
        crdk.rd_kafka_conf_set_error_cb(self._rd_kafka_conf, cb_error)
        for key, value in self.producer_settings.items():
            conf_resp = crdk.rd_kafka_conf_set(
                self._rd_kafka_conf,
                key, value,
                self.errstr,
                sizeof(self.errstr)
            )
            utils.parse_rd_kafka_conf_response(conf_resp, key, value)

    def _init_producer(self):
        self._rd_kafka_producer = crdk.rd_kafka_new(
            crdk.RD_KAFKA_PRODUCER,
            self._rd_kafka_conf,
            self.errstr,
            sizeof(self.errstr)
        )
        if not self._rd_kafka_producer:
            err_str = "Failed creating a rd kafka producer"
            logger.error(err_str)
            raise exceptions.ProducerError(err_str)
        logger.info("Created producer")

    def _init_topic(self):
        self._rd_kafka_topic = crdk.rd_kafka_topic_new(self._rd_kafka_producer,
                                               self.topic, NULL)
        if not self._rd_kafka_topic:
            err_str = "Failed to create topic object"
            logger.error(err_str)
            logger.info("Destroying kafka producer")
            crdk.rd_kafka_destroy(self._rd_kafka_producer)
            raise exceptions.ProducerError(err_str)
        logger.info("Created kafka topic")

    async def _periodic_rd_kafka_poll(self):
        while True:
            await asyncio.sleep(1, loop=self.loop)
            crdk.rd_kafka_poll(self._rd_kafka_producer, 0)

    async def produce(self, message: bytes, key=None):
        cdef char *message_ptr = message
        cdef char *key_ptr
        if key:
            key_ptr = key
        else:
            key_ptr = NULL
        while self.producer_state == producer_states.STARTED:
            resp = crdk.rd_kafka_produce(
                self._rd_kafka_topic,
                crdk._RD_KAFKA_PARTITION_UA, crdk._RD_KAFKA_MSG_F_COPY,
                message_ptr, len(message),
                key_ptr, len(key) if key else 0,
                NULL
            )
            if resp == -1:
                error = crdk.rd_kafka_last_error()
                if self._debug:
                    logger.debug(crdk.rd_kafka_err2str(error).decode())
                if error == crdk.RD_KAFKA_RESP_ERR__QUEUE_FULL:
                    if self._debug:
                        logger.debug(
                            "Rd kafka production queue is full "
                            "waiting 0.1 seconds to retry"
                        )
                    await asyncio.sleep(0.1)
                else:
                    err_str = "Error producing message"
                    logger.error(err_str)
                    raise exceptions.ProducerError(err_str)
            else:
                if self._debug: logger.debug("Sent message")
                return
        else:
            err_str = "Produce called in a stopped producer"
            logger.warning(err_str)
            raise exceptions.ProducerError(err_str)

    def start(self):
        if self.producer_state == producer_states.STOPPED:
            self._init_rd_kafka_configs()
            self._init_producer()
            self._init_topic()
            self._periodic_poll_task = asyncio.ensure_future(
                self._periodic_rd_kafka_poll(),
                loop=self.loop
            )
            self.producer_state = producer_states.STARTED
        else:
            error_str = "Tried to start a producer already started"
            logger.error(error_str)
            raise exceptions.ProducerError(error_str)

    def stop(self, timeout=10):
        logger.info("Called producer stop")
        if self.producer_state == producer_states.STARTED:
            self.producer_state = producer_states.STOPPED
            logger.info("Waiting to deliver all the remaining messages")
            err = crdk.rd_kafka_flush(self._rd_kafka_producer, timeout * 1000)
            if err != crdk.RD_KAFKA_RESP_ERR_NO_ERROR:
                err_str = bytes(crdk.rd_kafka_err2str(err)).decode()
                logger.error(f"Timeout occurred waiting to deliver all the "
                             f"remaining messages")
            logger.info("Destroying rd kafka structures")
            crdk.rd_kafka_topic_destroy(self._rd_kafka_topic)
            crdk.rd_kafka_destroy(self._rd_kafka_producer)
            logger.info("Canceling asyncio poll task")
            self._periodic_poll_task.cancel()
            logger.info("Producer stopped")
        else:
            error_str = "Tried to stop a producer that is already stopped"
            logger.error(error_str)
            raise exceptions.ProducerError(error_str)

    def is_started(self):
        return self.producer_state == producer_states.STARTED

    def set_debug(self, debug: bool):
        self._debug = 1 if debug else 0

    def is_in_debug(self):
        return self._debug == 1
