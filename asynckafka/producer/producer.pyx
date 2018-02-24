import logging

import asyncio

from asynckafka import exceptions
from asynckafka.callbacks cimport cb_logger, cb_error
from asynckafka.includes cimport c_rd_kafka as crdk
from asynckafka.settings cimport debug
from asynckafka.settings import PRODUCER_RD_KAFKA_POLL_PERIOD_MILLISECONDS
from asynckafka import utils


logger = logging.getLogger("asynckafka")



cdef class Producer:

    def __init__(self, brokers, producer_settings=None, topic_settings=None,
                 loop=None):
        self.rdk_producer = RdKafkaProducer(
            brokers=brokers, producer_settings=producer_settings,
            topic_settings=topic_settings
        )

        self.loop = loop if loop else asyncio.get_event_loop()
        self.periodic_poll_task = None
        self.producer_state = producer_states.STOPPED

    async def produce(self, topic, message, key=None):
        cdef crdk.rd_kafka_topic_t *rdk_topic
        cdef char *message_ptr = message
        cdef char *key_ptr
        if key:
            key_ptr = key
        else:
            key_ptr = NULL
        while self.producer_state == producer_states.STARTED:
            rdk_topic = self.rdk_producer.get_rdk_topic(topic)
            resp = crdk.rd_kafka_produce(
                rdk_topic,
                crdk._RD_KAFKA_PARTITION_UA, crdk._RD_KAFKA_MSG_F_COPY,
                message_ptr, len(message),
                key_ptr, len(key) if key else 0,
                NULL
            )
            if resp == -1:
                error = crdk.rd_kafka_last_error()
                if debug: logger.debug(crdk.rd_kafka_err2str(error).decode())
                if error == crdk.RD_KAFKA_RESP_ERR__QUEUE_FULL:
                    if debug: logger.debug(
                        "Rd kafka production queue is full "
                        "waiting 0.1 seconds to retry"
                    )
                    await asyncio.sleep(
                        PRODUCER_RD_KAFKA_POLL_PERIOD_MILLISECONDS,
                        loop=self.loop
                    )
                else:
                    # TODO read ERROR !!
                    err_str = "Error producing message"
                    logger.error(err_str)
                    raise exceptions.ProducerError(err_str)
            else:
                if debug: logger.debug("Sent message")
                return
        else:
            err_str = "producer.produce called in a stopped producer"
            logger.warning(err_str)
            raise exceptions.ProducerError(err_str)

    def start(self):
        if self.producer_state == producer_states.STOPPED:
            self.rdk_producer.start()
            self.periodic_poll_task = asyncio.ensure_future(
                utils.periodic_rd_kafka_poll(
                    <long> self.rdk_producer.producer, self.loop),
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
            self.rdk_producer.stop(timeout)
            logger.info("Canceling asyncio poll task")
            self.periodic_poll_task.cancel()
            logger.info("Producer stopped")
        else:
            error_str = "Tried to stop a producer that is already stopped"
            logger.error(error_str)
            raise exceptions.ProducerError(error_str)

    def is_started(self):
        return self.producer_state == producer_states.STARTED
