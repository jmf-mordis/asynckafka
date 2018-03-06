import logging

import asyncio

from asynckafka import exceptions
from asynckafka.callbacks import register_error_callback, \
    unregister_error_callback
from asynckafka.includes cimport c_rd_kafka as crdk
from asynckafka.settings cimport debug
from asynckafka.settings import PRODUCER_RD_KAFKA_POLL_PERIOD_SECONDS
from asynckafka import utils


logger = logging.getLogger("asynckafka")


cdef class Producer:
    """
    TODO DOC

    Args:
        brokers (str): Brokers separated with ",", example:
            "192.168.1.1:9092,192.168.1.2:9092".
        rdk_producer_config (dict): Rdkafka producer settings.
        rdk_topic_config (dict): Rdkafka topic settings.
        error_callback (Coroutine[asyncio.exceptions.KafkaError]): Coroutine
            with one argument (KafkaError). It is scheduled in the loop when
            there is an error, for example, if the broker is down.
        loop (asyncio.AbstractEventLoop): Asyncio event loop.
    """
    def __init__(self, brokers=None, rdk_producer_config=None,
                 rdk_topic_config=None, error_callback=None, loop=None):
        brokers = brokers if brokers else "127.0.0.1:9092"
        self.rdk_producer = RdKafkaProducer(
            brokers=brokers, producer_config=rdk_producer_config,
            topic_config=rdk_topic_config
        )

        self.periodic_poll_task = None
        self.producer_state = producer_states.STOPPED
        self.error_callback = error_callback
        self.loop = loop if loop else asyncio.get_event_loop()

    async def produce(self, topic, message, key=None):
        """
        Produce one message to a certain topic. The producer should be running.
        The message will be copied to internal queues and it will
        be sent in batches before the end of this coroutine, so this
        function does not guarantee the delivery of the message.

        Args:
            topic (str):
            message (bytes):
            key (bytes):
        """
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
                        PRODUCER_RD_KAFKA_POLL_PERIOD_SECONDS,
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
        """
        Start the producer. It is necessary call this method before start to
        produce messages.

        Raises:
            asynckafka.exceptions.ProducerError: Error in the initialization of
                the producer client.
            asynckafka.exceptions.InvalidSetting: Invalid setting in
                producer_settings or topic_settings.
            asynckafka.exceptions.UnknownSetting: Unknown setting in
                producer_settings or topic_settings.
        """
        if self.producer_state == producer_states.STOPPED:
            self.rdk_producer.start()
            self.periodic_poll_task = asyncio.ensure_future(
                utils.periodic_rd_kafka_poll(
                    <long> self.rdk_producer.producer, self.loop),
                loop=self.loop
            )
            if self.error_callback:
                register_error_callback(
                    self, self.rdk_producer.get_name())
            self.producer_state = producer_states.STARTED
        else:
            error_str = "Tried to start a producer already started"
            logger.error(error_str)
            raise exceptions.ProducerError(error_str)

    def stop(self, timeout=10):
        """
        Stop the producer. Tt is advisable to call this method before
        closing the python interpreter. Once the producer is stopped, all
        calls to produce should raise a ProducerError.

        Args:
            timeout (float, int): Maximum time available time to flush the
                messages.

        Raises:
            asynckafka.exceptions.ProducerError : Error in the shut down of
                the producer client.
            asynckafka.exceptions.InvalidSetting: Invalid setting in
                consumer_settings or topic_settings.
            asynckafka.exceptions.UnknownSetting: Unknown setting in
                consumer_settings or topic_settings.
        """
        logger.info("Called producer stop")
        if self.producer_state == producer_states.STARTED:
            self.rdk_producer.stop(timeout)
            if self.error_callback:
                unregister_error_callback(self.rdk_producer.get_name())
            logger.info("Canceling asyncio poll task")
            self.periodic_poll_task.cancel()
            logger.info("Producer stopped")
            self.producer_state = producer_states.STOPPED
        else:
            error_str = "Tried to stop a producer that is already stopped"
            logger.error(error_str)
            raise exceptions.ProducerError(error_str)

    def is_started(self):
        return self.producer_state == producer_states.STARTED
