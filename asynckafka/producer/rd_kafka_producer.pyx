import logging

from asynckafka import utils, exceptions
from asynckafka.callbacks cimport cb_logger, cb_error


logger = logging.getLogger("asynckafka")


cdef class KafkaTopic:
    pass


cdef class RdKafkaProducer:

    def __init__(self, brokers, producer_config=None, topic_config=None):
        self.brokers = brokers.encode()
        self.topics = {}

        producer_config = producer_config if producer_config else {}
        producer_config['bootstrap.servers'] = brokers
        self.producer_config = utils.parse_and_encode_settings(
            producer_config
        )
        topic_config = topic_config if topic_config else {}
        self.topic_config = utils.parse_and_encode_settings(
            topic_config
        )

    def _init_configs(self):
        self.conf = crdk.rd_kafka_conf_new()
        self.topic_conf = crdk.rd_kafka_topic_conf_new()
        crdk.rd_kafka_conf_set_log_cb(self.conf, cb_logger)
        crdk.rd_kafka_conf_set_error_cb(self.conf, cb_error)
        for key, value in self.producer_config.items():
            conf_resp = crdk.rd_kafka_conf_set(
                self.conf,
                key, value,
                self.errstr,
                sizeof(self.errstr)
            )
            utils.parse_rd_kafka_conf_response(conf_resp, key, value)
        for key, value in self.topic_config.items():
            conf_resp = crdk.rd_kafka_topic_conf_set(
                self.topic_conf,
                key, value,
                self.errstr,
                sizeof(self.errstr)
            )
            utils.parse_rd_kafka_conf_response(conf_resp, key, value)

    def _init_producer(self):
        self.producer = crdk.rd_kafka_new(
            crdk.RD_KAFKA_PRODUCER,
            self.conf,
            self.errstr,
            sizeof(self.errstr)
        )
        if not self.producer:
            err_str = "Failed creating a rd kafka producer"
            logger.error(err_str)
            raise exceptions.ProducerError(err_str)
        logger.info("Created producer")

    cdef inline KafkaTopic _rd_kafka_topic_factory(self, name):
        cdef KafkaTopic kafka_topic
        cdef crdk.rd_kafka_topic_t *rdk_topic
        kafka_topic = KafkaTopic()
        kafka_topic.name = name.encode()
        rdk_topic = crdk.rd_kafka_topic_new(
            self.producer,
            kafka_topic.name,
            self.topic_conf
        )
        if not rdk_topic:
            err_str = "Failed to create topic object"
            logger.error(err_str)
            raise exceptions.ProducerError(err_str)
        kafka_topic.rdk_topic_memory_address = <long> rdk_topic
        return kafka_topic

    def start(self):
        self._init_configs()
        self._init_producer()

    def stop(self, timeout):
        cdef crdk.rd_kafka_topic_t *rdk_topic
        logger.info("Waiting to deliver all the remaining messages")
        err = crdk.rd_kafka_flush(self.producer, timeout * 1000)
        if err != crdk.RD_KAFKA_RESP_ERR_NO_ERROR:
            err_str = bytes(crdk.rd_kafka_err2str(err)).decode()
            logger.error(f"Timeout occurred waiting to deliver all the "
                         f"remaining messages")
        for topic in self.topics.values():
            logger.info(f"Destroying rd kafka topic {topic.name}")
            rdk_topic = self._get_rdk_topic(topic)
            crdk.rd_kafka_topic_destroy(rdk_topic)
        logger.info("Destroying rd kafka producer")
        crdk.rd_kafka_destroy(self.producer)
        logger.info("Rd kafka producer correctly destroyed")

    cdef inline KafkaTopic add_topic(self, topic_name):
        kafka_topic = self._rd_kafka_topic_factory(topic_name)
        self.topics[topic_name] = kafka_topic
        return kafka_topic

    cdef inline crdk.rd_kafka_topic_t *_get_rdk_topic(self, kafka_topic):
        cdef long topic_memory_address = kafka_topic.rdk_topic_memory_address
        rdk_topic = <crdk.rd_kafka_topic_t*> topic_memory_address
        return rdk_topic

    cdef inline crdk.rd_kafka_topic_t *get_rdk_topic(self, topic_name):
        cdef crdk.rd_kafka_topic_t *rdk_topic
        try:
            kafka_topic = self.topics[topic_name]
        except KeyError:
            kafka_topic = self.add_topic(topic_name)
        rdk_topic = self._get_rdk_topic(kafka_topic)
        return rdk_topic

    def get_name(self):
        return bytes(crdk.rd_kafka_name(self.producer)).decode()
