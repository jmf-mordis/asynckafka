import logging
from libc.stdint cimport int32_t

from asynckafka.includes cimport c_rd_kafka as crdk
from asynckafka.callbacks cimport cb_rebalance, cb_error, cb_logger
from asynckafka import exceptions
from asynckafka import utils
from asynckafka.consumer.topic_partition cimport current_partition_assignment

logger = logging.getLogger('asynckafka')


cdef class RdKafkaConsumer:

    def __init__(self, brokers: str, consumer_config: dict,
                 topic_config: dict, group_id=None):
        self.topics = []
        self.brokers = brokers.encode() if brokers else b"127.0.0.1:9092"

        consumer_config = consumer_config if consumer_config else {}
        consumer_config['group.id'] = group_id if group_id else \
            "default_consumer_group"
        self.consumer_config = utils.parse_and_encode_settings(
            consumer_config)

        topic_config = topic_config if topic_config else {}
        if 'group.id' in consumer_config:
            topic_config["offset.store.method"] = "broker"
        self.topic_config = utils.parse_and_encode_settings(topic_config)

        self.status = consumer_states.NOT_STARTED

    def start(self):
        if not self.topics:
            raise exceptions.ConsumerError(
                "Starting rd kafka consumer without topics"
            )
        self._init_rd_kafka_configs()
        self._init_rd_kafka_consumer_group()
        self._init_rd_kafka_consumer()
        self._init_rd_kafka_topic_partition_lists()
        self._init_rd_kafka_subscription()
        logger.info("RdKafkaConsumer correctly initialized")
        self.status = consumer_states.STARTED

    def stop(self):
        logger.info('Triggered the stop of the rdkafka consumer')
        logger.info('Destroying the topic partition list')
        crdk.rd_kafka_topic_partition_list_destroy(
            self.topic_partition_list)
        logger.info('Closing rdkafka consumer')
        err_code = crdk.rd_kafka_consumer_close(self.consumer)
        if err_code:
            err_str = str(crdk.rd_kafka_err2str(err_code))
            logger.error(f"Error closing rdkafka consumer: {err_str}")
            raise exceptions.ConsumerError(err_str)
        logger.info('Destroying rdkafka consumer')
        crdk.rd_kafka_destroy(self.consumer)
        logger.info('Rdkafka consumer destroyed correctly')
        self.status = consumer_states.STOPPED

    def add_topic(self, topic: str):
        self.topics.append(topic.encode())

    def seek(self, topic_partition, timeout):
      cdef crdk.rd_kafka_topic_t *rdk_topic
      rdk_topic = crdk.rd_kafka_topic_new(
          self.consumer,
          topic_partition.topic,
          NULL
      )
      if not rdk_topic:
          err_str = "Failed to create topic object"
          logger.error(err_str)
          print(err_str)
          raise exceptions.ConsumerError(err_str)
      err = crdk.rd_kafka_seek(rdk_topic, topic_partition.partition, topic_partition.offset, timeout)
      if err != crdk.RD_KAFKA_RESP_ERR_NO_ERROR:
          err_str = bytes(crdk.rd_kafka_err2str(err)).decode()
          logger.error("Timeout occurred while making seek ")
          raise exceptions.ConsumerError(err_str)
      logger.info('Seek done.')

    def _init_rd_kafka_configs(self):
        self.conf = crdk.rd_kafka_conf_new()
        crdk.rd_kafka_conf_set_log_cb(self.conf, cb_logger)
        crdk.rd_kafka_conf_set_error_cb(self.conf, cb_error)
        self.topic_conf = crdk.rd_kafka_topic_conf_new()
        for key, value in self.consumer_config.items():
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

    def _init_rd_kafka_consumer_group(self):
        crdk.rd_kafka_conf_set_rebalance_cb(self.conf, cb_rebalance)
        crdk.rd_kafka_conf_set_default_topic_conf(self.conf, self.topic_conf)

    def _init_rd_kafka_consumer(self):
        self.consumer = crdk.rd_kafka_new(
            crdk.RD_KAFKA_CONSUMER,
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
        resp = crdk.rd_kafka_brokers_add(self.consumer, brokers_ptr)
        if resp == 0:
            err_str = f"Invalid kafka brokers: {self.brokers}"
            logger.error(err_str)
            raise exceptions.ConsumerError(err_str)
        logger.debug("Added brokers to kafka consumer")

    def assign_topic_offset(self, topic, partition, offset):
      partitions = crdk.rd_kafka_topic_partition_list_new(0)
      crdk.rd_kafka_topic_partition_list_add(partitions, topic.encode(), partition).offset = offset
      crdk.rd_kafka_assign(self.consumer, partitions)
      crdk.rd_kafka_topic_partition_list_destroy(partitions)

    def _init_rd_kafka_topic_partition_lists(self):
        cdef int32_t partition
        cdef char *topic_ptr
        self.topic_partition_list = \
            crdk.rd_kafka_topic_partition_list_new(len(self.topics))
        for topic in self.topics:
            partition = -1
            topic_ptr = topic
            crdk.rd_kafka_topic_partition_list_add(
                self.topic_partition_list, topic_ptr, partition)

    def _init_rd_kafka_subscription(self):
        err = crdk.rd_kafka_subscribe(
            self.consumer,
            self.topic_partition_list
        )
        if err:
            error_str = crdk.rd_kafka_err2str(err)
            logger.error(f"Error subscribing to topic: {error_str}")
            raise exceptions.ConsumerError(error_str)
        logger.debug("Subscribed to topics")

    def get_name(self):
        return bytes(crdk.rd_kafka_name(self.consumer)).decode()

    def assignment(self):
        return current_partition_assignment(self.consumer)
