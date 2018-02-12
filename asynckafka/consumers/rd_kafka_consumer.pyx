import logging
from libc.stdint cimport int32_t

from asynckafka.includes cimport c_rd_kafka as crdk
from asynckafka import exceptions

logger = logging.getLogger('asynckafka')


cdef void cb_logger(const crdk.rd_kafka_t *rk, int level, const char *fac,
                    const char *buf):
    if level in {1, 2}:
        logger.critical(f"{str(fac)}:{str(buf)}")
    elif level == 3:
        logger.error(f"{str(fac)}:{str(buf)}")
    elif level in {4, 5}:
        logger.info(f"{str(fac)}:{str(buf)}")
    elif level in {6, 7} :
        logger.debug(f"{str(fac)}:{str(buf)}")
    else:
        logger.critical(f"Unexpected logger level {level}")
        logger.critical(f"{fac}:{buf}")


cdef log_partition_list(crdk.rd_kafka_topic_partition_list_t *partitions):
    string = "List of partitions:"
    for i in range(partitions.cnt):
        topic = partitions.elems[i].topic
        partition = partitions.elems[i].partition
        offset = partitions.elems[i].offset
        string += f" [Topic: {str(topic)}, " \
                  f"Partition: {str(partition)}, " \
                  f"Offset: {str(offset)}]"
    logger.debug(string)


cdef void cb_rebalance(
        crdk.rd_kafka_t *rk, crdk.rd_kafka_resp_err_t err,
        crdk.rd_kafka_topic_partition_list_t *partitions, void *opaque):
    logger.debug("Consumer group rebalance")
    if err == crdk.RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
        logger.debug("New partitions assigned")
        log_partition_list(partitions)
        crdk.rd_kafka_assign(rk, partitions)
    elif err == crdk.RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
        logger.debug("Revoked Partitions")
        log_partition_list(partitions)
        crdk.rd_kafka_assign(rk, NULL)
    else:
        err_str = crdk.rd_kafka_err2str(err)
        logger.error(
            f"Error in rebalance callback, "
            f"Revoked partitions {err_str}"
        )
        crdk.rd_kafka_assign(rk, NULL)


cdef class RdKafkaConsumer:

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

    def start(self):
        self._init_rd_kafka_configs()
        self._init_rd_kafka_consumer_group()
        self._init_rd_kafka_consumer()
        self._init_rd_kafka_topic_partition_lists()
        self._init_rd_kafka_subscription()

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

    def add_topic(self, topic):
        self.topics.append(topic)

    def _init_rd_kafka_configs(self):
        self.conf = crdk.rd_kafka_conf_new()
        crdk.rd_kafka_conf_set_log_cb(self.conf, cb_logger)
        self.topic_conf = crdk.rd_kafka_topic_conf_new()
        for key, value in self.consumer_settings.items():
            conf_resp = crdk.rd_kafka_conf_set(
                self.conf, key, value, self.errstr, sizeof(self.errstr)
            )
            self._parse_rd_kafka_conf_response(conf_resp, key, value)
        for key, value in self.topic_settings.items():
            conf_resp = crdk.rd_kafka_topic_conf_set(
                self.topic_conf,
                key, value,
                self.errstr,
                sizeof(self.errstr)
            )
            self._parse_rd_kafka_conf_response(conf_resp, key, value)
        crdk.rd_kafka_conf_set_default_topic_conf(
            self.conf, self.topic_conf)

    @staticmethod
    def _parse_rd_kafka_conf_response(conf_respose, key, value):
        if conf_respose == crdk.RD_KAFKA_CONF_OK:
            logger.debug(f"Correctly configured rdkafka {key} with value "
                         f"{value}")
        elif conf_respose == crdk.RD_KAFKA_CONF_INVALID:
            err_str = f"Invalid {key} setting with value: {value}"
            logger.error(err_str)
            raise exceptions.InvalidSetting(err_str)
        elif conf_respose == crdk.RD_KAFKA_CONF_UNKNOWN:
            err_str = f"Unknown {value} setting with value {value}"
            logger.error(err_str)
            raise exceptions.UnknownSetting(err_str)

    def _init_rd_kafka_consumer_group(self):
        if b'group.id' in self.consumer_settings:
            crdk.rd_kafka_conf_set_rebalance_cb(
                self.conf,
                cb_rebalance
            )

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
            raise exceptions.InvalidBrokers(err_str)
        logger.debug("Added brokers to kafka consumer")

        err_poll = crdk.rd_kafka_poll_set_consumer(self.consumer)
        if err_poll:
            err_str_poll = crdk.rd_kafka_err2str(err_poll)
            logger.error(err_str_poll)
            raise exceptions.ConsumerError(err_str_poll)

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
            raise exceptions.SubscriptionError(error_str)
        logger.debug("Subscribed to topic ")
