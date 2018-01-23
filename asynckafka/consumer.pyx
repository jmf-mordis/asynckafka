cimport rdkafka

from libc.stdint cimport int32_t, int64_t

cdef int wait_eof = 0


# Kafka logger callback (optional)
cdef void cb_logger(const rdkafka.rd_kafka_t *rk, int level, const char *fac, const char *buf):
    print("logger callback. TODO: call to python logging")


cdef void cb_msg_consume(rdkafka.rd_kafka_message_t *rkmessage):
    err = rkmessage[0].err
    # topic = rdkafka.rd_kafka_topic_name(rkmessage[0].rkt)
    # offset = rkmessage[0].offset
    # partition = rkmessage[0].partition
    # print(f"Topic: {topic}. Offset: {offset}. Partition: {partition}")
    if err:
        print("Error. TODO: error handling!!")
    else:
        # payload = rkmessage[0].payload
        print("Message payload: ")
        # TODO open asyncio task


cdef void cb_rebalance(rdkafka.rd_kafka_t *rk, rdkafka.rd_kafka_resp_err_t err,
                       rdkafka.rd_kafka_topic_partition_list_t *partitions, void *opaque):
    print("%% Consumer group rebalanced: ")
    if err == rdkafka.RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
        print("Partition assigned: ")
        rdkafka.rd_kafka_assign(rk, partitions)
        global wait_eof
        wait_eof += partitions[0].cnt
    elif err == rdkafka.RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
        print("Partition revoqued: ")
        rdkafka.rd_kafka_assign(rk, NULL)
        wait_eof = 0
    else:
        print("Error: ", rdkafka.rd_kafka_err2str(err))
        rdkafka.rd_kafka_assign(rk, NULL)


cdef class Consumer:

    cdef rdkafka.rd_kafka_t *kafka_consumer
    cdef rdkafka.rd_kafka_topic_t *kafka_topic
    cdef char errstr[512]       # librdkafka API error reporting buffer
    cdef rdkafka.rd_kafka_conf_t *conf
    cdef rdkafka.rd_kafka_topic_conf_t *topic_conf
    cdef rdkafka.rd_kafka_topic_partition_list_t *topic_list
    cdef char *brokers
    cdef char *topic
    cdef char *group_id

    def __cinit__(self, brokers, topic, group_id):
        self.brokers = "127.0.0.1:9092"
        self.topic = "test"
        self.group_id = "pytest"
        self._init_config()
        self._init_consumer_group()
        self._init_consumer()
        self._init_topic()

    cpdef _init_config(self):
        self.conf = rdkafka.rd_kafka_conf_new()
        rdkafka.rd_kafka_conf_set_log_cb(self.conf, cb_logger)

        #/* Topic configuration */
        self.topic_conf = rdkafka.rd_kafka_topic_conf_new()

    cdef _init_consumer_group(self):
        conf_resp = rdkafka.rd_kafka_conf_set(self.conf, "group.id", self.group_id, self.errstr, sizeof(self.errstr))
        if conf_resp != rdkafka.RD_KAFKA_CONF_OK:
            print("Wrong response in consumer group")
            # TODO launch exception
            exit(1)
        topic_conf_resp = rdkafka.rd_kafka_topic_conf_set(self.topic_conf, "offset.store.method", "broker", self.errstr, sizeof(self.errstr))
        if topic_conf_resp != rdkafka.RD_KAFKA_CONF_OK:
            print("Wrong response in consumer group")
            # TODO launch exception
            exit(1)

        # Set default topic config for pattern-matched topics. */
        rdkafka.rd_kafka_conf_set_default_topic_conf(self.conf, self.topic_conf)

        # Callback called on partition assignment changes */
        rdkafka.rd_kafka_conf_set_rebalance_cb(self.conf, cb_rebalance)

    def _init_consumer(self):
        self.kafka_consumer = rdkafka.rd_kafka_new(rdkafka.RD_KAFKA_PRODUCER, self.conf, self.errstr, sizeof(self.errstr))
        if not self.kafka_consumer:
            print("null kafka consumer pointer")
            exit(1)
        print("initialized kafka consumer")

        # Added brokers
        resp = rdkafka.rd_kafka_brokers_add(self.kafka_consumer, self.brokers)
        if resp == 0:
            print("No valid brokers")
            # TODO launch exception
            exit(1)

        resp = rdkafka.rd_kafka_poll_set_consumer(self.kafka_consumer)

    def _init_topic(self):
        self.topic_list = rdkafka.rd_kafka_topic_partition_list_new(1)
        cdef int32_t partition = -1
        rdkafka.rd_kafka_topic_partition_list_add(self.topic_list, self.topic, partition)

    def consume_messages(self):
        cdef rdkafka.rd_kafka_message_t *rkmessage
        print("asdfads")
        while True:
            rkmessage = rdkafka.rd_kafka_consumer_poll(self.kafka_consumer, 1000)
            print("asdfadsdsfadsfsa")
            if rkmessage:
              cb_msg_consume(rkmessage)
            #     rdkafka.rd_kafka_message_destroy(rkmessage)

