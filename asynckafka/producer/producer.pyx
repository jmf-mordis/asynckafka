from asynckafka.includes cimport c_rd_kafka as rdk


cdef void dr_msg_cb (
        rdk.rd_kafka_t *rk, const rdk.rd_kafka_message_t *rkmessage,
        void *opaque):
    # TODO callback is not working well
    err = rkmessage[0].err
    if err:
        print("Message delivery failed: ")
        exit(1)
    else:
        print("Message delivered ")


cdef class Producer:

    def __cinit__(self, brokers: str, topic: str):
        self._init_config(brokers)
        self._init_producer()
        self._init_topic(topic)

    def _init_config(self, brokers: str):
        brokers_bytes = brokers.encode()
        self.conf = rdk.rd_kafka_conf_new()
        rdk.rd_kafka_conf_set_dr_msg_cb(self.conf, dr_msg_cb)
        conf_resp = rdk.rd_kafka_conf_set(self.conf, "bootstrap.servers",
                                      brokers_bytes, self.errstr,
                                      sizeof(self.errstr))
        if conf_resp != 0:
            print("Wrong response from settings")
            exit(1)

    def _init_producer(self):
        self.kafka_producer = rdk.rd_kafka_new(rdk.RD_KAFKA_PRODUCER,
                                               self.conf, self.errstr,
                                               sizeof(self.errstr))
        if not self.kafka_producer:
            print("null kafka producer pointer")
            exit(1)
        print("initialized kafka producer")

    def _init_topic(self, topic: str):
        topic_bytes = topic.encode()
        self.kafka_topic = rdk.rd_kafka_topic_new(self.kafka_producer,
                                               topic_bytes, NULL)
        if not self.kafka_topic:
            print("Null kafka topic pointer")
            exit(1)
        print("initialized kafka topic")

    def produce(self, message: str):
        cdef bytes message_bytes = message.encode()
        cdef char *message_ptr = message_bytes
        resp = rdk.rd_kafka_produce(
            self.kafka_topic,
            rdk._RD_KAFKA_PARTITION_UA, rdk._RD_KAFKA_MSG_F_BLOCK,
            message_ptr, len(message_bytes),
            NULL, 0,
            NULL
        )
        if resp == -1:
            # TODO Proper error handling
            print("Failed producing message")
            exit(1)
        print("Sent message")


