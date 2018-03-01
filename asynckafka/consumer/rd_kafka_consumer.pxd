from asynckafka.includes cimport c_rd_kafka as crdk


ctypedef enum consumer_states:
    NOT_STARTED
    STARTED
    STOPPED


cdef class RdKafkaConsumer:
    cdef:
        crdk.rd_kafka_t *consumer
        crdk.rd_kafka_conf_t *conf
        crdk.rd_kafka_topic_conf_t *topic_conf
        crdk.rd_kafka_topic_partition_list_t *topic_partition_list
        char errstr[512]

        bytes brokers
        public list topics
        public dict consumer_config
        public dict topic_config
        public consumer_states status
