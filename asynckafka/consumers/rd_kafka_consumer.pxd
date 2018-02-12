from asynckafka.includes cimport c_rd_kafka as crdk


cdef void cb_logger(
        const crdk.rd_kafka_t *rk,
        int level,
        const char *fac,
        const char *buf
)


cdef log_partition_list(
        crdk.rd_kafka_topic_partition_list_t *partitions
)


cdef void cb_rebalance(
        crdk.rd_kafka_t *rk,
        crdk.rd_kafka_resp_err_t err,
        crdk.rd_kafka_topic_partition_list_t *partitions, void *opaque
)


cdef class RdKafkaConsumer:
    cdef crdk.rd_kafka_t *consumer
    cdef crdk.rd_kafka_conf_t *conf
    cdef crdk.rd_kafka_topic_conf_t *topic_conf
    cdef crdk.rd_kafka_topic_partition_list_t *topic_partition_list
    cdef char errstr[512]

    cdef bytes brokers
    cdef list topics
    cdef dict consumer_settings
    cdef dict topic_settings
