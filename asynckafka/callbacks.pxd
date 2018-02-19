from asynckafka.includes cimport c_rd_kafka as crdk


cdef void cb_logger(
        const crdk.rd_kafka_t *rk,
        int level,
        const char *fac,
        const char *buf
)


cdef void cb_error(
        crdk.rd_kafka_t *rk,
        int err,
        const char *reason,
        void *opaque
)


cdef void cb_rebalance(
        crdk.rd_kafka_t *rk,
        crdk.rd_kafka_resp_err_t err,
        crdk.rd_kafka_topic_partition_list_t *partitions,
        void *opaque
)
