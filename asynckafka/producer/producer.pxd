from asynckafka.includes cimport c_rd_kafka as rdk


cdef void dr_msg_cb (
    rdk.rd_kafka_t *rk,
    const rdk.rd_kafka_message_t
    *rkmessage, void *opaque
)


cdef class Producer:
    cdef:
        rdk.rd_kafka_t *kafka_producer
        rdk.rd_kafka_topic_t *kafka_topic
        char errstr[512]       # librdkafka API error reporting buffer
        rdk.rd_kafka_conf_t *conf

