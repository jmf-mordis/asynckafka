from asynckafka.includes cimport c_rd_kafka as rdk


ctypedef enum producer_states:
    STARTED
    STOPPED


cdef class Producer:
    cdef:
        rdk.rd_kafka_t *_rd_kafka_producer
        rdk.rd_kafka_topic_t *_rd_kafka_topic
        char errstr[512]       # librdkafka API error reporting buffer
        rdk.rd_kafka_conf_t *_rd_kafka_conf

        char _debug
        object _periodic_poll_task
        producer_states producer_state

        public bytes brokers
        public bytes topic
        public dict producer_settings
        public object loop
