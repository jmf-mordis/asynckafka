from asynckafka.includes cimport c_rd_kafka as crdk


cdef class KafkaTopic:
    cdef:
        public bytes name
        public long rdk_topic_memory_address



cdef class RdKafkaProducer:
    cdef:
        crdk.rd_kafka_t *producer
        crdk.rd_kafka_conf_t *conf
        crdk.rd_kafka_topic_conf_t *topic_conf
        char errstr[512]

        public bytes brokers
        public dict topics
        public dict producer_config
        public dict topic_config

        inline KafkaTopic _rd_kafka_topic_factory(self, name)
        inline KafkaTopic add_topic(self, topic_name)
        inline crdk.rd_kafka_topic_t *_get_rdk_topic(self, kafka_topic)
        inline crdk.rd_kafka_topic_t *get_rdk_topic(self, topic_name)
