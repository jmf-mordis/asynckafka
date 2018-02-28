from asynckafka.consumer.rd_kafka_consumer cimport RdKafkaConsumer


ctypedef enum consumer_states:
    CONSUMING
    NOT_CONSUMING


cdef class Consumer:
    cdef:
        RdKafkaConsumer rdk_consumer
        consumer_states consumer_state

        object poll_rd_kafka_task
        public object loop
        list topics
        public object error_callback
