from asynckafka.producer.rd_kafka_producer cimport RdKafkaProducer


ctypedef enum producer_states:
    STARTED
    STOPPED


cdef class Producer:
    cdef:
        RdKafkaProducer rdk_producer

        public object loop
        object periodic_poll_task
        producer_states producer_state
        public object error_callback
