from asynckafka.consumers.rd_kafka_consumer cimport RdKafkaConsumer
from asynckafka.includes cimport c_rd_kafka as crdk


cdef class ConsumerThread:
    cdef RdKafkaConsumer _rd_kafka

    cdef object thread
    cdef object stop_event

    cdef public list thread_communication_list
    cdef public unsigned long consumption_limiter

    cdef char _debug

    cpdef _main_poll_rdkafka(self)

    cdef _cb_consume_message(
            self,
            crdk.rd_kafka_message_t *rk_message
    )
    cdef _send_message_to_asyncio(
            self,
            crdk.rd_kafka_message_t *rk_message
    )

    cdef increase_consumption_limiter(self)
    cdef inline decrease_consumption_limiter(self)
    cdef _destroy_remaining_messages(self)

