from asynckafka.includes cimport c_rd_kafka as crdk
from libc.stdint cimport int32_t, int64_t


cdef Message message_factory(crdk.rd_kafka_message_t *rk_message)


cdef class Message:
    cdef:
        public char error

        public bytes payload
        public bytes key

        public str topic
        public int32_t offset

