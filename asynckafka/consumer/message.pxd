from asynckafka.includes cimport c_rd_kafka as crdk
from libc.stdint cimport int32_t


cdef Message message_factory(crdk.rd_kafka_message_t *rk_message)


cdef class Message:
    cdef public char error
    """1 if the message is a error 0 if not"""
    cdef public bytes payload
    cdef public bytes key

    cdef public str topic
    cdef public int32_t offset

