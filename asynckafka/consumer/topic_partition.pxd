from asynckafka.includes cimport c_rd_kafka as crdk
from libc.stdint cimport int32_t, int64_t


cdef list current_partition_assignment(
        crdk.rd_kafka_t *rk
)

cdef class TopicPartition:
    cdef:
        public bytes topic
        public int32_t partition
        public int64_t offset
        public bytes metadata
