import logging

from asynckafka.includes cimport c_rd_kafka as crdk


logger = logging.getLogger('asynckafka')


cdef list current_partition_assignment(crdk.rd_kafka_t *rk):
    cdef crdk.rd_kafka_topic_partition_list_t *partitions = new_partition()
    err = crdk.rd_kafka_assignment(rk, &partitions)
    if err:
        error_str = crdk.rd_kafka_err2str(err)
        logger.warning(f"Error creating a topic partition list {error_str}")
        return []
    topic_partitions = []
    for i in range(partitions.cnt):
        crdk_partition = partitions.elems[i]
        topic_partition = TopicPartition()
        topic_partition.topic = crdk_partition.topic
        topic_partition.offset = crdk_partition.offset
        topic_partition.partition = crdk_partition.partition
        metadata_ptr = <char*> crdk_partition.metadata
        topic_partition.metadata = metadata_ptr[:crdk_partition.metadata_size]
        topic_partitions.append(topic_partition)
    crdk.rd_kafka_topic_partition_list_destroy(partitions)
    return topic_partitions


cdef crdk.rd_kafka_topic_partition_list_t* new_partition():
    cdef crdk.rd_kafka_topic_partition_list_t partition_list = \
        crdk.rd_kafka_topic_partition_list_t()
    return &partition_list


cdef class TopicPartition:

    def __repr__(self):
        return f"topic: {self.topic.decode()}, partition: {self.partition}, " \
               f"offset: {self.offset}, metadata: {self.metadata}"
