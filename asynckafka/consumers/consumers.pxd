from asynckafka.consumers.consumer_thread cimport ConsumerThread
from asynckafka.consumers.rd_kafka_consumer cimport RdKafkaConsumer


ctypedef enum consumer_states:
    CONSUMING
    NOT_CONSUMING


cdef class ConsumerBase:
    cdef:
        RdKafkaConsumer rdk_consumer
        ConsumerThread consumer_thread
        consumer_states consumer_state
        char debug

        object poll_rd_kafka_task
        object loop


cdef class Consumer(ConsumerBase):
    cdef:
        object message_handlers
        object _poll_consumer_thread_task
        object _spawn_tasks

        _open_asyncio_task(self, long message_memory_address)


cdef class StreamConsumer(ConsumerBase):
    cdef:
        object _open_tasks_task
        object _spawn_tasks
        object topic

        _destroy_message(self, long message_memory_address)
