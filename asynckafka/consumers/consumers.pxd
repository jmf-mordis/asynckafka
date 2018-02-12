from asynckafka.consumers.consumer_thread cimport ConsumerThread
from asynckafka.consumers.rd_kafka_consumer cimport RdKafkaConsumer


cdef class Consumer:
    cdef RdKafkaConsumer _rdk_consumer
    cdef ConsumerThread _consumer_thread

    cdef object message_handlers
    cdef object loop
    cdef object _poll_consumer_thread_task
    cdef object _spawn_tasks
    cdef bint _debug

    cdef _open_asyncio_task(self, long message_memory_address)


cdef class StreamConsumer:
    cdef RdKafkaConsumer _rd_kafka
    cdef ConsumerThread _consumer_thread

    cdef object _loop
    cdef object _stop
    cdef object _open_tasks_task
    cdef object _spawn_tasks
    cdef bint _debug
    cdef object _topic

    cdef _destroy_message(self, long message_memory_address)
