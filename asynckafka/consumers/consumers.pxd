from asynckafka.consumers.consumer_thread cimport ConsumerThread
from asynckafka.consumers.rd_kafka_consumer cimport RdKafkaConsumer


ctypedef enum consumer_states:
    CONSUMING
    NOT_CONSUMING


cdef class Consumer:
    cdef:
        RdKafkaConsumer _rdk_consumer
        ConsumerThread _consumer_thread
        consumer_states _consumer_state

        object message_handlers
        object loop
        object _poll_consumer_thread_task
        object _poll_rd_kafka_task
        object _spawn_tasks
        char _debug

        _open_asyncio_task(self, long message_memory_address)


cdef class StreamConsumer:
    cdef:
        RdKafkaConsumer _rd_kafka
        ConsumerThread _consumer_thread
        consumer_states _consumer_state

        object _loop
        object _stop
        object _open_tasks_task
        object _poll_rd_kafka_task
        object _spawn_tasks
        char _debug
        object _topic

        _destroy_message(self, long message_memory_address)
