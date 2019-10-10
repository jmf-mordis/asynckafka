Error callback
==============

The error callback can be passed to the consumer of producer, it should be a
coroutine function and accepts one parameter that it is a KafkaError.

Example::

    async def error_callback(kafka_error):
        print(kafka_error)

    # Should be a wrong port
    self.producer = Producer(
        brokers="127.0.0.1:6000",
        error_callback=error_callback
    )

