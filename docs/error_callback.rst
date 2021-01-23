Error callback
==============

The error callback can be used in the consumer or producer. The callback can be function or a coroutine that
accepts the kafka_error parameter.

Example::

    async def error_callback(kafka_error):
        print(kafka_error)

    # Should be a wrong port
    self.producer = Producer(
        brokers="127.0.0.1:6000",
        error_callback=error_callback
    )

