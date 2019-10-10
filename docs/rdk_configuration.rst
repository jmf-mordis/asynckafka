RdKafka configuration
=====================

This library is built on top of rdkafka, and as wrapper it uses the same
configuration that it. The rdkafka configuration is very extensive and can
be found in the following link:

https://github.com/edenhill/librdkafka/blob/v0.11.3/CONFIGURATION.md

.. _configuration: https://github.com/edenhill/librdkafka/blob/v0.11.3/CONFIGURATION.md

The asynckafka producer and consumer accepts configuration dicts, one for the producer
or consumer and another for the topic.

Consumer configuration, example::

    Consumer(
        brokers='127.0.0.1:9092',
        topics=['my_topic'],
        rdk_consumer_config={
            'api.version.request': 'true'
            'enable.auto.commit': 'false'
        },
        rdk_topic_config={
            'auto.offset.reset':  'smallest'
        }
    )

Producer configuration, example::

    producer = Producer(
        brokers='127.0.0.1:9092',
        rdk_producer_config={
            'batch.num.messages': '100000',
            'message.send.max.retries': 4,
        },
        rdk_topic_config={
            'message.timeout.ms': '10000'
        },
    )

If you are looking to use this library, you should carefully read the configuration of
rdkafka in its different sections, because in the end this library is a wrapper of rdkafka.
