Settings
========

This library is built on top of rdkafka, and as wrapper it uses the same
configuration that it. The rdkafka configuration is very extensive and can
be found in the following link:

https://github.com/edenhill/librdkafka/blob/v0.11.3/CONFIGURATION.md

.. _configuration: https://github.com/edenhill/librdkafka/blob/v0.11.3/CONFIGURATION.md

The asynckafka producer and consumer accepts two configuration dicts. The
dicts should have strings in the key and value.

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

The configuration that can be passed to `rdk_consumer_config correspond with
the` configuration found in the 'Global configuration properties' in the
rdkafka configuration_ documentation (less the configuration exclusive from
the producer indicated with a 'P' to the left of the name). The
rdk_producer_config is exactly the same of the consumer but excluding the
ones indicated with 'P'.

At the end of configuration_ you can find the 'Topic configuration
properties', there is located the configuration that can be passed to the
argument `rdk_topic_config` of the consumer of producer.

