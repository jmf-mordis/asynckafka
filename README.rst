**********
Asynckafka
**********

.. image:: https://travis-ci.com/jmf-mordis/asynckafka.svg?branch=master
    :target: https://travis-ci.com/jmf-mordis/asynckafka

Fast python Kafka client for asyncio.
Asynckafka is written in Cython_ on top of Rdkafka_ as Kafka driver.

.. _Cython: cython.org
.. _Rdkafka: https://github.com/edenhill/librdkafka

The documentation can be found here_.

.. _here: https://jmf-mordis.github.io/asynckafka/

Features
========

* Consumer using a balanced group
* Consumer using custom topic partitions and offset
* Producer

The library was born as a project to learn Cython and and has not been actively maintained in the last years,
Therefore it should be used with caution.

Examples
========

Simple consumer
---------------

How to use a consumer::

    consumer = Consumer(
        brokers='localhost:9092',
        topics=['my_topic'],
        group_id='my_group_id',
    )
    consumer.start()

    async for message in consumer:
        print(f"Received message: {message.payload}")

Simple producer
---------------

How to use a producer::

    producer = Producer(brokers="localhost:9092")
    producer.start()
    await producer.produce("my_topic", b"my_message")

Benchmark
#########

The test was performed in June of 2018 using a single Kafka broker without replication.
The purpose of the benchmark was only to have an idea of the order of magnitude of the
library's performance under these conditions.

Comparison between asynckafka and aiokafka in production and consumption:

Production
**********

.. image:: https://github.com/jmf-mordis/asynckafka/raw/master/docs/benchmark/graphs/producer.png
   :width: 800

Consumption
***********

.. image:: https://github.com/jmf-mordis/asynckafka/raw/master/docs/benchmark/graphs/consumer.png
   :width: 800

The asynckafka benchmark can be found in the benchmark directory while the
benchmark used for aiokafka can be found in its own repository.