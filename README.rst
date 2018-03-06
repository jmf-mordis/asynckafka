**********
Asynckafka
**********

Fast python kafka client for asyncio.
Asynckafka is written in Cython_ on top of Rdkafka_ as kafka driver.

Right now it is work in progress, so use it at our own risk. Before the 1.0.0
release i don't warranty stability in the api between the minor version
numbers.

.. _Cython: cython.org
.. _Rdkafka: https://github.com/edenhill/librdkafka

Documentation url: WIP

Performance
###########

This project was born from the need to have a high performance kafka library
for asyncio.

Benchmark
*********

Simple benchmark with one kafka broker, one partition, 200 bytes per message
and 10 millions of messages::

    Preparing benchmark.
    Filling topic  benchmark_ad5682b7-9469-4f35-ad72-933c5e9879e1 with
    10000000 messages of 200 bytes each one.
    The time used to produce the messages is 21.905211210250854 seconds.
    Throughput: 91.30247505050632 mb/s

    Starting to consume the messages.
    The time used to consume the messages is 20.685954093933105 seconds.
    Throughput: 96.68396202167787 mb/s



