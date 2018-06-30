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

Documentation_

.. _Documentation: https://jmf-mordis.github.io/asynckafka/


Benchmark
#########

This project was born from the need to have a high performance kafka library
for asyncio.

Comparison between Asynckafka and Aiokafka in production and consumption.

Production
**********

.. image:: docs/benchmark/graphs/producer.png
   :width: 800

Consumption
***********

.. image:: docs/benchmark/graphs/consumer.png
   :width: 800

The benchmark used for asynckafka is in benchmark directory while the
benchmark used for aiokafka is in its own repository, also in the benchmark folder.