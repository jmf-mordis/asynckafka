**********
Asynckafka
**********

Fast python kafka client for asyncio.
Asynckafka is written in Cython_ cython on top of Rdkafka_ as kafka driver.

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


Using Asynckafka
################

You can find one simple producer and consumer example in examples/.

Requirements
############

#. Python 3.6 or greater
#. Rdkafka 0.11.X

Install rdkafka from source
***************************

You can download rdkafka from here_, then unpack the package
and run::

    ./configure
    make
    sudo make install

.. _here: https://github.com/edenhill/librdkafka/releases

Install rdkafka from package managers
*************************************

Also there are a lot packages managers (it depends on your operating system)
with the precompiled binaries, for example:

Ubuntu 17.10::

    sudo apt-get install librdkafka1

MacOS using homebrew::

    brew install librdkafka

Install asynckafka package
**************************

The package is in pypi, you can install it with pip::

    $ pip install asynckafka


Logging
#######

Asynckafka uses the standard python logging library, with "asynckafka" as
logger.

To enable all the logging to stdout it is enough with::

    import logging
    import sys

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

There are some python logger debug lines disabled by default in the consumer
and producer. The reason is avoid python calls and python strings
composition for the python logger in the critical path of cython. Anyway, you
can enable them with::

    import asynckafka

    asynckafka.set_debug(True)

Develop
#######

How to build the package
************************

WIP

How to run the tests
********************

WIP


