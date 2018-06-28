Requirements
============

#. Python 3.6 or greater
#. Rdkafka 0.11.X

Install rdkafka from source
---------------------------

You need the Rdkafka_ headers to be able to compile asynckafka, download
rdkafka from here_, then unpack the package and run::

    ./configure
    make
    sudo make install

.. _here: https://github.com/edenhill/librdkafka/releases
.. _Rdkafka: https://github.com/edenhill/librdkafka

Install asynckafka package
--------------------------

The package is in pypi, you can install it with pip::

    $ pip install asynckafka
