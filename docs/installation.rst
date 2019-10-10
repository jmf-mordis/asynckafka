Requirements
============

#. Python 3.6 or 3.7
#. GCC
#. Rdkafka >= 0.11

Install rdkafka from source
---------------------------

You need the Rdkafka_ headers to be able to compile asynckafka, download
rdkafka from here_, then unpack the package and run::

    ./configure
    make
    sudo make install

.. _here: https://github.com/edenhill/librdkafka/releases
.. _Rdkafka: https://github.com/edenhill/librdkafka

Some package managers have a package with the rdkafka headers. For example in Ubuntu 18.04::

    apt-get install librdkafka-dev

For the MacOS users with Homebrew, the rdkafka headers already comes with the librdkafka package::

    brew install librdkafka

Install asynckafka package
--------------------------

You also are going to need gcc to be able to compile the Cython package.

The package is in Pypi, you can install it with pip::

    $ pip install asynckafka
