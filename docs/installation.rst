Requirements
============

#. Python 3.6+
#. GCC
#. Rdkafka >= 0.11

Install rdkafka from source
---------------------------

Rdkafka_ headers are required to compile asynckafka, download
rdkafka from here_, then unpack the package and run::

    ./configure
    make
    sudo make install

.. _here: https://github.com/edenhill/librdkafka/releases
.. _Rdkafka: https://github.com/edenhill/librdkafka

Most of package managers have a package with the rdkafka headers.

Ubuntu 18.04::

    apt-get install librdkafka-dev

For the MacOS users with Homebrew, the rdkafka headers already comes with the librdkafka package::

    brew install librdkafka

For more information go to the librdkafka repository.

Install asynckafka package
--------------------------

A C compiler is also required, the Cython package is compiled from source when asynckafka is installed with pip.

The installation of the package is done as a regular Pypi package::

    $ pip install asynckafka
