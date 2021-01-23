Logging
=======

Asynckafka uses the standard Python logging. The name of the logger is "asynckafka".

To enable the logging to stdout::

    import logging
    import sys

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

There are some Python debug lines disabled by default in the consumer
and producer that only can be enabled with an the following call::

    import asynckafka

    asynckafka.set_debug(True)

