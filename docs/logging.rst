Logging
=======

Asynckafka uses the standard Python logging library, with "asynckafka" as
logger.

To enable all the logging to stdout::

    import logging
    import sys

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

There are some Python debug lines disabled by default in the consumer
and producer to avoid the Python string composition of the logger in the
 critical path of Cython. Anyway, you can enable it with::

    import asynckafka

    asynckafka.set_debug(True)

