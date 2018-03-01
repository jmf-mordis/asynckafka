Logging
=======

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

