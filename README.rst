**********
Asynckafka
**********

Fast python kafka client for asyncio.
Asynckafka is written in cython on top of rdkafka as kafka driver.


Right now it is work in progress, so use it at our own risk. Before the 1.0.0
release i don't warranty stability in the api between the minor version
numbers.

Performance
###########

This project was born from the need to have a high performance kafka library
for asyncio. The others asyncio kafka clients that i tested do not offer a
good enough performance for some applications.

Benchmarks
**********


Using Asynckafka
################


Consumer
********

It works as an asyncronous iterator.

Example::

    import asyncio

    from asynckafka import StreamConsumer


    async def consume_messages(message_stream):
        async for message in message_stream:
            print(message)


    loop = asyncio.get_event_loop()
    message_stream = AsyncIterConsumer(
        brokers='localhost:9092',
        topic='my_topic',
        group_id='my_group_id',
        loop=loop
    )
    message_stream.start()

    consume_coroutine = consume_messages(message_stream)
    asyncio.ensure_future(consume_coroutine, loop=loop)

    try:
        loop.run_forever()
    finally:
        loop.stop()
        message_stream.stop()


Producer
********

Producer example::

    import asyncio

    from asynckafka import Producer


    async def send_messages(producer):
        while True:
            await asyncio.sleep(1)
            producer.produce("my_message")


    loop = asyncio.get_event_loop()
    producer = Producer(
        brokers="localhost:9092",
        topic="my_topic"
    )

    asyncio.ensure_future(send_message(producer), loop=loop)
    loop.run_forever()


Requirements
############

#. Python 3.6 or greater
#. Rdkafka 0.11.X

Install rdkafka
***************

WIP

Install package
***************

Install it with pip::

    $ pip install asynckafka


Logging
#######

Asynckafka uses the standard python logging library, with "asynckafka" as
logger.

To enable the logging to stdout it is enough with::

    import logging
    import sys
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

Develop
#######

How to build the package
************************

How to run the tests
********************


