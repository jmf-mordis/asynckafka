**********
Asynckafka
**********

Fast python kafka library for asyncio. 
Asyncafka is written in cython and essentially provides an user
friendly interface for use rdkafka with asyncio.

Right now it is work in progress and it's little more than a proof of concept,
please don't use it in production, or use it at our own risk. I also do not
guarantee stability in the api during this period.

Performance
###########

The performance is the main porpoise of this library, there is others good
python library's with good performance, as confluent-kafka, but do not work
natively asyncio.

On the other hand, the asyncio libraries that i tested do not offer a good
enough performance for some applications.


WIP


Using Asynckafka
################

Consumer with message handlers
******************************
It opens a asyncio task per message in each message handler. You can manage
the number of tasks opened by this consumer with the parameter max_coroutines,
allowing you to control the back pressure of the service.

Consumer with message handlers example::

    import asyncio

    from asynckafka import Consumer


    async def message_handler(message):
        print(message)


    loop = asyncio.get_event_loop()

    consumer = Consumer(brokers='localhost:9092', group_id='my_group_id',
        loop=loop)
    consumer.set_message_handler('my_topic', message_handler)
    consumer.start()

    loop.run_forever()


Stream Consumer
**************************
Consumer that consume messages from one topic as an asyncronous iterator.
In contrast with the message handler consumer this consumer don't spawn a
asyncio task per message. It is going to increase the performance if you
really don't need the tasks. But if you want the a good performance take in
account that you should't do any blocking operation inside the async for.

Async iterator consumer example::

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
**************

Basic producer example::

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

How to use
##########

Requirements
****************

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

Asynckafka uses the standard logging library, the logger name is ""asynckafka".

For enable it to stdout is enough with::

    import logging
    import sys
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

