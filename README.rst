**********
Asynckafka
**********

Fast python kafka library for asyncio. 
Asyncafka is written in cython and essentially it provides an easy interface
for use rdkafka with asyncio.

Right now it is work in progress, it's little more than a proof of concept,
please don't use it in production, or use it at our own risk. I also do not
guarantee stability in the api during this period.

Performance
###########

This is the main porpoise of this library, there is others python library's
for python with good performance, but them they do not work natively
with asyncio.

On the other hand the libraries that support asyncio do not offer a good
enough performance for some applications.


WIP


Using Asynckafka
################

Consumer
**************

Basic consumer example::

    import asyncio

    from asynckafka import Consumer


    async def message_handler(message):
        print(message)


    loop = asyncio.get_event_loop()
    consumer = Consumer(
        brokers='localhost:9092', 
        message_handlers={
            'my_topic': message_handler
        },
        consumer_settings={'group.id': 'my_group'},
        loop=loop
    )
    consumer.start()
    loop.run_forever()



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
