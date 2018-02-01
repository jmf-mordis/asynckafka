import asyncio

from asynckafka import Consumer


async def message_handler(message):
    print(message)

loop = asyncio.get_event_loop()
consumer = Consumer(
    brokers="127.0.0.1:9092", topic='my_topic', message_handler=message_handler,
    loop=loop, consumer_settings={'group.id': 'my_consumer_group'}
)
consumer.start()
loop.run_forever()
