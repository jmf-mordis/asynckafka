import asyncio

from asynckafka import Consumer

import logging
import sys
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


async def message_handler(message):
    print(message)

loop = asyncio.get_event_loop()
consumer = Consumer(
    brokers="127.0.0.1:9092", topic='my_topic', message_handler=message_handler,
    loop=loop, consumer_settings={'group.id': 'my_group'}
)
consumer.start()
loop.run_forever()
