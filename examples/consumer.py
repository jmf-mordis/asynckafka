import asyncio
import uvloop

import sys
sys.path.append('.')


import time

from asynckafka import StreamConsumer, set_debug

import logging
import sys
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
set_debug(True)

messages = 0


async def print_messages():
    while True:
        await asyncio.sleep(1)
        print(messages, time.time())


async def consume_messages():
    async for message in consumer:
        print(message.payload)


loop = asyncio.get_event_loop()
consumer = StreamConsumer(
    brokers="127.0.0.1:9092",
    topics=['my_topic'],
    group_id='my_group',
    loop=loop,
    debug=False
)
consumer.start()

asyncio.ensure_future(consume_messages(), loop=loop)
asyncio.ensure_future(print_messages(), loop=loop)

try:
    loop.run_forever()
finally:
    consumer.stop()
    loop.stop()
