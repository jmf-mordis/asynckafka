import asyncio
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

import sys
sys.path.append('.')


import time

from asynckafka import StreamConsumer

import logging
import sys
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

messages = 0
initial_time = time.time()


async def print_messages():
    while True:
        await asyncio.sleep(1)
        print(messages, time.time())


async def consume_messages():
    async for message in consumer:
        print(message.tobytes())


loop = asyncio.get_event_loop()
consumer = StreamConsumer(
    brokers="127.0.0.1:9092",
    topic='my_topic',
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
