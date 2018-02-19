import asyncio
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

import sys
sys.path.append('.')


import time

from asynckafka import Consumer

import logging
import sys
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

messages = 0
initial_time = time.time()


async def print_messages():
    while True:
        await asyncio.sleep(1)
        print(messages, time.time())


async def message_handler_my_topic(message):
    global messages
    messages += 1
    list()


loop = asyncio.get_event_loop()
consumer = Consumer(
    brokers="127.0.0.1:9092",
    group_id='my_group',
    loop=loop,
    spawn_tasks=False,
    debug=False
)
consumer.add_message_handler('my_topic', message_handler_my_topic)
consumer.start()
asyncio.ensure_future(print_messages(), loop=loop)

try:
    loop.run_forever()
finally:
    consumer.stop()
    loop.stop()
