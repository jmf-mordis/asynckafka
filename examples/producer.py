import asyncio

import logging
import sys
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

from asynckafka import Producer, set_debug

set_debug(True)


async def send_message(producer):
    while True:
        await asyncio.sleep(1)
        await producer.produce(topic="my_topic", message=b"Test message !!!!")

loop = asyncio.get_event_loop()
producer = Producer(brokers="127.0.0.1:9092")
producer.start()
asyncio.ensure_future(send_message(producer), loop=loop)

try:
    loop.run_forever()
finally:
    producer.stop()
    loop.stop()
