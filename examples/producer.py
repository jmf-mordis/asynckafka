import asyncio

import logging
import sys
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

from asynckafka import Producer


async def send_message(producer):
    while True:
        await asyncio.sleep(1)
        await producer.produce(b"Test message !!!!")

loop = asyncio.get_event_loop()
producer = Producer(brokers="127.0.0.1:9092", topic="my_topic", debug=True)
producer.start()
asyncio.ensure_future(send_message(producer), loop=loop)

try:
    loop.run_forever()
finally:
    producer.stop()
    loop.stop()
