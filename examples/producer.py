import asyncio
import logging
import sys

from asynckafka import Producer

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


async def send_messages(producer):
    while True:
        await producer.produce("my_topic", b"my_message")
        print('sent message')
        await asyncio.sleep(1)

producer = Producer(brokers="localhost:9092")
producer.start()

asyncio.ensure_future(send_messages(producer))

loop = asyncio.get_event_loop()

try:
    loop.run_forever()
finally:
    producer.stop()
    loop.stop()
