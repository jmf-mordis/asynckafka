import asyncio
import logging
import sys

from asynckafka import Consumer

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)


async def consume_messages(consumer):
    async for message in consumer:
        print(f"Received message: {message.payload}")

consumer = Consumer(
    brokers='localhost:9092',
    topics=['my_topic'],
    group_id='my_group_id',
)
consumer.start()

asyncio.ensure_future(consume_messages(consumer))

loop = asyncio.get_event_loop()
try:
    loop.run_forever()
finally:
    consumer.stop()
    loop.stop()
