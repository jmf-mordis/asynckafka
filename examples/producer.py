import asyncio

from asynckafka import Producer


async def send_message(producer):
    while True:
        await asyncio.sleep(1)
        producer.produce("Test message !!!!")

loop = asyncio.get_event_loop()
producer = Producer(brokers="127.0.0.1:9092", topic="my_topic")
asyncio.ensure_future(send_message(producer), loop=loop)
loop.run_forever()
