import asyncio

from asynckafka import Producer, StreamConsumer
from benchmarks import config

loop = asyncio.get_event_loop()


def print_throughput(time_interval):
    megabytes_per_second = (
        config.MESSAGE_NUMBER * config.MESSAGE_BYTES)/time_interval/1e6
    print(f"Throughput: {megabytes_per_second} mb/s ")


async def fill_topic_with_messages():
    producer = Producer(
        brokers=config.KAFKA_URL,
        topic=config.TOPIC,
        producer_settings=config.PRODUCER_SETTINGS,
        debug=False,
    )
    producer.start()

    messages_consumed = 0

    async def print_messages_produced():
        with config.Timer() as timer:
            while True:
                await asyncio.sleep(1)
                print(f"{messages_consumed} produced "
                      f"in {timer.elapsed_time} seconds")

    print(f"Preparing benchmark. Filling topic  {config.TOPIC} with "
          f"{config.MESSAGE_NUMBER} of {config.MESSAGE_BYTES} bytes.")
    print_task = asyncio.ensure_future(print_messages_produced())

    with config.Timer() as timer:
        for _ in range(config.MESSAGE_NUMBER):
            messages_consumed += 1
            await producer.produce(config.MESSAGE)
        producer.stop()
    print(f"the producer time to send the messages is {timer.interval} "
          f"seconds.")
    print_throughput(timer.interval)
    print_task.cancel()


async def consume_the_messages():
    stream_consumer = StreamConsumer(
        brokers=config.KAFKA_URL,
        topic=config.TOPIC,
        consumer_settings=config.CONSUMER_SETTINGS,
        topic_settings=config.TOPIC_SETTINGS
    )
    stream_consumer.start()

    messages_consumed = 0

    async def print_messages_consumed():
        with config.Timer() as timer:
            while True:
                await asyncio.sleep(1)
                print(f"{messages_consumed} consumed "
                      f"in {timer.elapsed_time} seconds")

    print("Starting to consume the messages.")
    print_task = asyncio.ensure_future(print_messages_consumed())
    with config.Timer() as timer:
        async for message in stream_consumer:
            messages_consumed += 1
            if messages_consumed == config.MESSAGE_NUMBER:
                stream_consumer.stop()
    print(f"The time used to consume the messages is {timer.interval} "
          f"seconds.")
    print_throughput(timer.interval)
    print_task.cancel()


async def main_coro():
    await fill_topic_with_messages()
    await consume_the_messages()


if __name__ == "__main__":
    loop.run_until_complete(main_coro())
