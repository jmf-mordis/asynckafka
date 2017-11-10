import asyncio
from confluent_kafka import Producer
from benchmarks.bench_utils.config import KAFKA_URL, TOPIC
from benchmarks.bench_utils import MessageGenerator, Benchmark


class ConfluentBenchmark(Benchmark):

    async def bench_simple(self):
        producer = Producer({'bootstrap.servers': KAFKA_URL})

        reporter_task = self.loop.create_task(self._stats_report(self.loop.time()))

        for _ in range(self._num):
            await async_produce(producer, self._topic, self.payload)
            await asyncio.sleep(0)
            self._stats[-1]['count'] += 1

        reporter_task.cancel()
        await reporter_task

        producer.flush()


async def async_produce(producer, topic, payload):
    producer.produce(topic=topic, key=payload)



async def none():
    return None


if __name__ == "__main__":
    ConfluentBenchmark.initialize_benchmark()