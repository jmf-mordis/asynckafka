import asyncio
from confluent_kafka import Producer, Consumer, KafkaError
from benchmarks.bench_utils.config import KAFKA_URL, TOPIC
from benchmarks.bench_utils import MessageGenerator, Benchmark


class ConfluentBenchmark(Benchmark):

    async def bench_simple(self):
        c = Consumer({'bootstrap.servers': KAFKA_URL})
        c.subscribe(self._topic)

        reporter_task = self.loop.create_task(self._stats_report(self.loop.time()))
        for _ in range(self._num):
            msg = c.poll()
            if not msg.error():
                self._stats[-1]['count'] += 1
                await asyncio.sleep(0)
            elif msg.error().code() != KafkaError._PARTITION_EOF:
                print("ERROR!!")
                break
        c.close()

        reporter_task.cancel()
        await reporter_task


if __name__ == "__main__":
    ConfluentBenchmark.initialize_benchmark()