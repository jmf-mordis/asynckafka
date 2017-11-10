import asyncio

from aiokafka import AIOKafkaProducer

from benchmarks.bench_utils import Benchmark


class AioKafkaBenchmark(Benchmark):

    async def bench_simple(self):
        producer = AIOKafkaProducer(loop=self.loop, **self._producer_kwargs)
        await producer.start()

        # We start from after producer connect
        try:
            for i in range(self._num):
                # payload[i % self._size] = random.randint(0, 255)
                await producer.send(self._topic, self.payload, partition=self._partition)
                self._stats[-1]['count'] += 1
        except asyncio.CancelledError:
            pass
        finally:
            await producer.stop()
            reporter_task.cancel()
            await reporter_task


if __name__ == "__main__":
    AioKafkaBenchmark.initialize_benchmark()
