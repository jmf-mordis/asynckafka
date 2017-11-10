import argparse
import os

import asyncio
import signal
from collections import Counter

from .config import *

message = bytes(os.urandom(BYTES_PER_MESSAGE))
MessageGenerator = (message for _ in range(NUMBER_OF_MESSAGES))


class Benchmark:

    def __init__(self, args):
        self._num = args.num
        self._size = args.size
        self._topic = args.topic
        self._producer_kwargs = dict(
            linger_ms=args.linger_ms,
            max_batch_size=args.batch_size,
            bootstrap_servers=args.broker_list,
        )
        self._partition = args.partition
        self._stats_interval = 0.1
        self._stats = [Counter()]
        self.payload = bytes(b"m" * self._size)
        self.loop = asyncio.get_event_loop()
        self.reporter_task = None

    async def _stats_report(self, start):
        loop = asyncio.get_event_loop()
        interval = self._stats_interval
        i = 1
        try:
            while True:
                await asyncio.sleep(
                    (start + i * interval) - loop.time())
                stats = self._stats[-1]
                self._stats.append(Counter())
                i += 1
                print(
                    "Produced {stats[count]} messages in {interval} second(s)."
                        .format(stats=stats, interval=interval)
                )
        except asyncio.CancelledError:
            stats = sum(self._stats, Counter())
            total_time = loop.time() - start
            print(
                "Total produced {stats[count]} messages in "
                "{time:.2f} second(s). Avg {avg} m/s".format(
                    stats=stats,
                    time=total_time,
                    avg=stats['count'] // total_time
                )
            )

    @staticmethod
    def parse_args():
        parser = argparse.ArgumentParser(
            description='Benchmark for maximum throughput to broker on produce')
        parser.add_argument(
            '-b', '--broker-list', default="localhost:9092",
            help='List of bootstrap servers. Default {default}.')
        parser.add_argument(
            '-n', '--num', type=int, default=100000,
            help='Number of messages to send. Default {default}.')
        parser.add_argument(
            '-s', '--size', type=int, default=100,
            help='Size of message payload in bytes. Default {default}.')
        parser.add_argument(
            '--batch-size', type=int, default=16384,
            help='`max_batch_size` attr of Producer. Default {default}.')
        parser.add_argument(
            '--linger-ms', type=int, default=5,
            help='`linger_ms` attr of Producer. Default {default}.')
        parser.add_argument(
            '--topic', default="test",
            help='Topic to produce messages to. Default {default}.')
        parser.add_argument(
            '--partition', type=int, default=0,
            help='Partition to produce messages to. Default {default}.')
        parser.add_argument(
            '--uvloop', action='store_true',
            help='Use uvloop instead of asyncio default loop.')
        return parser.parse_args()

    @classmethod
    def initialize_benchmark(Cls):
        args = Cls.parse_args()
        if args.uvloop:
            import uvloop
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

        loop = asyncio.get_event_loop()
        task = loop.create_task(Cls(args).bench_simple())
        task.add_done_callback(lambda _, loop=loop: loop.stop())

        def signal_hndl(_task=task):
            _task.cancel()
        loop.add_signal_handler(signal.SIGTERM, signal_hndl)
        loop.add_signal_handler(signal.SIGINT, signal_hndl)

        try:
            loop.run_forever()
        finally:
            loop.close()
            if not task.cancelled():
                task.result()

    async def __aenter__(self):
        self.reporter_task = self.loop.create_task(self._stats_report(self.loop.time()))

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.reporter_task.cancel()
        await self.reporter_task
