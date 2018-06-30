import time

import config


class Timer:

    @property
    def elapsed_time(self):
        return time.time() - self.start

    def __enter__(self):
        self.start = time.time()
        
        return self

    def __exit__(self, *args):
        self.end = time.time()
        self.interval = self.end - self.start


def print_statistics(time_interval):
    megabytes_per_second = (
        config.MESSAGE_NUMBER * config.MESSAGE_BYTES) / time_interval / 1e6
    print(f"Throughput: {megabytes_per_second} mb/s ")
    print(f"Messages per second: {config.MESSAGE_NUMBER/time_interval}")
