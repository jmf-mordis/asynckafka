import uuid

import os

import time

MESSAGE_BYTES = 200
MESSAGE_NUMBER = 10_000_000
MESSAGE = os.urandom(MESSAGE_BYTES)

KAFKA_IP = '127.0.0.1'
KAFKA_PORT = 9092
KAFKA_URL = f'{KAFKA_IP}:{KAFKA_PORT}'

TOPIC = 'benchmark_' + str(uuid.uuid4())

RDK_PRODUCER_CONFIG = {
    'queue.buffering.max.messages': '1000000',
    'queue.buffering.max.ms': '1000',
    'batch.num.messages': '1000000'
}

RDK_CONSUMER_CONFIG = {
    'queue.buffering.max.messages': '1000000',
    'queue.buffering.max.ms': '1000',
    'batch.num.messages': '1000000'
}

RDK_TOPIC_CONFIG = {
    'auto.offset.reset':  'smallest'
}


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
