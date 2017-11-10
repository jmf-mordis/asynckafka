import asyncio
from .settings import DEFAULT_MAX_COROUTINES


class KafkaManager:

    def __init__(self, bootstrap_servers, loop=None, on_close=None, max_coroutines=DEFAULT_MAX_COROUTINES, *kwargs):
        self.loop = loop if loop else asyncio.get_event_loop()
        self.bootstrap_servers = bootstrap_servers
        self.on_close = on_close
        self.consumers = {}
        self.producers = {}
        self.connected = False
        self._kwargs = kwargs

    def create_consumer(self):
        pass

    def create_producer(self):
        pass

    async def connect(self):
        pass

    async def close(self):
        pass


class KafkaConsumer:

    def __init__(self, topic, group_id, partition=None):
        self.connected = False
        self.topic = topic
        self.group_id = group_id

    async def connect(self):
        pass

    async def close(self):
        pass

    async def commit(self):
        pass

    async def __aiter__(self):
        pass

    async def __anext__(self):
        pass


class KafkaProducer:

    def __init__(self, topic, key=None, partition=None, timestamp=None):
        self.topic = topic

    def send(self):
        pass

    async def connect(selfiifdsfiisdfisdfdsffdsfsdfdsfsd):
        pass

    async def close(self):
        pass

    async def flush(self):
        pass
