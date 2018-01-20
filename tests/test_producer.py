import time

from producer import Producer


producer = Producer(brokers="127.0.0.1", topic="test2")
[producer.produce(b"testing_message") for _ in range(10)]
time.sleep(10)