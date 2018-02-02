import time
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')
while True:
    producer.send('my_topic', b'some_message_bytes')
    time.sleep(1)
