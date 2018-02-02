from kafka import KafkaConsumer

# To consume latest messages and auto-commit offsets
topic = "my_topic"
print(topic)
consumer = KafkaConsumer(topic,
                         group_id='my-group',
                         bootstrap_servers=['localhost:9092'])
for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))
