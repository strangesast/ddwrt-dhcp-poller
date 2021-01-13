import json
from itertools import islice
from kafka import KafkaConsumer, TopicPartition

from pprint import pprint


topic = 'dhcp'
consumer = KafkaConsumer(topic, bootstrap_servers='localhost:9092')

partition = next(iter(consumer.partitions_for_topic(topic)))
partition = TopicPartition(topic, partition)
position = consumer.position(partition)
consumer.seek(partition, position - 1)

try:
    for msg in islice(consumer, 1):
        v = json.loads(msg.value)
        pprint(v)

except KeyboardInterrupt:
    pass
