import json
from itertools import islice
from datetime import datetime
from kafka import KafkaConsumer, TopicPartition

topic = "dhcp"

consumer = KafkaConsumer(auto_offset_reset="earliest")

offsets = consumer.end_offsets(
    [
        TopicPartition(topic, partition)
        for partition in consumer.partitions_for_topic(topic)
    ]
)

tp = next(iter(offsets))

last_offset = offsets[tp]


consumer.assign([tp])
consumer.seek_to_beginning(tp)

hostnames = {}
for msg in islice(consumer, last_offset):
    k, v = msg.key.decode(), json.loads(msg.value)
    hostnames[k] = (v["ip"], v["hostname"])

consumer.unsubscribe()

consumer.subscribe(topics=["rssi"])
for msg in consumer:
    k, v = msg.key.decode(), json.loads(msg.value)
    dt = datetime.fromtimestamp(msg.timestamp / 1000.0)
    print(
        f"{dt:%Y-%m-%d %H:%M:%S}",
        (f"{hn[0]} ({hn[1]})" if (hn := hostnames.get(k)) else k).ljust(28),
        v["ap"],
        v["iface"],
        v["rssi"],
    )

"""
macs = set()
hostnames = dict()
for msg in consumer:
    dt = datetime.fromtimestamp(msg.timestamp / 1000.0)
    v = json.loads(msg.value)
    loop = set()
    for r in v:
        if r['expiry'] != 0:
            mac, hostname = r['mac'], r['hostname']
            loop.add(mac)
            if mac not in macs:
                macs.add(mac)
                hostnames[mac] = hostname
                print(f'{dt} added {mac} ({hostname})')

    for mac in list(macs):
        if mac not in loop:
            hostname = hostnames.get(mac) or ''
            print(f'{dt} removed {mac} ({hostname})')
            macs.remove(mac)
"""
