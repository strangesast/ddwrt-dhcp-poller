import matplotlib

matplotlib.use("Agg")

import json
from itertools import islice
import matplotlib.pyplot as plt
from collections import defaultdict
from datetime import datetime
from kafka import KafkaConsumer, TopicPartition
from pprint import pprint

consumer = KafkaConsumer(auto_offset_reset="earliest")


def read_whole_topic(topic):
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

    l = []
    for msg in islice(consumer, last_offset):
        k, v = msg.key.decode(), json.loads(msg.value)
        dt = datetime.fromtimestamp(msg.timestamp / 1000.0)
        l.append((k, v, dt))
    consumer.unsubscribe()
    return l


dhcp_msgs = read_whole_topic("dhcp")

hostnames = {}
for k, v, _ in dhcp_msgs:
    hostnames[k] = (v["ip"], v["hostname"])

rssi_msgs = read_whole_topic("rssi")

d = defaultdict(lambda: defaultdict(list))
# macs = set()
for k, v, dt in rssi_msgs:
    d[k][v["ap"]].append((dt, v["rssi"]))
    # macs.add(k)

# for ss in s:
#    print(ss, hostnames.get(ss))

for mac, v in d.items():
    fig = plt.figure(figsize=(12, 8))
    fig.suptitle(hn[1] if (hn := hostnames.get(mac)) else mac, fontsize=16)
    ax = fig.add_subplot(111)
    for vv in v.values():
        x, y = zip(*vv)
        ax.plot(x, y, ".")

    fig.savefig(f"rssi-{mac}.png")

"""
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
