import json
from datetime import datetime
from kafka import KafkaConsumer

consumer = KafkaConsumer('dhcp', auto_offset_reset='earliest')
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
