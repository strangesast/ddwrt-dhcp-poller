import json
import asyncio
from pathlib import Path
from collections import namedtuple
from asyncio.subprocess import PIPE
from aiokafka import AIOKafkaProducer

DHCPEntry = namedtuple('DHCPEntry', ['mac', 'ip', 'expiry', 'hostname'])
EMPTY_MAC = ':'.join(['00'] * 6)


async def retrieve(router_ip, router_username):
    fname = 'leases'
    try:
        args = ['scp', f'{router_username}@{router_ip}:/tmp/udhcpd.leases', fname]
        process = await asyncio.create_subprocess_exec(*args, stderr=PIPE, stdout=PIPE)
        await process.wait()
    except asyncio.CancelledError:
        process.terminate()
        await process.wait()
    
    
    entries = []
    with open(fname, 'rb') as f:
        while (chunk := f.read(88)):
            mac = chunk[:6].hex(':')
            if mac == EMPTY_MAC:
                continue
            ip = '.'.join(map(str, chunk[16:20]))
            expiry = int.from_bytes(chunk[20:24], byteorder='big', signed=False)
            hostname = chunk[24:].decode().strip('\x00')
            entries.append(DHCPEntry(mac, ip, expiry, hostname))

    return entries


async def poll(producer):
    router_username = 'root'
    router_ip = '10.0.0.1'

    entries = await retrieve(router_ip, router_username)
    await producer.send_and_wait('dhcp', json.dumps([d._asdict() for d in entries]).encode(), key=router_ip.encode())


async def main():
    interval = 60
    producer = AIOKafkaProducer(bootstrap_servers='localhost:9092')
    try:
        await producer.start()

        while True:
            await asyncio.gather(poll(producer), asyncio.sleep(interval))
    finally:
        await producer.stop()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
