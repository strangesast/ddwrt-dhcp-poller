import asyncio
from pathlib import Path
from aiokafka import AIOKafkaProducer
from collections import namedtuple
from asyncio.subprocess import PIPE

DHCPEntry = namedtuple('DHCPEntry', ['mac', 'ip', 'expiry', 'hostname'])

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
            if mac == ':'.join(['00'] * 6):
                continue
            ip = list(chunk[16:20])
            expiry = int.from_bytes(chunk[20:24], byteorder='big', signed=False)
            hostname = chunk[24:].decode().strip('\x00')
            entries.append(DHCPEntry(mac, ip, expiry, hostname))

    return entries


async def main():
    interval = 1
    router_username = 'root'
    router_ip = '10.0.0.1'

    while True:
        res, _ = await asyncio.gather(
                retrieve(router_ip, router_username),
                asyncio.sleep(interval))

        pprint(res)
        break


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
