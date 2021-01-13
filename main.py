import json
import asyncio
import random
from pathlib import Path
from contextlib import suppress
from asyncio.subprocess import PIPE
from aiokafka import AIOKafkaProducer

from pprint import pprint

EMPTY_MAC = ":".join(["00"] * 6)

DHCP_TOPIC = "dhcp"
RSSI_TOPIC = "rssi"
IP_CONNTRACK_TOPIC = "ip_conntrack"

SSH_KEY_ONLY = ["-oPasswordAuthentication=no", "-oPreferredAuthentications=publickey"]


async def run_command(args):
    return await asyncio.create_subprocess_exec(*args, stderr=PIPE, stdout=PIPE)


def grouper(b, n):
    for i in range(0, len(b), n):
        yield b[i : i + n]


class SSHException(Exception):
    pass


last_dhcp_leases = {}


async def retrieve_dhcp_leases(router_ip, router_user):
    try:
        args = [
            "ssh",
            *SSH_KEY_ONLY,
            f"{router_user}@{router_ip}",
            "cat",
            "/tmp/udhcpd.leases",
        ]
        process = await run_command(args)
        stdout, stderr = await process.communicate()
    except asyncio.CancelledError:
        process.terminate()
        await process.wait()

    entries = []
    for chunk in grouper(stdout, 88):
        mac = chunk[:6].hex(":")
        if mac == EMPTY_MAC:
            continue
        entry = {
            "mac": mac.lower(),
            "ip": ".".join(map(str, chunk[16:20])),
            "expiry": int.from_bytes(chunk[20:24], byteorder="big", signed=True),
            "hostname": chunk[24:].decode().strip("\x00"),
        }
        entries.append(entry)

    return entries


async def retrieve_wclients(router_ip, router_user):
    cpath = "/tmp/root/script.sh"
    args = ["ssh", *SSH_KEY_ONLY, f"{router_user}@{router_ip}", "test", "-e", cpath]
    process = await run_command(args)
    retcode = await process.wait()
    if retcode:
        args = ["scp", *SSH_KEY_ONLY, "script.sh", f"{router_user}@{router_ip}:{cpath}"]
        process = await run_command(args)
        await process.wait()
    args = ["ssh", *SSH_KEY_ONLY, f"{router_user}@{router_ip}", "sh", cpath]
    process = await run_command(args)
    stdout, stderr = await process.communicate()

    if process.returncode:
        raise SSHException("wclients failed")

    stdout = stdout.decode().rstrip()

    entries = []
    if stdout:
        for line in stdout.split("\n"):
            iface, mac, rssi = line.split()
            entries.append(
                {"mac": mac.lower(), "ap": router_ip, "iface": iface, "rssi": int(rssi)}
            )

    return entries


async def retrieve_ip_conntrack(router_ip, router_user):
    args = ["ssh", f"{router_user}@{router_ip}", "cat", "/proc/net/ip_conntrack"]
    process = await run_command(args)
    stdout, stderr = await process.communicate()
    if process.returncode:
        raise SSHException("ip_conntrack failed")
    stdout = stdout.decode().rstrip()

    # if stdout:
    #    d = defaultdict(set)
    #    records = []
    #    for line in stdout.split('\n'):
    #        s = line.split()
    #        i = 4 if s[0] == 'tcp' else 3
    #        print(s)
    #        for k, v in (ss.split("=") for ss in s[i:] if "=" in ss):
    #            if k == 'src' or k == 'dst':
    #                pass
    return {"ap": router_ip, "file": stdout}


async def poll(producer):
    router_user = "root"
    router_ip = "10.0.0.1"

    with suppress(SSHException):
        batch = producer.create_batch()
        entries = await retrieve_dhcp_leases(router_ip, router_user)
        for entry in entries:
            if last_dhcp_leases.get(entry["mac"]) != entry:
                last_dhcp_leases[entry["mac"]] = entry
                batch.append(
                    key=entry["mac"].encode(),
                    value=json.dumps(entry).encode(),
                    timestamp=None,
                )
        partitions = await producer.partitions_for(DHCP_TOPIC)
        partition = random.choice(tuple(partitions))
        await producer.send_batch(batch, DHCP_TOPIC, partition=partition)

    for router_ip in ["10.0.0.1", "10.0.0.2"]:
        with suppress(SSHException):
            batch = producer.create_batch()
            entries = await retrieve_wclients(router_ip, router_user)
            for entry in entries:
                batch.append(
                    key=entry["mac"].encode(),
                    value=json.dumps(entry).encode(),
                    timestamp=None,
                )
            partitions = await producer.partitions_for(RSSI_TOPIC)
            partition = random.choice(tuple(partitions))
            await producer.send_batch(batch, RSSI_TOPIC, partition=partition)

            ip_conntrack = await retrieve_ip_conntrack(router_ip, router_user)
            await producer.send_and_wait(
                IP_CONNTRACK_TOPIC,
                json.dumps(ip_conntrack).encode(),
                key=ip_conntrack["ap"].encode(),
            )


async def main():
    interval = 60
    producer = AIOKafkaProducer(bootstrap_servers="localhost:9092")
    try:
        await producer.start()

        while True:
            await asyncio.gather(poll(producer), asyncio.sleep(interval))
    finally:
        await producer.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
