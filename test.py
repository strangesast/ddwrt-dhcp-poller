from subprocess import run
import asyncio
from collections import defaultdict
from asyncio.subprocess import PIPE

from pprint import pprint

async def main():
    router_user = 'root'
    router_ip = '10.0.0.1'

    dhcp_clients = await retrieve_dhcp_leases(router_ip, router_user)

    for router_ip in ['10.0.0.1', '10.0.0.2']:
        wclients = await retrieve_wclients(router_ip, router_user)
        ip_conntrack = await retrieve_ip_conntrack(router_ip, router_user)

    ip_conntrack
    for c in wclients:
        print(c['mac'], c, dd.get(c['mac']), d[c['mac']])


if __name__ == '__main__':
    asyncio.run(main())
