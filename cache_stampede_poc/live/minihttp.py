"""A minimal asyncio HTTP/1.1 client (one connection per request, Connection: close).

Why not httpx? At 1,000 concurrent connections httpx spent ~5.5 ms of CPU per request in this lab
(raw sockets: ~0.06 ms), which turned a simultaneous burst into a 5-second trickle and hid the
stampede. The lab needs the burst to arrive inside the 200 ms refresh window.
"""
import asyncio
import json
import socket
from urllib.parse import urlsplit

_ips = {}


def _resolve(host):
    if host not in _ips:
        _ips[host] = socket.gethostbyname(host)
    return _ips[host]


async def request(method, url, timeout=30.0):
    u = urlsplit(url)
    path = u.path + (f"?{u.query}" if u.query else "")
    r, w = await asyncio.wait_for(asyncio.open_connection(_resolve(u.hostname), u.port or 80), timeout)
    try:
        w.write(f"{method} {path} HTTP/1.1\r\nHost: {u.hostname}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n".encode())
        await w.drain()
        head = await asyncio.wait_for(r.readuntil(b"\r\n\r\n"), timeout)
        body = await asyncio.wait_for(r.read(), timeout)
        return int(head.split(b" ", 2)[1]), body
    finally:
        w.close()


async def get_json(url):
    status, body = await request("GET", url)
    return status, (json.loads(body) if body else None)


async def post(url):
    return await request("POST", url)
