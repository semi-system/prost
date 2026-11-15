"""Isolated DNS-01 adapter fixture: authenticated HTTPS changes + real UDP/TCP DNS.

The API adds/removes one TXT value, preserving concurrent ACME authorizations.
Never included in the production image or used as an Internet DNS service.
"""
import asyncio
import hmac
import ipaddress
import json
from pathlib import Path
import re
import socket
import ssl
import struct

RECORDS = {}
TOKEN = Path("/cases/reload-token").read_text().strip()


def encode(name):
    return b"".join(bytes([len(label)]) + label.encode("ascii") for label in name.rstrip(".").split(".")) + b"\0"


def answer(data):
    if len(data) < 12 or len(data) > 4096 or data[4:6] != b"\0\1":
        return b""
    position, labels = 12, []
    while position < len(data):
        length = data[position]
        position += 1
        if not length:
            break
        if length > 63 or position + length > len(data) or len(labels) > 127:
            return b""
        labels.append(data[position:position + length].decode("ascii"))
        position += length
    if position + 4 > len(data):
        return b""
    name = ".".join(labels).lower()
    kind, klass = struct.unpack("!HH", data[position:position + 4])
    question = data[12:position + 4]
    answers = []
    if klass == 1 and kind == 16:
        answers = [bytes([len(value)]) + value.encode("ascii") for value in sorted(RECORDS.get(name, set()))]
    elif klass == 1 and kind == 1 and name in {"mail.test", "admin.test", "mail-backend", "admin-backend", "backend.test", "dns.test", "target.test"}:
        host = "mail-backend" if name == "target.test" else name
        answers = [ipaddress.ip_address(socket.gethostbyname(host)).packed]
    elif klass == 1 and kind == 33 and name == "_smtp._tcp.mail.test":
        answers = [struct.pack("!HHH", 10, 3, 1025) + encode("mail-backend")]
    result = data[:2] + struct.pack("!HHHHH", 0x8180, 1, len(answers), 0, 0) + question
    for value in answers:
        result += b"\xc0\x0c" + struct.pack("!HHIH", kind, klass, 1, len(value)) + value
    return result


class DNS(asyncio.DatagramProtocol):
    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, peer):
        try:
            result = answer(data)
            if result:
                self.transport.sendto(result, peer)
        except (ValueError, UnicodeError, OSError):
            pass


async def tcp(reader, writer):
    try:
        async with asyncio.timeout(5):
            size = struct.unpack("!H", await reader.readexactly(2))[0]
            if size > 4096:
                return
            reply = answer(await reader.readexactly(size))
            writer.write(struct.pack("!H", len(reply)) + reply)
            await writer.drain()
    except (ValueError, OSError, EOFError, asyncio.TimeoutError, asyncio.IncompleteReadError):
        pass
    finally:
        writer.close()


async def api(reader, writer):
    try:
        async with asyncio.timeout(10):
            data = await reader.readuntil(b"\r\n\r\n")
            lines = data.decode("ascii").split("\r\n")
            method, path, _ = lines[0].split(" ")
            headers = dict((key.lower(), value.strip()) for key, value in (line.split(":", 1) for line in lines[1:] if ":" in line))
            assert hmac.compare_digest(headers.get("authorization", ""), "Bearer " + TOKEN)
            size = int(headers.get("content-length", "0"))
            assert method == "POST" and path in ("/present", "/cleanup") and 0 < size <= 4096
            value = json.loads(await reader.readexactly(size))["record"]
            name, content = value["name"].lower().rstrip("."), value["value"]
            assert name.startswith("_acme-challenge.") and len(name) <= 253 and re.fullmatch(r"[a-z0-9_.-]+", name)
            assert re.fullmatch(r"[A-Za-z0-9_-]{1,128}", content)
            assert len(RECORDS) < 256 or name in RECORDS
            values = RECORDS.setdefault(name, set())
            if path == "/present":
                assert len(values) < 100 or content in values
                values.add(content)
            else:
                values.discard(content)
                if not values:
                    RECORDS.pop(name, None)
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
            await writer.drain()
    except (AssertionError, ValueError, KeyError, OSError, asyncio.TimeoutError, asyncio.IncompleteReadError):
        pass
    finally:
        writer.close()


async def main():
    loop = asyncio.get_running_loop()
    # All listeners belong exclusively to the harness's private Docker network.
    await loop.create_datagram_endpoint(DNS, local_addr=("0.0.0.0", 8053))
    dns = await asyncio.start_server(tcp, "0.0.0.0", 8053, limit=4096)
    tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    tls.minimum_version = ssl.TLSVersion.TLSv1_2
    tls.load_cert_chain("/cases/api.pem", "/cases/api.key")
    https = await asyncio.start_server(api, "0.0.0.0", 8888, ssl=tls, ssl_handshake_timeout=5, limit=16384)
    await asyncio.gather(dns.serve_forever(), https.serve_forever())


if __name__ == "__main__":
    asyncio.run(main())
