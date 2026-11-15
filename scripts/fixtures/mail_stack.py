"""Private protocol fixture: strict PROXY v2, SMTP/IMAP STARTTLS and IMAP IDLE.

No authentication/mail storage: this verifies edge transport behavior, not a mail
server implementation. Only used on an isolated Docker network by the soak.
"""
import asyncio
import json
import os
from pathlib import Path
import socket
import ssl
import struct

STATS = {"proxy": 0, "local": 0, "invalid": 0, "smtp": 0, "imap": 0, "starttls": 0}
ROOT = Path("/certificates/edge/current")


async def header(reader):
    data = await asyncio.wait_for(reader.readexactly(16), 5)
    assert data[:12] == b"\r\n\r\n\x00\r\nQUIT\n", "PROXY signature"
    size = struct.unpack("!H", data[14:])[0]
    if data[12:] == b"\x20\x00\x00\x00":
        STATS["local"] += 1
        return "LOCAL"
    assert data[12] == 0x21 and (data[13], size) in ((0x11, 12), (0x21, 36)), "PROXY command/family/length/TLV"
    payload = await reader.readexactly(size)
    STATS["proxy"] += 1
    length, family = (4, socket.AF_INET) if size == 12 else (16, socket.AF_INET6)
    return socket.inet_ntop(family, payload[:length])


def tls_context():
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(ROOT / "fullchain.pem", ROOT / "privkey.pem")
    return context


async def serve(reader, writer, protocol):
    try:
        source = await header(reader)
        if protocol == "http":
            request = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), 10)
            path = request.split(b" ")[1]
            body = json.dumps(STATS if path == b"/stats" else {"source": source}).encode()
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body)
            await writer.drain()
            return
        writer.write(b"220 mail.test ESMTP fixture\r\n" if protocol == "smtp" else b"* OK IMAP4rev1 fixture\r\n")
        await writer.drain()
        encrypted = False
        while line := await reader.readline():
            fields = line.decode("ascii").strip().split()
            if not fields:
                continue
            tag, command = ("", fields[0].upper()) if protocol == "smtp" else (fields[0], fields[1].upper())
            if protocol == "smtp":
                if command == "EHLO":
                    reply = f"250-mail.test\r\n250-X-CLIENT-IP {source}\r\n250 STARTTLS\r\n".encode()
                elif command == "STARTTLS" and not encrypted:
                    reply = b"220 Ready to start TLS\r\n"
                elif command == "QUIT":
                    reply = b"221 Bye\r\n"
                else:
                    reply = b"250 OK\r\n"
            else:
                if command == "STARTTLS" and not encrypted:
                    reply = f"{tag} OK Begin TLS\r\n".encode()
                elif command == "IDLE":
                    writer.write(b"+ idling\r\n")
                    await writer.drain()
                    assert (await reader.readline()).strip().upper() == b"DONE"
                    reply = f"{tag} OK IDLE terminated\r\n".encode()
                elif command == "CAPABILITY":
                    reply = f"* CAPABILITY IMAP4rev1 STARTTLS IDLE\r\n{tag} OK CAPABILITY\r\n".encode()
                else:
                    reply = f"{tag} OK {source}\r\n".encode()
            writer.write(reply)
            await writer.drain()
            STATS[protocol] += 1
            if command == "STARTTLS" and not encrypted:
                await writer.start_tls(tls_context(), ssl_handshake_timeout=5)
                encrypted = True
                STATS["starttls"] += 1
            if command in ("QUIT", "LOGOUT"):
                break
    except (ConnectionError, asyncio.IncompleteReadError, asyncio.TimeoutError, ssl.SSLError):
        pass  # TCP health checks can close immediately after the LOCAL header.
    except Exception as error:
        STATS["invalid"] += 1
        print(json.dumps({"error": str(error), "protocol": protocol}), flush=True)
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except (ConnectionError, ssl.SSLError):
            pass


async def main():
    servers = []
    # Private fixture: the harness never publishes these backend ports.
    for port, protocol in ((1025, "smtp"), (1587, "smtp"), (1143, "imap"), (8080, "http")):
        servers.append(await asyncio.start_server(lambda r, w, p=protocol: serve(r, w, p), "0.0.0.0", port, limit=16384))
    await asyncio.gather(*(server.serve_forever() for server in servers))


if __name__ == "__main__":
    asyncio.run(main())
