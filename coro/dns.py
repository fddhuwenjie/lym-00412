import ipaddress
import random
import socket
import struct
from typing import Generator, List, Optional

from .core import Future, TimeoutError, get_event_loop, sleep


_DNS_PORT = 53
_DEFAULT_DNS_SERVER = "8.8.8.8"
_QUERY_TIMEOUT = 5.0
_MAX_RETRIES = 3


def _is_ip_address(host: str) -> bool:
    try:
        ipaddress.ip_address(host)
        return True
    except ValueError:
        return False


def _build_query(domain: str, qtype: int = 1, qclass: int = 1) -> bytes:
    transaction_id = random.randint(0, 0xFFFF)
    flags = 0x0100

    header = struct.pack("!HHHHHH", transaction_id, flags, 1, 0, 0, 0)

    question = b""
    for part in domain.split("."):
        question += struct.pack("!B", len(part)) + part.encode("ascii")
    question += b"\x00"

    question += struct.pack("!HH", qtype, qclass)

    return header + question, transaction_id


def _parse_response(data: bytes, expected_id: int) -> List[str]:
    if len(data) < 12:
        return []

    header = struct.unpack("!HHHHHH", data[:12])
    transaction_id, flags, qdcount, ancount, nscount, arcount = header

    if transaction_id != expected_id:
        return []

    qr = (flags >> 15) & 1
    rcode = flags & 0x0F
    if qr != 1 or rcode != 0:
        return []

    offset = 12

    for _ in range(qdcount):
        while offset < len(data):
            length = data[offset]
            offset += 1
            if length == 0:
                break
            if (length & 0xC0) == 0xC0:
                offset += 1
                break
            offset += length
        offset += 4

    results = []

    for _ in range(ancount):
        while offset < len(data):
            length = data[offset]
            offset += 1
            if length == 0:
                break
            if (length & 0xC0) == 0xC0:
                offset += 1
                break
            offset += length

        if offset + 10 > len(data):
            break

        rtype, rclass, ttl, rdlength = struct.unpack("!HHIH", data[offset:offset + 10])
        offset += 10

        if rtype == 1 and rdlength == 4:
            ip = socket.inet_ntoa(data[offset:offset + 4])
            results.append(ip)

        offset += rdlength

    return results


def _udp_send_recv(
    sock: socket.socket,
    data: bytes,
    server: str,
    port: int,
    timeout: float,
) -> Generator[Future, None, bytes]:
    loop = get_event_loop()
    fd = sock.fileno()

    while True:
        send_future = loop.create_future()

        def on_writable():
            if not send_future.done():
                try:
                    sock.sendto(data, (server, port))
                    send_future.set_result(True)
                except BlockingIOError:
                    loop.add_writer(fd, on_writable)
                except OSError as e:
                    send_future.set_exception(e)

        loop.add_writer(fd, on_writable)
        try:
            sent = yield send_future
        finally:
            loop.remove_writer(fd)
        if sent:
            break

    recv_future = loop.create_future()

    def on_readable():
        if not recv_future.done():
            try:
                resp, _ = sock.recvfrom(512)
                recv_future.set_result(resp)
            except BlockingIOError:
                loop.add_reader(fd, on_readable)
            except OSError as e:
                recv_future.set_exception(e)

    timer_handle = loop.call_later(timeout, lambda: (
        recv_future.done() or recv_future.set_exception(TimeoutError("DNS query timed out"))
    ))

    loop.add_reader(fd, on_readable)
    try:
        response = yield recv_future
    finally:
        loop.remove_reader(fd)
        timer_handle.cancel()

    return response


def resolve(
    domain: str,
    dns_server: Optional[str] = None,
    timeout: float = _QUERY_TIMEOUT,
) -> Generator[Future, None, List[str]]:
    if _is_ip_address(domain):
        return [domain]

    if dns_server is None:
        dns_server = _DEFAULT_DNS_SERVER

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setblocking(False)

    try:
        query_data, tx_id = _build_query(domain)

        for attempt in range(_MAX_RETRIES):
            try:
                response = yield from _udp_send_recv(
                    sock, query_data, dns_server, _DNS_PORT, timeout
                )
                ips = _parse_response(response, tx_id)
                if ips:
                    return ips
            except TimeoutError:
                if attempt == _MAX_RETRIES - 1:
                    raise
                continue
            except OSError:
                if attempt == _MAX_RETRIES - 1:
                    raise
                yield from sleep(0.1)
                continue

        return []
    finally:
        sock.close()
