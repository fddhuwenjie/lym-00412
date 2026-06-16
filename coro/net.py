import os
import socket
import threading
from typing import Any, Generator, Optional, Tuple

from .core import (
    CancelledError,
    Future,
    TimeoutError,
    get_event_loop,
)


def _set_nonblocking(sock: socket.socket) -> None:
    sock.setblocking(False)


def tcp_connect(
    host: str,
    port: int,
    timeout: Optional[float] = None,
) -> Generator[Future, None, socket.socket]:
    loop = get_event_loop()
    addrinfo = yield from _resolve_addr(host, port)
    if not addrinfo:
        raise OSError(f"Could not resolve {host}")

    last_error = None
    for family, socktype, proto, _, sockaddr in addrinfo:
        sock = None
        try:
            sock = socket.socket(family, socktype, proto)
            _set_nonblocking(sock)
            try:
                sock.connect(sockaddr)
            except BlockingIOError:
                pass

            future = loop.create_future()
            fd = sock.fileno()

            def make_on_writable(s, fut):
                def on_writable():
                    if not fut.done():
                        err = s.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
                        if err == 0:
                            fut.set_result(None)
                        else:
                            fut.set_exception(OSError(err, f"Connect failed: {os.strerror(err)}"))
                return on_writable

            on_writable = make_on_writable(sock, future)
            loop.add_writer(fd, on_writable)

            try:
                if timeout is not None:
                    yield from _with_timeout(future, timeout)
                else:
                    yield future
            finally:
                loop.remove_writer(fd)

            return sock
        except (CancelledError, TimeoutError):
            if sock:
                sock.close()
            raise
        except OSError as e:
            if sock:
                sock.close()
            last_error = e
            continue

    if last_error:
        raise last_error
    raise OSError("Could not connect")


def _resolve_addr(host: str, port: int) -> Generator[Future, None, list]:
    loop = get_event_loop()
    future = loop.create_future()

    def resolve_thread():
        try:
            result = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
            loop.call_soon(lambda: future.set_result(result))
        except Exception as e:
            loop.call_soon(lambda: future.set_exception(e))

    thread = threading.Thread(target=resolve_thread, daemon=True)
    thread.start()
    result = yield future
    return result


def tcp_accept(
    sock: socket.socket,
    timeout: Optional[float] = None,
) -> Generator[Future, None, Tuple[socket.socket, Tuple[str, int]]]:
    loop = get_event_loop()
    future = loop.create_future()
    fd = sock.fileno()

    def on_readable():
        if not future.done():
            try:
                client_sock, addr = sock.accept()
                _set_nonblocking(client_sock)
                future.set_result((client_sock, addr))
            except BlockingIOError:
                loop.add_reader(fd, on_readable)
            except OSError as e:
                future.set_exception(e)

    loop.add_reader(fd, on_readable)

    try:
        if timeout is not None:
            result = yield from _with_timeout_result(future, timeout)
        else:
            result = yield future
    finally:
        loop.remove_reader(fd)

    return result


def tcp_read(
    sock: socket.socket,
    nbytes: int,
    timeout: Optional[float] = None,
) -> Generator[Future, None, bytes]:
    loop = get_event_loop()
    future = loop.create_future()
    fd = sock.fileno()

    def on_readable():
        if not future.done():
            try:
                data = sock.recv(nbytes)
                future.set_result(data)
            except BlockingIOError:
                loop.add_reader(fd, on_readable)
            except OSError as e:
                future.set_exception(e)

    loop.add_reader(fd, on_readable)

    try:
        if timeout is not None:
            result = yield from _with_timeout_result(future, timeout)
        else:
            result = yield future
    finally:
        loop.remove_reader(fd)

    return result


def tcp_read_all(
    sock: socket.socket,
    timeout: Optional[float] = None,
) -> Generator[Future, None, bytes]:
    chunks = []
    while True:
        chunk = yield from tcp_read(sock, 4096, timeout)
        if not chunk:
            break
        chunks.append(chunk)
    return b"".join(chunks)


def tcp_write(
    sock: socket.socket,
    data: bytes,
    timeout: Optional[float] = None,
) -> Generator[Future, None, int]:
    loop = get_event_loop()
    total_written = 0
    data_view = memoryview(data)

    while total_written < len(data_view):
        future = loop.create_future()
        fd = sock.fileno()
        current_total = total_written

        def make_on_writable(tv, offset, fut):
            def on_writable():
                if not fut.done():
                    try:
                        n = sock.send(tv[offset:])
                        fut.set_result(n)
                    except BlockingIOError:
                        loop.add_writer(fd, on_writable)
                    except OSError as e:
                        fut.set_exception(e)
            return on_writable

        on_writable = make_on_writable(data_view, current_total, future)
        loop.add_writer(fd, on_writable)

        try:
            if timeout is not None:
                n = yield from _with_timeout_result(future, timeout)
            else:
                n = yield future
        finally:
            loop.remove_writer(fd)

        total_written += n

    return total_written


def _with_timeout(future: Future, timeout: float) -> Generator[Future, None, None]:
    loop = get_event_loop()
    done = loop.create_future()

    timer_handle = None
    timed_out = False

    def on_timer():
        nonlocal timed_out
        timed_out = True
        if not done.done():
            future.cancel()
            done.set_exception(TimeoutError(f"Operation timed out after {timeout}s"))

    timer_handle = loop.call_later(timeout, on_timer)

    def on_future_done(f):
        if timer_handle:
            timer_handle.cancel()
        if not done.done():
            if f.cancelled():
                if timed_out:
                    return
                done.cancel()
            elif f.exception() is not None:
                done.set_exception(f.exception())
            else:
                done.set_result(None)

    future.add_done_callback(on_future_done)

    try:
        yield done
    finally:
        future.remove_done_callback(on_future_done)
        if timer_handle:
            timer_handle.cancel()


def _with_timeout_result(future: Future, timeout: float) -> Generator[Future, None, Any]:
    loop = get_event_loop()
    done = loop.create_future()

    timer_handle = None
    timed_out = False

    def on_timer():
        nonlocal timed_out
        timed_out = True
        if not done.done():
            future.cancel()
            done.set_exception(TimeoutError(f"Operation timed out after {timeout}s"))

    timer_handle = loop.call_later(timeout, on_timer)

    def on_future_done(f):
        if timer_handle:
            timer_handle.cancel()
        if not done.done():
            if f.cancelled():
                if timed_out:
                    return
                done.cancel()
            elif f.exception() is not None:
                done.set_exception(f.exception())
            else:
                done.set_result(f.result())

    future.add_done_callback(on_future_done)

    try:
        result = yield done
    finally:
        future.remove_done_callback(on_future_done)
        if timer_handle:
            timer_handle.cancel()

    return result


class SocketStream:
    def __init__(self, sock: socket.socket):
        self._sock = sock

    @classmethod
    def connect(
        cls, host: str, port: int, timeout: Optional[float] = None
    ) -> Generator[Future, None, "SocketStream"]:
        sock = yield from tcp_connect(host, port, timeout)
        return cls(sock)

    def read(self, nbytes: int, timeout: Optional[float] = None) -> Generator[Future, None, bytes]:
        return tcp_read(self._sock, nbytes, timeout)

    def read_all(self, timeout: Optional[float] = None) -> Generator[Future, None, bytes]:
        return tcp_read_all(self._sock, timeout)

    def write(self, data: bytes, timeout: Optional[float] = None) -> Generator[Future, None, int]:
        return tcp_write(self._sock, data, timeout)

    def close(self) -> None:
        self._sock.close()

    @property
    def socket(self) -> socket.socket:
        return self._sock


def tcp_server(
    host: str,
    port: int,
    handler,
    backlog: int = 128,
) -> Generator[Future, None, socket.socket]:
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((host, port))
    server_sock.listen(backlog)
    _set_nonblocking(server_sock)

    loop = get_event_loop()

    def accept_loop():
        def accept_next():
            try:
                while True:
                    try:
                        client_sock, addr = server_sock.accept()
                        _set_nonblocking(client_sock)
                        loop.create_task(handler(client_sock, addr))
                    except BlockingIOError:
                        break
            except OSError:
                pass
            loop.add_reader(server_sock.fileno(), accept_next)

        loop.add_reader(server_sock.fileno(), accept_next)

    accept_loop()
    yield loop.create_future()
    return server_sock
