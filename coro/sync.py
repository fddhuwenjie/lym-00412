from collections import deque
from typing import Any, Deque, Generator, List, Optional

from .core import CancelledError, Future, Task, get_event_loop


class Lock:
    def __init__(self):
        self._locked = False
        self._waiters: Deque[Future] = deque()

    def locked(self) -> bool:
        return self._locked

    def acquire(self) -> Generator[Future, None, bool]:
        if not self._locked and not self._waiters:
            self._locked = True
            fut = get_event_loop().create_future()
            fut.set_result(True)
            yield fut
            return True

        loop = get_event_loop()
        future = loop.create_future()
        self._waiters.append(future)
        try:
            yield future
        except CancelledError:
            try:
                self._waiters.remove(future)
            except ValueError:
                pass
            self._wakeup_next()
            raise
        self._locked = True
        return True

    def release(self) -> None:
        if not self._locked:
            raise RuntimeError("Lock is not acquired")
        self._locked = False
        self._wakeup_next()

    def _wakeup_next(self) -> None:
        while self._waiters:
            waiter = self._waiters.popleft()
            if not waiter.done():
                waiter.set_result(True)
                break

    def __iter__(self):
        return self.acquire()

    def __await__(self):
        return self.acquire()

    def __enter__(self):
        raise RuntimeError("Use async with instead")

    def __exit__(self, *args):
        pass


class _LockContextManager:
    def __init__(self, lock: Lock):
        self._lock = lock

    def __iter__(self) -> Generator[Future, None, "_LockContextManager"]:
        yield from self._lock.acquire()
        return self

    def __await__(self) -> Generator[Future, None, "_LockContextManager"]:
        return self.__iter__()

    def __enter__(self) -> "_LockContextManager":
        return self

    def __exit__(self, *args) -> None:
        self._lock.release()


class Semaphore:
    def __init__(self, value: int = 1):
        if value < 0:
            raise ValueError("Semaphore initial value must be >= 0")
        self._value = value
        self._waiters: Deque[Future] = deque()

    def locked(self) -> bool:
        return self._value == 0

    @property
    def value(self) -> int:
        return self._value

    def acquire(self) -> Generator[Future, None, bool]:
        if self._value > 0 and not self._waiters:
            self._value -= 1
            fut = get_event_loop().create_future()
            fut.set_result(True)
            yield fut
            return True

        loop = get_event_loop()
        future = loop.create_future()
        self._waiters.append(future)
        try:
            yield future
        except CancelledError:
            try:
                self._waiters.remove(future)
            except ValueError:
                pass
            self._wakeup_next()
            raise
        self._value -= 1
        return True

    def release(self) -> None:
        self._value += 1
        self._wakeup_next()

    def _wakeup_next(self) -> None:
        while self._waiters:
            waiter = self._waiters.popleft()
            if not waiter.done():
                waiter.set_result(True)
                break

    def __iter__(self):
        return self.acquire()

    def __await__(self):
        return self.acquire()


class Event:
    def __init__(self):
        self._set = False
        self._waiters: List[Future] = []

    def is_set(self) -> bool:
        return self._set

    def set(self) -> None:
        if not self._set:
            self._set = True
            for waiter in self._waiters:
                if not waiter.done():
                    waiter.set_result(None)
            self._waiters.clear()

    def clear(self) -> None:
        self._set = False

    def wait(self) -> Generator[Future, None, bool]:
        if self._set:
            fut = get_event_loop().create_future()
            fut.set_result(True)
            yield fut
            return True

        loop = get_event_loop()
        future = loop.create_future()
        self._waiters.append(future)
        try:
            yield future
        except CancelledError:
            try:
                self._waiters.remove(future)
            except ValueError:
                pass
            raise
        return True

    def __iter__(self):
        return self.wait()

    def __await__(self):
        return self.wait()


class Channel:
    def __init__(self, capacity: int = 0):
        self._capacity = capacity
        self._buffer: Deque[Any] = deque()
        self._closed = False
        self._recv_waiters: Deque[Future] = deque()
        self._send_waiters: Deque[Tuple[Future, Any]] = deque()

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def qsize(self) -> int:
        return len(self._buffer)

    def empty(self) -> bool:
        return len(self._buffer) == 0

    def full(self) -> bool:
        if self._capacity <= 0:
            return False
        return len(self._buffer) >= self._capacity

    def close(self) -> None:
        self._closed = True
        for waiter, _ in self._send_waiters:
            if not waiter.done():
                waiter.set_exception(EOFError("Channel is closed"))
        self._send_waiters.clear()
        for waiter in self._recv_waiters:
            if not waiter.done():
                waiter.set_exception(EOFError("Channel is closed"))
        self._recv_waiters.clear()

    def closed(self) -> bool:
        return self._closed

    def send(self, item: Any) -> Generator[Future, None, None]:
        if self._closed:
            raise EOFError("Channel is closed")

        while self._recv_waiters:
            waiter = self._recv_waiters.popleft()
            if not waiter.done():
                waiter.set_result(item)
                fut = get_event_loop().create_future()
                fut.set_result(None)
                yield fut
                return

        if self._capacity <= 0 or len(self._buffer) < self._capacity:
            self._buffer.append(item)
            fut = get_event_loop().create_future()
            fut.set_result(None)
            yield fut
            return

        loop = get_event_loop()
        future = loop.create_future()
        self._send_waiters.append((future, item))
        try:
            yield future
        except CancelledError:
            try:
                self._send_waiters.remove((future, item))
            except ValueError:
                pass
            raise

    def recv(self) -> Generator[Future, None, Any]:
        if self._buffer:
            item = self._buffer.popleft()
            self._wakeup_sender()
            fut = get_event_loop().create_future()
            fut.set_result(item)
            yield fut
            return item

        if self._closed:
            raise EOFError("Channel is closed")

        loop = get_event_loop()
        future = loop.create_future()
        self._recv_waiters.append(future)
        try:
            item = yield future
        except CancelledError:
            try:
                self._recv_waiters.remove(future)
            except ValueError:
                pass
            raise
        return item

    def _wakeup_sender(self) -> None:
        while self._send_waiters:
            sender_future, item = self._send_waiters.popleft()
            if not sender_future.done():
                self._buffer.append(item)
                sender_future.set_result(None)
                break
