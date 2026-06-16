import heapq
import os
import select
import socket
import sys
import time
import types
from collections import deque
from typing import Any, Callable, Deque, Dict, Generator, List, Optional, Set, Tuple


class CancelledError(BaseException):
    pass


class TimeoutError(Exception):
    pass


class ExceptionGroup(Exception):
    def __init__(self, message: str, exceptions: List[BaseException]):
        super().__init__(message)
        self.exceptions = exceptions

    def __repr__(self) -> str:
        return f"ExceptionGroup({self.args[0]!r}, {self.exceptions!r})"


class Future:
    _NO_RESULT = object()

    def __init__(self, loop: Optional["EventLoop"] = None):
        self._loop = loop or get_event_loop()
        self._result: Any = Future._NO_RESULT
        self._exception: Optional[BaseException] = None
        self._callbacks: List[Callable[["Future"], None]] = []
        self._done = False
        self._cancelled = False

    def done(self) -> bool:
        return self._done or self._cancelled

    def cancelled(self) -> bool:
        return self._cancelled

    def result(self) -> Any:
        if self._cancelled:
            raise CancelledError()
        if self._exception is not None:
            raise self._exception
        if self._result is Future._NO_RESULT:
            raise RuntimeError("Future is not done yet")
        return self._result

    def exception(self) -> Optional[BaseException]:
        if self._cancelled:
            return CancelledError()
        return self._exception

    def set_result(self, result: Any) -> None:
        if self._done or self._cancelled:
            return
        self._result = result
        self._done = True
        self._schedule_callbacks()

    def set_exception(self, exception: BaseException) -> None:
        if self._done or self._cancelled:
            return
        self._exception = exception
        self._done = True
        self._schedule_callbacks()

    def cancel(self) -> bool:
        if self._done or self._cancelled:
            return False
        self._cancelled = True
        self._schedule_callbacks()
        return True

    def add_done_callback(self, callback: Callable[["Future"], None]) -> None:
        if self._done or self._cancelled:
            self._loop.call_soon(callback, self)
        else:
            self._callbacks.append(callback)

    def remove_done_callback(self, callback: Callable[["Future"], None]) -> None:
        try:
            self._callbacks.remove(callback)
        except ValueError:
            pass

    def _schedule_callbacks(self) -> None:
        for cb in self._callbacks:
            self._loop.call_soon(cb, self)
        self._callbacks.clear()

    def __await__(self) -> Generator["Future", None, Any]:
        yield self
        return self.result()

    def __iter__(self) -> Generator["Future", None, Any]:
        return self.__await__()


class Task(Future):
    _task_counter = 0

    def __init__(self, coro: Generator, loop: Optional["EventLoop"] = None):
        super().__init__(loop)
        Task._task_counter += 1
        self._name = f"Task-{Task._task_counter}"
        self._coro = coro
        self._cancel_requested = False
        self._shield_count = 0
        self._loop.call_soon(self._step)

    def cancel(self) -> bool:
        if self.done():
            return False
        self._cancel_requested = True
        if self._shield_count > 0:
            return True
        self._loop.call_soon(self._step, CancelledError())
        return True

    def _step(self, exc: Optional[BaseException] = None, value: Any = None) -> None:
        if self.done():
            return
        old_task = self._loop._current_task
        self._loop._current_task = self
        try:
            if self._cancel_requested and self._shield_count == 0 and exc is None:
                exc = CancelledError()
                self._cancel_requested = False
            try:
                if exc is not None:
                    result = self._coro.throw(exc)
                elif value is not None:
                    result = self._coro.send(value)
                else:
                    result = next(self._coro)
            except StopIteration as e:
                self.set_result(e.value)
                return
            except BaseException as e:
                self.set_exception(e)
                return

            if isinstance(result, Future):
                result.add_done_callback(self._wakeup)
            elif result is None:
                self._loop.call_soon(self._step)
            elif isinstance(result, types.GeneratorType):
                inner_task = self._loop.create_task(result)
                inner_task.add_done_callback(self._wakeup)
            else:
                self.set_exception(TypeError(f"coroutine yielded unknown object: {result!r}"))
        finally:
            self._loop._current_task = old_task

    def _wakeup(self, future: Future) -> None:
        try:
            result = future.result()
        except BaseException as e:
            self._step(e)
        else:
            self._step(value=result)


class _TimerHandle:
    def __init__(self, when: float, callback: Callable, args: Tuple = ()):
        self.when = when
        self.callback = callback
        self.args = args
        self.cancelled = False

    def __lt__(self, other: "_TimerHandle") -> bool:
        return self.when < other.when

    def cancel(self) -> None:
        self.cancelled = True


class EventLoop:
    def __init__(self):
        self._kqueue = select.kqueue()
        self._ready: Deque[Tuple[Callable, Tuple]] = deque()
        self._timers: List[_TimerHandle] = []
        self._fd_callbacks: Dict[int, Tuple[Optional[Callable], Optional[Callable]]] = {}
        self._fd_events: Dict[int, int] = {}
        self._running = False
        self._stopping = False
        self._current_task: Optional[Task] = None
        self._tasks: Set[Task] = set()
        self._exception_handler: Optional[Callable] = None

    def _fd_ctl(self, fd: int, filter_: int, flags: int, fflags: int = 0):
        kevent = select.kevent(fd, filter_, flags, fflags)
        self._kqueue.control([kevent], 0)

    def _register_fd(self, fd: int, filter_: int, callback: Callable):
        flags = select.KQ_EV_ADD | select.KQ_EV_ONESHOT
        try:
            self._fd_ctl(fd, filter_, flags)
        except OSError:
            pass
        current_callbacks = self._fd_callbacks.get(fd, (None, None))
        if filter_ == select.KQ_FILTER_READ:
            self._fd_callbacks[fd] = (callback, current_callbacks[1])
        else:
            self._fd_callbacks[fd] = (current_callbacks[0], callback)
        self._fd_events[fd] = self._fd_events.get(fd, 0) | filter_

    def _unregister_fd(self, fd: int, filter_: int):
        current_events = self._fd_events.get(fd, 0)
        if current_events & filter_:
            flags = select.KQ_EV_DELETE
            try:
                self._fd_ctl(fd, filter_, flags)
            except OSError:
                pass
            current_callbacks = self._fd_callbacks.get(fd, (None, None))
            if filter_ == select.KQ_FILTER_READ:
                self._fd_callbacks[fd] = (None, current_callbacks[1])
            else:
                self._fd_callbacks[fd] = (current_callbacks[0], None)
            remaining = current_events & ~filter_
            if remaining == 0:
                self._fd_callbacks.pop(fd, None)
                self._fd_events.pop(fd, None)
            else:
                self._fd_events[fd] = remaining

    def call_soon(self, callback: Callable, *args) -> None:
        self._ready.append((callback, args))

    def call_later(self, delay: float, callback: Callable, *args) -> _TimerHandle:
        when = time.monotonic() + delay
        handle = _TimerHandle(when, callback, args)
        heapq.heappush(self._timers, handle)
        return handle

    def call_at(self, when: float, callback: Callable, *args) -> _TimerHandle:
        handle = _TimerHandle(when, callback, args)
        heapq.heappush(self._timers, handle)
        return handle

    def add_reader(self, fd: int, callback: Callable, *args) -> None:
        def cb():
            callback(*args)
        self._register_fd(fd, select.KQ_FILTER_READ, cb)

    def remove_reader(self, fd: int) -> None:
        self._unregister_fd(fd, select.KQ_FILTER_READ)

    def add_writer(self, fd: int, callback: Callable, *args) -> None:
        def cb():
            callback(*args)
        self._register_fd(fd, select.KQ_FILTER_WRITE, cb)

    def remove_writer(self, fd: int) -> None:
        self._unregister_fd(fd, select.KQ_FILTER_WRITE)

    def time(self) -> float:
        return time.monotonic()

    def create_task(self, coro: Generator) -> Task:
        task = Task(coro, self)
        self._tasks.add(task)
        task.add_done_callback(lambda t: self._tasks.discard(t))
        return task

    def create_future(self) -> Future:
        return Future(self)

    def current_task(self) -> Optional[Task]:
        return self._current_task

    def run_forever(self) -> None:
        self._running = True
        self._stopping = False
        try:
            while self._running and not self._stopping:
                self._run_once()
        finally:
            self._running = False

    def run_until_complete(self, future: Future) -> Any:
        def _run_until_done_callback(f):
            self.stop()

        future.add_done_callback(_run_until_done_callback)
        self.run_forever()
        future.remove_done_callback(_run_until_done_callback)
        return future.result()

    def stop(self) -> None:
        self._stopping = True

    def is_running(self) -> bool:
        return self._running

    def close(self) -> None:
        if self._running:
            raise RuntimeError("Cannot close a running event loop")
        self._kqueue.close()

    def _run_once(self) -> None:
        timeout = None

        now = time.monotonic()
        while self._timers:
            timer = self._timers[0]
            if timer.cancelled:
                heapq.heappop(self._timers)
                continue
            if timer.when <= now:
                heapq.heappop(self._timers)
                if not timer.cancelled:
                    self._ready.append((timer.callback, timer.args))
            else:
                timeout = timer.when - now
                break

        if not self._ready and timeout is None and not self._fd_callbacks:
            return

        if timeout is None:
            timeout = 0 if self._ready else 1.0
        else:
            timeout = max(0, min(timeout, 1.0))

        if not self._ready:
            n_events = 1024
            try:
                events = self._kqueue.control(None, n_events, timeout)
            except InterruptedError:
                return
            for event in events:
                fd = event.ident
                callbacks = self._fd_callbacks.get(fd)
                if callbacks is None:
                    continue
                if event.filter == select.KQ_FILTER_READ and callbacks[0]:
                    self._ready.append((callbacks[0], ()))
                    current_events = self._fd_events.get(fd, 0)
                    if current_events & select.KQ_FILTER_READ:
                        self._fd_events[fd] = current_events & ~select.KQ_FILTER_READ
                elif event.filter == select.KQ_FILTER_WRITE and callbacks[1]:
                    self._ready.append((callbacks[1], ()))
                    current_events = self._fd_events.get(fd, 0)
                    if current_events & select.KQ_FILTER_WRITE:
                        self._fd_events[fd] = current_events & ~select.KQ_FILTER_WRITE

        for _ in range(len(self._ready)):
            if not self._ready:
                break
            callback, args = self._ready.popleft()
            try:
                callback(*args)
            except BaseException as e:
                self._handle_exception(e)

    def _handle_exception(self, exc: BaseException) -> None:
        if self._exception_handler:
            try:
                self._exception_handler(self, {"exception": exc})
                return
            except BaseException:
                pass
        if isinstance(exc, (CancelledError,)):
            return
        print(f"Unhandled exception in event loop: {exc!r}", file=sys.stderr)
        import traceback
        traceback.print_exc()

    def set_exception_handler(self, handler: Callable) -> None:
        self._exception_handler = handler


_event_loop: Optional[EventLoop] = None


def get_event_loop() -> EventLoop:
    global _event_loop
    if _event_loop is None:
        _event_loop = EventLoop()
    return _event_loop


def set_event_loop(loop: EventLoop) -> None:
    global _event_loop
    _event_loop = loop


def run(main_coro: Generator) -> Any:
    loop = get_event_loop()
    task = loop.create_task(main_coro)
    try:
        return loop.run_until_complete(task)
    finally:
        pass


def sleep(seconds: float) -> Future:
    loop = get_event_loop()
    future = loop.create_future()
    loop.call_later(seconds, lambda: future.set_result(None))
    return future


def create_task(coro: Generator) -> Task:
    loop = get_event_loop()
    return loop.create_task(coro)


def current_task() -> Optional[Task]:
    loop = get_event_loop()
    return loop.current_task()


def shield(arg: Generator) -> Generator:
    loop = get_event_loop()
    if isinstance(arg, types.GeneratorType):
        inner = loop.create_task(arg)
    else:
        inner = arg

    shield_done = loop.create_future()

    def inner_done_callback(fut):
        if shield_done.done():
            return
        if fut.cancelled():
            shield_done.cancel()
        elif fut.exception() is not None:
            shield_done.set_exception(fut.exception())
        else:
            shield_done.set_result(fut.result())

    inner.add_done_callback(inner_done_callback)

    def shield_coro():
        task = current_task()
        shield_active = False
        if task is not None:
            task._shield_count += 1
            shield_active = True
        try:
            result = yield shield_done
            return result
        finally:
            if shield_active and task is not None:
                task._shield_count -= 1
                if task._shield_count == 0 and task._cancel_requested and not task.done():
                    task.cancel()

    return shield_coro()
