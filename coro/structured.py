from typing import Any, Generator, List, Optional

from .core import CancelledError, ExceptionGroup, Future, Task, get_event_loop, current_task


class TaskGroup:
    def __init__(self):
        self._tasks: List[Task] = []
        self._exceptions: List[BaseException] = []
        self._entered = False
        self._exiting = False
        self._aborting = False
        self._cancel_scope_token: Optional[Any] = None

    def create_task(self, coro: Generator) -> Task:
        if not self._entered:
            raise RuntimeError("Cannot create task outside of TaskGroup context")
        if self._exiting:
            raise RuntimeError("Cannot create task during TaskGroup exit")
        loop = get_event_loop()
        task = loop.create_task(coro)
        task._taskgroup = self
        self._tasks.append(task)
        task.add_done_callback(self._on_task_done)
        return task

    def _on_task_done(self, task: Task) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None and not isinstance(exc, CancelledError):
            self._exceptions.append(exc)
            if not self._aborting:
                self._abort()

    def _abort(self) -> None:
        if self._aborting:
            return
        self._aborting = True
        for task in self._tasks:
            if not task.done():
                task.cancel()

    def __iter__(self) -> Generator[Future, None, "TaskGroup"]:
        return self.__aenter__()

    def __await__(self) -> Generator[Future, None, "TaskGroup"]:
        return self.__aenter__()

    def __aenter__(self) -> Generator[Future, None, "TaskGroup"]:
        if self._entered:
            raise RuntimeError("TaskGroup is already entered")
        self._entered = True
        fut = get_event_loop().create_future()
        fut.set_result(self)
        yield fut
        return self

    def __aexit__(self, etype, exc, tb) -> Generator[Future, None, bool]:
        self._exiting = True

        if exc is not None and not isinstance(exc, CancelledError):
            self._exceptions.append(exc)
            self._abort()

        yield from self._wait_completion()

        if self._exceptions:
            if len(self._exceptions) == 1:
                raise self._exceptions[0]
            raise ExceptionGroup(
                f"{len(self._exceptions)} exceptions in TaskGroup",
                self._exceptions,
            )

        return False

    def _wait_completion(self) -> Generator[Future, None, None]:
        loop = get_event_loop()
        while True:
            pending = [t for t in self._tasks if not t.done()]
            if not pending:
                break

            done_future = loop.create_future()

            def make_callback(task):
                def callback(f):
                    if not done_future.done():
                        done_future.set_result(task)
                return callback

            for task in pending:
                task.add_done_callback(make_callback(task))

            try:
                yield done_future
            finally:
                for task in pending:
                    pass
