from typing import Any, Generator, List, Tuple

from .core import CancelledError, ExceptionGroup, Future, Task, get_event_loop


def gather(*coros_or_futures, return_exceptions: bool = False) -> Generator[Future, None, List[Any]]:
    loop = get_event_loop()

    tasks = []
    for c in coros_or_futures:
        if isinstance(c, Task) or isinstance(c, Future):
            tasks.append(c)
        else:
            tasks.append(loop.create_task(c))

    if not tasks:
        fut = loop.create_future()
        fut.set_result([])
        yield fut
        return []

    n_tasks = len(tasks)
    results = [None] * n_tasks
    exceptions = []
    finished = 0
    done_future = loop.create_future()

    def make_callback(idx):
        def callback(fut):
            nonlocal finished
            finished += 1
            if fut.cancelled():
                if return_exceptions:
                    results[idx] = CancelledError()
                else:
                    exceptions.append(CancelledError())
            elif fut.exception() is not None:
                if return_exceptions:
                    results[idx] = fut.exception()
                else:
                    exceptions.append(fut.exception())
            else:
                results[idx] = fut.result()
            if finished == n_tasks and not done_future.done():
                if exceptions and not return_exceptions:
                    if len(exceptions) == 1:
                        done_future.set_exception(exceptions[0])
                    else:
                        done_future.set_exception(
                            ExceptionGroup(f"{len(exceptions)} exceptions", exceptions)
                        )
                else:
                    done_future.set_result(results)
        return callback

    for i, task in enumerate(tasks):
        task.add_done_callback(make_callback(i))

    try:
        yield done_future
    except CancelledError:
        for task in tasks:
            if not task.done():
                task.cancel()
        raise

    result = yield done_future
    return result


def race(*coros_or_futures) -> Generator[Future, None, Tuple[int, Any]]:
    loop = get_event_loop()

    tasks = []
    for c in coros_or_futures:
        if isinstance(c, Task) or isinstance(c, Future):
            tasks.append(c)
        else:
            tasks.append(loop.create_task(c))

    if not tasks:
        raise ValueError("race() requires at least one argument")

    done_future = loop.create_future()
    completed_idx = -1
    completed_result = None

    def make_callback(idx):
        def callback(fut):
            nonlocal completed_idx, completed_result
            if done_future.done():
                return
            completed_idx = idx
            if fut.cancelled():
                done_future.set_exception(CancelledError())
            elif fut.exception() is not None:
                done_future.set_exception(fut.exception())
            else:
                completed_result = fut.result()
                done_future.set_result((idx, completed_result))
            for t in tasks:
                if not t.done():
                    t.cancel()
        return callback

    for i, task in enumerate(tasks):
        task.add_done_callback(make_callback(i))

    try:
        idx, result = yield done_future
    except CancelledError:
        for task in tasks:
            if not task.done():
                task.cancel()
        raise

    return idx, result


def wait_first(*coros_or_futures) -> Generator[Future, None, Any]:
    loop = get_event_loop()

    tasks = []
    for c in coros_or_futures:
        if isinstance(c, Task) or isinstance(c, Future):
            tasks.append(c)
        else:
            tasks.append(loop.create_task(c))

    if not tasks:
        raise ValueError("wait_first() requires at least one argument")

    done_future = loop.create_future()

    def make_callback():
        def callback(fut):
            if done_future.done():
                return
            if fut.cancelled():
                all_done = all(t.done() for t in tasks)
                if all_done:
                    done_future.set_exception(CancelledError())
            elif fut.exception() is not None:
                done_future.set_exception(fut.exception())
                for t in tasks:
                    if not t.done():
                        t.cancel()
            else:
                done_future.set_result(fut.result())
                for t in tasks:
                    if not t.done():
                        t.cancel()
        return callback

    cb = make_callback()
    for task in tasks:
        task.add_done_callback(cb)

    try:
        result = yield done_future
    except CancelledError:
        for task in tasks:
            if not task.done():
                task.cancel()
        raise

    return result
