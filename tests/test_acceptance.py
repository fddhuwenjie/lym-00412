import time
import sys
import os
import socket
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from coro.core import (
    run,
    sleep,
    create_task,
    CancelledError,
    shield,
    get_event_loop,
    Task,
    Future,
)
from coro.sync import Lock, Semaphore, Event, Channel
from coro.structured import TaskGroup, ExceptionGroup
from coro.combinators import gather, race, wait_first
from coro.net import tcp_connect, tcp_read, tcp_write
from coro.dns import resolve as dns_resolve


def wait_task(task):
    while not task.done():
        yield from sleep(0.001)
    try:
        return task.result()
    except BaseException as e:
        raise e


def test_sleep_precision():
    print("Test 1: Sleep precision...")
    def coro():
        errors = []
        for _ in range(5):
            t0 = time.monotonic()
            yield from sleep(0.1)
            elapsed = (time.monotonic() - t0) * 1000
            error = abs(elapsed - 100)
            errors.append(error)
        max_error = max(errors)
        print(f"  Max 100ms sleep error: {max_error:.2f}ms (< 20ms required)")
        assert max_error < 20, f"Sleep precision too low: {max_error}ms > 20ms"

        t0 = time.monotonic()
        yield from sleep(1.0)
        elapsed = (time.monotonic() - t0) * 1000
        error = abs(elapsed - 1000)
        print(f"  1s sleep error: {error:.2f}ms (< 20ms required)")
        assert error < 20, f"1s sleep precision too low: {error}ms > 20ms"
        print("  PASS")
    run(coro())


def test_1000_coroutine_switching():
    print("Test 2: 1000 coroutine context switching...")
    def coro():
        results = []
        def worker(i):
            results.append(i)
            yield
        t0 = time.monotonic()
        tasks = [create_task(worker(i)) for i in range(1000)]
        for t in tasks:
            yield from wait_task(t)
        elapsed = (time.monotonic() - t0) * 1000
        print(f"  1000 coroutines scheduled in {elapsed:.2f}ms (< 50ms required)")
        assert len(results) == 1000
        assert elapsed < 50, f"Context switching too slow: {elapsed}ms > 50ms"
        print("  PASS")
    run(coro())


def test_channel_bounded():
    print("Test 3: Bounded Channel blocking on full...")
    def coro():
        ch = Channel(capacity=2)
        yield from ch.send(1)
        yield from ch.send(2)
        assert ch.full()

        blocked = [True]
        def sender():
            yield from ch.send(3)
            blocked[0] = False

        sender_task = create_task(sender())
        yield from sleep(0.1)
        assert blocked[0], "Sender should be blocked when channel is full"

        val = yield from ch.recv()
        assert val == 1
        yield from sleep(0.1)
        assert not blocked[0], "Sender should be unblocked after recv"
        yield from wait_task(sender_task)

        val = yield from ch.recv()
        assert val == 2
        val = yield from ch.recv()
        assert val == 3
        print("  PASS")
    run(coro())


def test_taskgroup_exception_cancel():
    print("Test 4: TaskGroup exception propagation and cancel...")
    def coro():
        cancelled = [False]
        task2_started = [False]

        def task1():
            yield from sleep(0.1)
            raise ValueError("task1 failed")

        def task2():
            task2_started[0] = True
            try:
                yield from sleep(5.0)
            except CancelledError:
                cancelled[0] = True
                raise

        caught_exc = [None]
        try:
            tg = TaskGroup()
            yield from tg.__aenter__()
            try:
                tg.create_task(task1())
                tg.create_task(task2())
                yield from sleep(2.0)
                caught_exc[0] = yield from tg.__aexit__(None, None, None)
            except BaseException as e:
                caught_exc[0] = yield from tg.__aexit__(type(e), e, e.__traceback__)
                if not caught_exc[0]:
                    raise
        except (ValueError, ExceptionGroup) as e:
            caught_exc[0] = e

        assert caught_exc[0] is not None, "Should have raised an exception"
        print(f"  Caught expected exception: {type(caught_exc[0]).__name__}")
        assert task2_started[0], "Task2 should have started"
        assert cancelled[0], "Task2 should be cancelled"
        print("  PASS")
    run(coro())


def test_cancel_propagation():
    print("Test 5: Cancel propagation to nested coroutines...")
    def coro():
        cancelled = {"level1": False, "level2": False, "level3": False}

        def level3():
            try:
                yield from sleep(1.0)
            except CancelledError:
                cancelled["level3"] = True
                raise

        def level2():
            try:
                yield from level3()
            except CancelledError:
                cancelled["level2"] = True
                raise

        def level1():
            try:
                yield from level2()
            except CancelledError:
                cancelled["level1"] = True
                raise

        task = create_task(level1())
        yield from sleep(0.1)
        assert task.cancel()
        yield from sleep(0.1)
        assert task.done() or task.cancelled()
        assert cancelled["level1"], "Cancel should propagate to level1"
        assert cancelled["level2"], "Cancel should propagate to level2"
        assert cancelled["level3"], "Cancel should propagate to level3"
        print("  All cancellation propagated correctly")
        print("  PASS")
    run(coro())


def test_shield():
    print("Test 6: Shield from cancellation...")
    def coro():
        completed = [False]

        def inner():
            yield from sleep(0.2)
            completed[0] = True
            return "result"

        def outer():
            try:
                result = yield from shield(inner())
                return result
            except CancelledError:
                return "cancelled"

        task = create_task(outer())
        yield from sleep(0.05)
        task.cancel()
        result = yield from wait_task(task)

        assert completed[0], "Shielded inner should complete"
        assert result == "result", f"Expected result, got {result}"
        print("  Shielded task completed despite cancellation")
        print("  PASS")
    run(coro())


def test_gather_race_waitfirst():
    print("Test 7: gather/race/wait_first combinators...")
    def coro():
        def a():
            yield from sleep(0.05)
            return 1

        def b():
            yield from sleep(0.2)
            return 2

        def c():
            yield from sleep(0.1)
            return 3

        results = yield from gather(a(), b(), c())
        assert results == [1, 2, 3], f"gather results mismatch: {results}"
        print("  gather: OK")

        idx, result = yield from race(a(), b(), c())
        assert idx == 0 and result == 1, f"race result mismatch: idx={idx}, result={result}"
        print("  race: OK")

        result = yield from wait_first(a(), b(), c())
        assert result == 1, f"wait_first result mismatch: {result}"
        print("  wait_first: OK")
        print("  PASS")
    run(coro())


def test_lock_semaphore_event():
    print("Test 8: Lock/Semaphore/Event primitives...")
    def coro():
        lock = Lock()
        counter = [0]

        def worker():
            yield from lock.acquire()
            try:
                tmp = counter[0]
                yield from sleep(0.01)
                counter[0] = tmp + 1
            finally:
                lock.release()

        tasks = [create_task(worker()) for _ in range(10)]
        for t in tasks:
            yield from wait_task(t)

        assert counter[0] == 10, f"Lock failed: counter={counter[0]}"
        print("  Lock: OK")

        sem = Semaphore(3)
        active = [0]
        max_active = [0]

        def sem_worker():
            yield from sem.acquire()
            try:
                active[0] += 1
                if active[0] > max_active[0]:
                    max_active[0] = active[0]
                yield from sleep(0.02)
            finally:
                active[0] -= 1
                sem.release()

        tasks = [create_task(sem_worker()) for _ in range(10)]
        for t in tasks:
            yield from wait_task(t)

        assert max_active[0] <= 3, f"Semaphore failed: max_active={max_active[0]}"
        print("  Semaphore: OK")

        event = Event()
        triggered = [0]

        def waiter():
            yield from event.wait()
            triggered[0] += 1

        waiters = [create_task(waiter()) for _ in range(5)]
        yield from sleep(0.1)
        assert triggered[0] == 0
        event.set()
        yield from sleep(0.1)
        assert triggered[0] == 5
        print("  Event: OK")
        print("  PASS")
    run(coro())


def test_dns_resolve():
    print("Test 9: DNS coroutine resolution...")
    def coro():
        try:
            ips = yield from dns_resolve("google.com")
            print(f"  Resolved google.com -> {ips}")
            assert len(ips) > 0, "No IPs returned"
            for ip in ips:
                parts = ip.split(".")
                assert len(parts) == 4
                for p in parts:
                    assert 0 <= int(p) <= 255
            print("  PASS")
        except Exception as e:
            print(f"  DNS test skipped (network issue): {e}")
    run(coro())


def test_tcp_echo_200():
    print("Test 10: 200 concurrent TCP connections to echo server...")

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", 0))
    server.listen(256)
    port = server.getsockname()[1]

    def echo_server():
        while True:
            try:
                client, addr = server.accept()
                t = threading.Thread(target=handle_client, args=(client,), daemon=True)
                t.start()
            except Exception:
                break

    def handle_client(client):
        try:
            data = b""
            while True:
                chunk = client.recv(4096)
                if not chunk:
                    break
                data += chunk
                if b"\n" in chunk:
                    break
            client.sendall(data)
        finally:
            client.close()

    server_thread = threading.Thread(target=echo_server, daemon=True)
    server_thread.start()

    def coro():
        completed = [0]
        errors = []

        def worker(i):
            try:
                sock = yield from tcp_connect("127.0.0.1", port, timeout=10.0)
                msg = f"Hello {i}\n".encode()
                yield from tcp_write(sock, msg, timeout=10.0)
                resp = yield from tcp_read(sock, len(msg), timeout=10.0)
                assert resp == msg, f"Mismatch: {resp} != {msg}"
                sock.close()
                completed[0] += 1
            except Exception as e:
                errors.append((i, e))

        tasks = [create_task(worker(i)) for i in range(200)]
        for t in tasks:
            yield from wait_task(t)

        print(f"  Completed: {completed[0]}/200, Errors: {len(errors)}")
        if errors:
            for i, e in errors[:5]:
                print(f"    Task {i} error: {e}")
        assert completed[0] == 200, f"Only {completed[0]}/200 connections completed"
        print("  PASS")

    try:
        run(coro())
    finally:
        server.close()


def main():
    print("=" * 60)
    print("Coroutine Framework Acceptance Tests")
    print("=" * 60)

    tests = [
        test_sleep_precision,
        test_1000_coroutine_switching,
        test_channel_bounded,
        test_taskgroup_exception_cancel,
        test_cancel_propagation,
        test_shield,
        test_gather_race_waitfirst,
        test_lock_semaphore_event,
        test_dns_resolve,
        test_tcp_echo_200,
    ]

    passed = 0
    failed = 0

    for test in tests:
        print()
        try:
            test()
            passed += 1
        except Exception as e:
            failed += 1
            print(f"  FAILED: {e}")
            import traceback
            traceback.print_exc()

    print()
    print("=" * 60)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 60)

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
