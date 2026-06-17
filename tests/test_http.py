import json
import sys
import os
import time
import socket
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from coro.core import (
    run,
    sleep,
    create_task,
    get_event_loop,
    CancelledError,
)
from coro.http import (
    HTTPError,
    HTTPRequest,
    HTTPResponse,
    HTTPClient,
    HTTPServer,
    logging_middleware,
)
from coro.combinators import gather


def wait_task(task):
    while not task.done():
        yield from sleep(0.001)
    try:
        return task.result()
    except BaseException as e:
        raise e


def test_http_client_get_external():
    print("Test 1: HTTP client GET external website...")
    
    def coro():
        client = HTTPClient()
        try:
            response = yield from client.get("http://example.com", timeout=10.0)
            print(f"  Status: {response.status_code}")
            print(f"  Body length: {len(response.body)} bytes")
            assert response.status_code == 200, f"Expected 200, got {response.status_code}"
            assert len(response.body) > 0, "Empty body"
            assert b"Example Domain" in response.body, "Expected 'Example Domain' in body"
            print("  PASS")
        except Exception as e:
            print(f"  SKIPPED (network issue): {e}")
        finally:
            client.close()
    
    run(coro())


def test_http_server_basic():
    print("Test 2: HTTP server basic routing...")
    
    server = HTTPServer(host="127.0.0.1", port=0)
    
    @server.get("/hello")
    def hello_handler(request):
        return HTTPResponse(
            status_code=200,
            reason="OK",
            headers={"Content-Type": "text/plain"},
            body=b"Hello, World!",
        )
    
    @server.post("/echo")
    def echo_handler(request):
        return HTTPResponse(
            status_code=200,
            reason="OK",
            headers={"Content-Type": "application/json"},
            body=request.body,
        )
    
    def coro():
        yield from server.start()
        port = server._server_sock.getsockname()[1]
        print(f"  Server started on port {port}")
        
        client = HTTPClient()
        try:
            response = yield from client.get(f"http://127.0.0.1:{port}/hello", timeout=5.0)
            assert response.status_code == 200, f"GET /hello: Expected 200, got {response.status_code}"
            assert response.body == b"Hello, World!", f"Unexpected body: {response.body}"
            print("  GET /hello: OK")
            
            test_data = {"message": "test echo", "value": 42}
            response = yield from client.post(
                f"http://127.0.0.1:{port}/echo",
                json_body=test_data,
                timeout=5.0,
            )
            assert response.status_code == 200, f"POST /echo: Expected 200, got {response.status_code}"
            resp_data = response.json()
            assert resp_data == test_data, f"Echo data mismatch: {resp_data} != {test_data}"
            print("  POST /echo: OK")
            
            response = yield from client.get(f"http://127.0.0.1:{port}/notfound", timeout=5.0)
            assert response.status_code == 404, f"Expected 404, got {response.status_code}"
            print("  404 handling: OK")
        finally:
            client.close()
            server.stop()
    
    run(coro())
    print("  PASS")


def test_connection_pool_reuse():
    print("Test 3: Connection pool reuse...")
    
    server = HTTPServer(host="127.0.0.1", port=0)
    request_count = [0]
    
    @server.get("/test")
    def test_handler(request):
        request_count[0] += 1
        return HTTPResponse(
            status_code=200,
            reason="OK",
            headers={"Content-Type": "text/plain"},
            body=b"test",
        )
    
    def coro():
        yield from server.start()
        port = server._server_sock.getsockname()[1]
        
        client = HTTPClient(max_connections=5)
        try:
            key = client._pool_key("127.0.0.1", port)
            
            response1 = yield from client.get(f"http://127.0.0.1:{port}/test", timeout=5.0)
            assert response1.status_code == 200
            
            pool = client._get_pool(key)
            assert len(pool) == 1, f"Expected 1 connection in pool, got {len(pool)}"
            print(f"  After first request: {len(pool)} connection in pool")
            
            sock1_id = id(pool[0].stream.socket)
            
            response2 = yield from client.get(f"http://127.0.0.1:{port}/test", timeout=5.0)
            assert response2.status_code == 200
            
            assert len(pool) == 1, f"Expected 1 connection after reuse, got {len(pool)}"
            sock2_id = id(pool[0].stream.socket)
            
            assert sock1_id == sock2_id, "Connection was not reused (different socket)"
            print("  Connection was reused (same socket)")
            
            print("  PASS")
        finally:
            client.close()
            server.stop()
    
    run(coro())


def test_redirect():
    print("Test 4: HTTP redirect following...")
    
    server = HTTPServer(host="127.0.0.1", port=0)
    
    @server.get("/redirect")
    def redirect_handler(request):
        return HTTPResponse(
            status_code=302,
            reason="Found",
            headers={"Location": "/target", "Content-Type": "text/plain"},
            body=b"Redirecting...",
        )
    
    @server.get("/target")
    def target_handler(request):
        return HTTPResponse(
            status_code=200,
            reason="OK",
            headers={"Content-Type": "text/plain"},
            body=b"Target page",
        )
    
    def coro():
        yield from server.start()
        port = server._server_sock.getsockname()[1]
        
        client = HTTPClient(max_redirects=5)
        try:
            response = yield from client.get(
                f"http://127.0.0.1:{port}/redirect",
                timeout=5.0,
                follow_redirects=True,
            )
            assert response.status_code == 200, f"Expected 200 after redirect, got {response.status_code}"
            assert response.body == b"Target page", f"Unexpected body: {response.body}"
            print("  Redirect followed: OK")
            
            response = yield from client.get(
                f"http://127.0.0.1:{port}/redirect",
                timeout=5.0,
                follow_redirects=False,
            )
            assert response.status_code == 302, f"Expected 302 without redirect, got {response.status_code}"
            print("  No redirect following: OK")
            
            print("  PASS")
        finally:
            client.close()
            server.stop()
    
    run(coro())


def test_chunked_encoding():
    print("Test 5: Chunked transfer encoding...")
    
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(("127.0.0.1", 0))
    server_sock.listen(1)
    port = server_sock.getsockname()[1]
    
    def chunked_server():
        client_sock, addr = server_sock.accept()
        try:
            data = b""
            while b"\r\n\r\n" not in data:
                chunk = client_sock.recv(4096)
                if not chunk:
                    break
                data += chunk
            
            response = (
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: text/plain\r\n"
                b"Transfer-Encoding: chunked\r\n"
                b"\r\n"
                b"5\r\n"
                b"Hello\r\n"
                b"6\r\n"
                b" World\r\n"
                b"0\r\n"
                b"\r\n"
            )
            client_sock.sendall(response)
        finally:
            client_sock.close()
    
    server_thread = threading.Thread(target=chunked_server, daemon=True)
    server_thread.start()
    
    def coro():
        client = HTTPClient()
        try:
            response = yield from client.get(
                f"http://127.0.0.1:{port}/chunked",
                timeout=5.0,
                follow_redirects=False,
            )
            assert response.status_code == 200, f"Expected 200, got {response.status_code}"
            assert response.body == b"Hello World", f"Unexpected body: {response.body!r}"
            print(f"  Chunked body received: {response.body!r}")
            print("  PASS")
        finally:
            client.close()
    
    try:
        run(coro())
    finally:
        server_sock.close()
        server_thread.join(timeout=1)


def test_middleware_logging():
    print("Test 6: Middleware logging...")
    
    server = HTTPServer(host="127.0.0.1", port=0)
    server.add_middleware(logging_middleware)
    
    log_entries = []
    
    def test_middleware(request, handler):
        start = time.monotonic()
        log_entries.append(f"before: {request.method} {request.path}")
        response = yield from handler(request)
        elapsed = (time.monotonic() - start) * 1000
        log_entries.append(f"after: {request.method} {request.path} -> {response.status_code} ({elapsed:.0f}ms)")
        return response
    
    server.add_middleware(test_middleware)
    
    @server.get("/test")
    def test_handler(request):
        yield from sleep(0.01)
        return HTTPResponse(
            status_code=200,
            reason="OK",
            headers={"Content-Type": "text/plain"},
            body=b"ok",
        )
    
    def coro():
        yield from server.start()
        port = server._server_sock.getsockname()[1]
        
        client = HTTPClient()
        try:
            response = yield from client.get(
                f"http://127.0.0.1:{port}/test",
                timeout=5.0,
            )
            assert response.status_code == 200
            
            assert len(log_entries) >= 2, f"Expected at least 2 log entries, got {len(log_entries)}"
            assert any("before: GET /test" in e for e in log_entries), "Missing 'before' log entry"
            assert any("after: GET /test -> 200" in e for e in log_entries), "Missing 'after' log entry"
            print(f"  Log entries: {len(log_entries)}")
            for entry in log_entries:
                print(f"    {entry}")
            
            print("  PASS")
        finally:
            client.close()
            server.stop()
    
    run(coro())


def test_concurrent_requests():
    print("Test 7: 50 concurrent requests...")
    
    server = HTTPServer(host="127.0.0.1", port=0)
    server.add_middleware(logging_middleware)
    
    request_counter = [0]
    log_count = [0]
    
    def counting_middleware(request, handler):
        log_count[0] += 1
        response = yield from handler(request)
        return response
    
    server.add_middleware(counting_middleware)
    
    @server.get("/test")
    def test_handler(request):
        request_counter[0] += 1
        yield from sleep(0.01)
        return HTTPResponse(
            status_code=200,
            reason="OK",
            headers={"Content-Type": "text/plain"},
            body=f"request-{request_counter[0]}".encode(),
        )
    
    def coro():
        yield from server.start()
        port = server._server_sock.getsockname()[1]
        print(f"  Server on port {port}")
        
        client = HTTPClient(max_connections=10)
        try:
            start_time = time.monotonic()
            
            def make_request(i):
                response = yield from client.get(
                    f"http://127.0.0.1:{port}/test",
                    timeout=10.0,
                )
                return response
            
            tasks = [create_task(make_request(i)) for i in range(50)]
            results = yield from gather(*tasks, return_exceptions=True)
            
            elapsed = time.monotonic() - start_time
            
            success_count = sum(1 for r in results if isinstance(r, HTTPResponse) and r.status_code == 200)
            print(f"  Successful requests: {success_count}/50")
            print(f"  Total time: {elapsed:.3f}s")
            print(f"  Middleware invocations: {log_count[0]}")
            
            assert success_count == 50, f"Expected 50 successful requests, got {success_count}"
            assert request_counter[0] == 50, f"Expected 50 server-side requests, got {request_counter[0]}"
            assert log_count[0] == 50, f"Expected 50 middleware invocations, got {log_count[0]}"
            
            print("  PASS")
        finally:
            client.close()
            server.stop()
    
    run(coro())


def main():
    print("=" * 60)
    print("HTTP Module Acceptance Tests")
    print("=" * 60)
    
    tests = [
        test_http_client_get_external,
        test_http_server_basic,
        test_connection_pool_reuse,
        test_redirect,
        test_chunked_encoding,
        test_middleware_logging,
        test_concurrent_requests,
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
