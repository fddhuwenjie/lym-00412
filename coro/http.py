import json
import time
import socket
from collections import deque
from typing import Any, Callable, Deque, Dict, Generator, List, Optional, Tuple

from .core import Future, TimeoutError, create_task, get_event_loop
from .net import SocketStream
from .sync import Lock


class HTTPError(Exception):
    pass


class HTTPRequest:
    def __init__(
        self,
        method: str,
        path: str,
        headers: Optional[Dict[str, str]] = None,
        body: Optional[bytes] = None,
    ):
        self.method = method.upper()
        self.path = path
        self.headers: Dict[str, str] = headers or {}
        self.body = body or b""

    def to_bytes(self) -> bytes:
        lines = [f"{self.method} {self.path} HTTP/1.1"]
        for key, value in self.headers.items():
            lines.append(f"{key}: {value}")
        lines.append("")
        lines.append("")
        header_bytes = "\r\n".join(lines).encode("latin-1")
        return header_bytes + self.body


class HTTPResponse:
    def __init__(
        self,
        status_code: int,
        reason: str,
        headers: Dict[str, str],
        body: bytes,
    ):
        self.status_code = status_code
        self.reason = reason
        self.headers = headers
        self.body = body

    def json(self) -> Any:
        return json.loads(self.body.decode("utf-8"))

    def text(self) -> str:
        return self.body.decode("utf-8")

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 300


def _parse_headers(lines: List[bytes]) -> Tuple[str, int, str, Dict[str, str]]:
    if not lines:
        raise HTTPError("Empty response")
    
    status_line = lines[0].decode("latin-1")
    parts = status_line.split(" ", 2)
    if len(parts) < 2:
        raise HTTPError(f"Invalid status line: {status_line}")
    
    version = parts[0]
    status_code = int(parts[1])
    reason = parts[2] if len(parts) > 2 else ""

    headers: Dict[str, str] = {}
    for line in lines[1:]:
        if not line:
            break
        line_str = line.decode("latin-1")
        if ":" in line_str:
            key, value = line_str.split(":", 1)
            headers[key.strip()] = value.strip()
    
    return version, status_code, reason, headers


def _read_line(sock: socket.socket, timeout: Optional[float] = None) -> Generator[Future, None, bytes]:
    from .net import tcp_read
    line = b""
    while True:
        ch = yield from tcp_read(sock, 1, timeout)
        if not ch:
            raise HTTPError("Connection closed")
        line += ch
        if line.endswith(b"\r\n"):
            return line[:-2]


def _read_headers(sock: socket.socket, timeout: Optional[float] = None) -> Generator[Future, None, List[bytes]]:
    lines: List[bytes] = []
    while True:
        line = yield from _read_line(sock, timeout)
        if not line:
            break
        lines.append(line)
    return lines


def _read_body(
    sock: socket.socket,
    headers: Dict[str, str],
    timeout: Optional[float] = None,
) -> Generator[Future, None, bytes]:
    from .net import tcp_read
    content_length = headers.get("Content-Length")
    transfer_encoding = headers.get("Transfer-Encoding", "")

    if "chunked" in transfer_encoding.lower():
        return (yield from _read_chunked_body(sock, timeout))
    elif content_length is not None:
        length = int(content_length)
        if length == 0:
            return b""
        data = b""
        while len(data) < length:
            chunk = yield from tcp_read(sock, length - len(data), timeout)
            if not chunk:
                raise HTTPError("Connection closed before full body received")
            data += chunk
        return data
    else:
        return b""


def _read_chunked_body(
    sock: socket.socket,
    timeout: Optional[float] = None,
) -> Generator[Future, None, bytes]:
    from .net import tcp_read
    body = b""
    while True:
        size_line = yield from _read_line(sock, timeout)
        if not size_line:
            raise HTTPError("Invalid chunked encoding")
        size_str = size_line.decode("latin-1").split(";", 1)[0].strip()
        chunk_size = int(size_str, 16)
        if chunk_size == 0:
            while True:
                trailer = yield from _read_line(sock, timeout)
                if not trailer:
                    break
            break
        chunk_data = b""
        while len(chunk_data) < chunk_size:
            chunk = yield from tcp_read(sock, chunk_size - len(chunk_data), timeout)
            if not chunk:
                raise HTTPError("Connection closed during chunked transfer")
            chunk_data += chunk
        body += chunk_data
        crlf = yield from tcp_read(sock, 2, timeout)
        if crlf != b"\r\n":
            raise HTTPError("Invalid chunked encoding: missing CRLF after chunk")
    return body


def parse_request(
    sock: socket.socket,
    timeout: Optional[float] = None,
) -> Generator[Future, None, HTTPRequest]:
    lines = yield from _read_headers(sock, timeout)
    if not lines:
        raise HTTPError("Empty request")
    
    request_line = lines[0].decode("latin-1")
    parts = request_line.split(" ")
    if len(parts) < 2:
        raise HTTPError(f"Invalid request line: {request_line}")
    
    method = parts[0]
    path = parts[1]

    headers: Dict[str, str] = {}
    for line in lines[1:]:
        line_str = line.decode("latin-1")
        if ":" in line_str:
            key, value = line_str.split(":", 1)
            headers[key.strip()] = value.strip()

    body = yield from _read_body(sock, headers, timeout)
    
    return HTTPRequest(method=method, path=path, headers=headers, body=body)


HTTPHandler = Callable[[HTTPRequest], Generator]
HTTPMiddleware = Callable[[HTTPRequest, HTTPHandler], Generator]


def logging_middleware(
    request: HTTPRequest,
    handler: HTTPHandler,
) -> Generator:
    start_time = time.monotonic()
    try:
        response = yield from handler(request)
        elapsed = (time.monotonic() - start_time) * 1000
        print(f"[HTTP] {request.method} {request.path} -> {response.status_code} ({elapsed:.2f}ms)")
        return response
    except Exception as e:
        elapsed = (time.monotonic() - start_time) * 1000
        print(f"[HTTP] {request.method} {request.path} -> ERROR: {e} ({elapsed:.2f}ms)")
        raise


class _PooledConnection:
    def __init__(self, stream: SocketStream, last_used: float):
        self.stream = stream
        self.last_used = last_used


class HTTPClient:
    def __init__(self, max_connections: int = 5, max_redirects: int = 5):
        self._max_connections = max_connections
        self._max_redirects = max_redirects
        self._pools: Dict[str, Deque[_PooledConnection]] = {}
        self._locks: Dict[str, Lock] = {}
        self._loop = get_event_loop()

    def _get_pool(self, key: str) -> Deque[_PooledConnection]:
        if key not in self._pools:
            self._pools[key] = deque()
        return self._pools[key]

    def _get_lock(self, key: str) -> Lock:
        if key not in self._locks:
            self._locks[key] = Lock()
        return self._locks[key]

    def _pool_key(self, host: str, port: int) -> str:
        return f"{host}:{port}"

    def _get_connection(
        self,
        host: str,
        port: int,
        timeout: Optional[float] = None,
    ) -> Generator:
        key = self._pool_key(host, port)
        pool = self._get_pool(key)
        lock = self._get_lock(key)

        yield from lock.acquire()
        try:
            while pool:
                conn = pool.popleft()
                if time.monotonic() - conn.last_used < 30:
                    return conn.stream
                else:
                    conn.stream.close()
        finally:
            lock.release()

        stream = yield from SocketStream.connect(host, port, timeout)
        return stream

    def _return_connection(
        self,
        host: str,
        port: int,
        stream: SocketStream,
    ) -> Generator:
        key = self._pool_key(host, port)
        pool = self._get_pool(key)
        lock = self._get_lock(key)

        yield from lock.acquire()
        try:
            while len(pool) >= self._max_connections:
                old = pool.popleft()
                old.stream.close()
            pool.append(_PooledConnection(stream, time.monotonic()))
        finally:
            lock.release()

    def close(self) -> None:
        for pool in self._pools.values():
            for conn in pool:
                conn.stream.close()
        self._pools.clear()

    def get(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
        follow_redirects: bool = True,
    ) -> Generator:
        result = yield from self.request(
            "GET", url, headers=headers, timeout=timeout,
            follow_redirects=follow_redirects,
        )
        return result

    def post(
        self,
        url: str,
        data: Optional[bytes] = None,
        json_body: Optional[Any] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
        follow_redirects: bool = True,
    ) -> Generator:
        body = data
        req_headers = dict(headers) if headers else {}
        
        if json_body is not None:
            body = json.dumps(json_body).encode("utf-8")
            req_headers["Content-Type"] = "application/json"
        
        if body:
            req_headers["Content-Length"] = str(len(body))
        
        result = yield from self.request(
            "POST", url, headers=req_headers, body=body,
            timeout=timeout, follow_redirects=follow_redirects,
        )
        return result

    def request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        body: Optional[bytes] = None,
        timeout: Optional[float] = None,
        follow_redirects: bool = True,
    ) -> Generator:
        current_url = url
        redirect_count = 0
        current_method = method
        current_body = body
        current_headers = dict(headers) if headers else {}

        while True:
            host, port, path = self._parse_url(current_url)
            
            req_headers = {
                "Host": host if port == 80 else f"{host}:{port}",
                "Connection": "keep-alive",
            }
            req_headers.update(current_headers)
            
            request = HTTPRequest(
                method=current_method, path=path,
                headers=req_headers, body=current_body,
            )
            
            stream = yield from self._get_connection(host, port, timeout)
            try:
                yield from stream.write(request.to_bytes(), timeout)
                
                lines = yield from _read_headers(stream.socket, timeout)
                version, status_code, reason, resp_headers = _parse_headers(lines)
                resp_body = yield from _read_body(stream.socket, resp_headers, timeout)
                
                response = HTTPResponse(
                    status_code=status_code,
                    reason=reason,
                    headers=resp_headers,
                    body=resp_body,
                )

                connection_header = resp_headers.get("Connection", "").lower()
                if connection_header == "close":
                    stream.close()
                else:
                    yield from self._return_connection(host, port, stream)

                if (follow_redirects and 300 <= status_code < 400
                        and redirect_count < self._max_redirects):
                    location = resp_headers.get("Location")
                    if location:
                        redirect_count += 1
                        if location.startswith("http://") or location.startswith("https://"):
                            current_url = location
                        elif location.startswith("/"):
                            current_url = f"http://{host}:{port}{location}"
                        else:
                            base_path = path.rsplit("/", 1)[0] if "/" in path else ""
                            current_url = f"http://{host}:{port}{base_path}/{location}"
                        
                        if status_code in (303,):
                            current_method = "GET"
                            current_body = None
                        continue
                
                return response
            except Exception:
                stream.close()
                raise

    def _parse_url(self, url: str) -> Tuple[str, int, str]:
        if url.startswith("http://"):
            url = url[7:]
        elif url.startswith("https://"):
            raise HTTPError("HTTPS is not supported")
        
        if "/" in url:
            host_port, path = url.split("/", 1)
            path = "/" + path
        else:
            host_port = url
            path = "/"
        
        if ":" in host_port:
            host, port_str = host_port.rsplit(":", 1)
            port = int(port_str)
        else:
            host = host_port
            port = 80
        
        return host, port, path


class HTTPServer:
    def __init__(self, host: str = "127.0.0.1", port: int = 8000):
        self.host = host
        self.port = port
        self._routes: Dict[str, HTTPHandler] = {}
        self._middlewares: List[HTTPMiddleware] = []
        self._server_sock: Optional[socket.socket] = None

    def route(self, path: str, methods: Optional[List[str]] = None):
        if methods is None:
            methods = ["GET"]
        
        def decorator(handler: HTTPHandler) -> HTTPHandler:
            for method in methods:
                key = f"{method.upper()}:{path}"
                self._routes[key] = handler
            return handler
        
        return decorator

    def get(self, path: str):
        return self.route(path, methods=["GET"])

    def post(self, path: str):
        return self.route(path, methods=["POST"])

    def add_middleware(self, middleware: HTTPMiddleware) -> None:
        self._middlewares.append(middleware)

    def _build_handler_chain(self, handler: HTTPHandler) -> HTTPHandler:
        def ensure_coro(h):
            def wrapper(request: HTTPRequest) -> Generator:
                result = h(request)
                if isinstance(result, Generator):
                    response = yield from result
                    return response
                else:
                    return result
            return wrapper
        
        current = ensure_coro(handler)
        for middleware in reversed(self._middlewares):
            def make_wrapper(mw, h):
                def wrapper(request: HTTPRequest) -> Generator:
                    result = yield from mw(request, h)
                    return result
                return wrapper
            current = make_wrapper(middleware, current)
        return current

    def _handle_request(self, request: HTTPRequest) -> Generator:
        key = f"{request.method}:{request.path}"
        handler = self._routes.get(key)
        
        if handler is None:
            return HTTPResponse(
                status_code=404,
                reason="Not Found",
                headers={"Content-Type": "text/plain"},
                body=b"Not Found",
            )
        
        handler_chain = self._build_handler_chain(handler)
        response = yield from handler_chain(request)
        return response

    def _handle_client(
        self,
        client_sock: socket.socket,
        addr: Tuple[str, int],
    ) -> Generator:
        from .net import tcp_write
        try:
            while True:
                try:
                    request = yield from parse_request(client_sock)
                except (HTTPError, ConnectionError, TimeoutError, OSError):
                    break
                
                try:
                    response = yield from self._handle_request(request)
                except Exception as e:
                    response = HTTPResponse(
                        status_code=500,
                        reason="Internal Server Error",
                        headers={"Content-Type": "text/plain"},
                        body=f"Internal Server Error: {e}".encode(),
                    )
                
                response_bytes = self._serialize_response(response)
                try:
                    yield from tcp_write(client_sock, response_bytes)
                except (ConnectionError, TimeoutError, OSError):
                    break
                
                connection_header = request.headers.get("Connection", "").lower()
                if connection_header == "close":
                    break
        finally:
            client_sock.close()

    def _serialize_response(self, response: HTTPResponse) -> bytes:
        lines = [f"HTTP/1.1 {response.status_code} {response.reason}"]
        headers = dict(response.headers)
        if "Content-Length" not in headers and "Transfer-Encoding" not in headers:
            headers["Content-Length"] = str(len(response.body))
        if "Content-Type" not in headers:
            headers["Content-Type"] = "text/plain"
        headers["Connection"] = "keep-alive"
        
        for key, value in headers.items():
            lines.append(f"{key}: {value}")
        lines.append("")
        lines.append("")
        header_bytes = "\r\n".join(lines).encode("latin-1")
        return header_bytes + response.body

    def start(self) -> Generator:
        import socket
        
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((self.host, self.port))
        server_sock.listen(128)
        server_sock.setblocking(False)
        self._server_sock = server_sock
        
        loop = get_event_loop()
        
        def accept_next():
            try:
                while True:
                    try:
                        client_sock, addr = server_sock.accept()
                        client_sock.setblocking(False)
                        loop.create_task(self._handle_client(client_sock, addr))
                    except BlockingIOError:
                        break
            except OSError:
                pass
            loop.add_reader(server_sock.fileno(), accept_next)
        
        loop.add_reader(server_sock.fileno(), accept_next)
        
        fut = loop.create_future()
        fut.set_result(None)
        yield fut

    def stop(self) -> None:
        if self._server_sock:
            self._server_sock.close()
            self._server_sock = None


__all__ = [
    "HTTPError",
    "HTTPRequest",
    "HTTPResponse",
    "HTTPClient",
    "HTTPServer",
    "HTTPMiddleware",
    "logging_middleware",
    "parse_request",
]
