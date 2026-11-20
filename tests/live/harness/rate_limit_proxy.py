"""A local HTTP proxy that answers 429 to the first requests, then forwards to the real API.

A test environment cannot be relied on to refuse requests for rate on cue, so the harness puts
this proxy between Airflow and Dreamhub to show the provider waits and continues.
It runs inside the Airflow container (``python rate_limit_proxy.py ...``), so it needs only the
standard library and httpx, both present wherever the provider is installed.

Nothing about a request is logged: headers carry the API token.
"""

from __future__ import annotations

import argparse
import http.server
import json
import threading
from pathlib import Path
from types import TracebackType

import httpx

# Headers that describe one hop, not the message: never forwarded in either direction.
HOP_BY_HOP = frozenset(
    {
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
        "host",
        "content-length",
        "content-encoding",
    }
)


class RateLimitProxy:
    """Refuses the first ``refusals`` requests with 429 and ``Retry-After``, forwards the rest to ``upstream``."""

    def __init__(
        self,
        upstream: str,
        refusals: int,
        retry_after_seconds: int,
        port: int = 0,
        stats_path: Path | None = None,
    ) -> None:
        self.upstream = upstream.rstrip("/")
        self.retry_after_seconds = retry_after_seconds
        self.stats_path = stats_path
        self.refused = 0
        self.forwarded = 0
        self._refusals_left = refusals
        self._lock = threading.Lock()
        self._client = httpx.Client(base_url=self.upstream, timeout=60, follow_redirects=False)
        self._server = http.server.ThreadingHTTPServer(("127.0.0.1", port), self._handler_class())
        self._thread: threading.Thread | None = None

    @property
    def url(self) -> str:
        return f"http://127.0.0.1:{self._server.server_address[1]}"

    def __enter__(self) -> RateLimitProxy:
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._client.close()

    def serve_forever(self) -> None:
        self._server.serve_forever()

    def take_refusal(self) -> bool:
        """Whether this request is refused; counts it either way."""
        with self._lock:
            if self._refusals_left > 0:
                self._refusals_left -= 1
                self.refused += 1
                return True
            self.forwarded += 1
            return False

    def forward(self, method: str, path: str, headers: dict[str, str], body: bytes) -> httpx.Response:
        return self._client.request(method, path, headers=headers, content=body)

    def record_stats(self) -> None:
        """Write the counts to ``stats_path`` so the harness can read them from outside the process."""
        if self.stats_path is not None:
            with self._lock:
                stats = {"refused": self.refused, "forwarded": self.forwarded}
            self.stats_path.write_text(json.dumps(stats))

    def _handler_class(self) -> type[http.server.BaseHTTPRequestHandler]:
        proxy = self

        class Handler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def do_GET(self) -> None:  # noqa: N802 (http.server API)
                self._handle()

            def do_POST(self) -> None:  # noqa: N802
                self._handle()

            def do_PUT(self) -> None:  # noqa: N802
                self._handle()

            def do_PATCH(self) -> None:  # noqa: N802
                self._handle()

            def do_DELETE(self) -> None:  # noqa: N802
                self._handle()

            def log_message(self, format: str, *args: object) -> None:  # noqa: A002 (http.server API)
                del format, args  # never log: request lines and headers can carry the token

            def _handle(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length) if length else b""
                if proxy.take_refusal():
                    proxy.record_stats()
                    self._reply(429, {"Retry-After": str(proxy.retry_after_seconds)}, b'{"detail":"Too Many Requests"}')
                    return
                headers = {k: v for k, v in self.headers.items() if k.lower() not in HOP_BY_HOP}
                try:
                    answer = proxy.forward(self.command, self.path, headers, body)
                except httpx.HTTPError:
                    proxy.record_stats()
                    self._reply(502, {}, b'{"detail":"proxy could not reach the API"}')
                    return
                proxy.record_stats()
                kept = {k: v for k, v in answer.headers.items() if k.lower() not in HOP_BY_HOP}
                self._reply(answer.status_code, kept, answer.content)

            def _reply(self, status: int, headers: dict[str, str], body: bytes) -> None:
                self.send_response(status)
                for name, value in headers.items():
                    self.send_header(name, value)
                self.send_header("Content-Type", headers.get("content-type", "application/json"))
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        return Handler


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", required=True, help="origin of the real API, e.g. https://host")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--refusals", type=int, default=2)
    parser.add_argument("--retry-after", type=int, default=2)
    parser.add_argument("--stats", type=Path, required=True)
    args = parser.parse_args()
    proxy = RateLimitProxy(args.upstream, args.refusals, args.retry_after, port=args.port, stats_path=args.stats)
    proxy.record_stats()
    proxy.serve_forever()


if __name__ == "__main__":
    main()
