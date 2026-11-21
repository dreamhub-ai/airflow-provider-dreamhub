"""The live Airflow harness's own safety checks. These run in the normal suite; they need no Airflow or network."""

from __future__ import annotations

import hashlib
import http.client
import http.server
import json
import threading
from collections.abc import Iterator
from pathlib import Path

import httpx
import pytest

from tests.live.harness import config
from tests.live.harness.config import ApiToken, HarnessConfigError, LiveTarget
from tests.live.harness.ledger import RecordLedger
from tests.live.harness.rate_limit_proxy import RateLimitProxy
from tests.live.harness.secrets import SecretScanner

# A reserved test domain stands in for the development environment, whose address is not published here.
DEV_ORIGIN = "https://dev.example.test"
DEV_API = f"{DEV_ORIGIN}/api/v1"
TOKEN = "pat_live_0123456789abcdefghijklmnopqrstuv"


@pytest.fixture
def dev_allowed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(config, "ALLOWED_ORIGIN_SHA256", frozenset({hashlib.sha256(DEV_ORIGIN.encode()).hexdigest()}))


class TestLiveTarget:
    def test_ships_exactly_one_allowed_origin(self) -> None:
        assert len(config.ALLOWED_ORIGIN_SHA256) == 1

    @pytest.mark.parametrize("origin", ["https://crm.dreamhub.ai", "https://api.dreamhub.ai"])
    def test_the_shipped_allowlist_refuses_production(self, origin: str) -> None:
        # Unpatched on purpose: this pins the allowlist that ships, not a test stand-in.
        assert not LiveTarget.is_allowed_origin(origin)
        with pytest.raises(HarnessConfigError):
            LiveTarget.from_url(origin + "/api/v1")

    @pytest.mark.usefixtures("dev_allowed")
    def test_accepts_the_allowed_api(self) -> None:
        target = LiveTarget.from_url(DEV_API + "/")

        assert target.base_url == DEV_API
        assert target.origin == DEV_ORIGIN

    @pytest.mark.usefixtures("dev_allowed")
    @pytest.mark.parametrize(
        "url",
        [
            "https://crm.dreamhub.ai/api/v1",  # production
            "https://api.dreamhub.ai/api/v1",
            "https://other.dreamhub.ai/api/v1",  # any other environment on the same domain
            "http://dev.example.test/api/v1",  # not TLS
            "https://dev.example.test.attacker.example/api/v1",  # suffix trick
            "https://user@dev.example.test/api/v1",  # userinfo trick
            "https://dev.example.test:8443/api/v1",  # unexpected port
            "https://dev.example.test/other/path",  # not the API
        ],
    )
    def test_refuses_anything_else(self, url: str) -> None:
        with pytest.raises(HarnessConfigError):
            LiveTarget.from_url(url)


class TestApiToken:
    def test_reads_an_owner_only_file(self, tmp_path: Path) -> None:
        path = tmp_path / "token"
        path.write_text(TOKEN + "\n")
        path.chmod(0o600)

        assert ApiToken.from_file(path).reveal() == TOKEN

    def test_refuses_a_file_others_can_read(self, tmp_path: Path) -> None:
        path = tmp_path / "token"
        path.write_text(TOKEN)
        path.chmod(0o644)

        with pytest.raises(HarnessConfigError, match="0600"):
            ApiToken.from_file(path)

    def test_reads_the_environment_when_no_file_is_given(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("DREAMHUB_LIVE_TOKEN_FILE", raising=False)
        monkeypatch.setenv("DREAMHUB_LIVE_TOKEN", TOKEN)

        assert ApiToken.from_environment().reveal() == TOKEN

    def test_never_shows_the_value(self) -> None:
        token = ApiToken(TOKEN)

        assert TOKEN not in repr(token)
        assert TOKEN not in str(token)
        assert TOKEN not in f"{token}"


class TestSecretScanner:
    def test_finds_the_token_and_its_distinctive_prefix(self) -> None:
        scanner = SecretScanner(ApiToken(TOKEN))

        assert scanner.findings("Authorization: Bearer " + TOKEN)
        assert scanner.findings("key starts with " + TOKEN[:16])

    def test_a_clean_log_has_no_findings(self) -> None:
        scanner = SecretScanner(ApiToken(TOKEN))

        assert scanner.findings("POST companies/filter answered 200; 12 records") == []

    def test_redacts_the_token_and_its_prefix(self) -> None:
        scanner = SecretScanner(ApiToken(TOKEN))

        redacted = scanner.redact(f"Bearer {TOKEN} then {TOKEN[:12]}")

        assert TOKEN[:12] not in redacted
        assert redacted == "Bearer *** then ***"


class TestRecordLedger:
    def test_balanced_when_every_created_record_is_deleted(self) -> None:
        ledger = RecordLedger(run_marker="live-abc")
        ledger.created("companies", ["c1", "c2"])
        ledger.deleted("companies", ["c1", "c2"])

        assert ledger.leftovers() == {}
        assert ledger.summary() == {"companies": {"created": 2, "deleted": 2}}

    def test_reports_what_was_not_deleted(self) -> None:
        ledger = RecordLedger(run_marker="live-abc")
        ledger.created("companies", ["c1", "c2"])
        ledger.deleted("companies", ["c1"])

        assert ledger.leftovers() == {"companies": ["c2"]}

    def test_marker_names_this_run_only(self) -> None:
        first, second = RecordLedger.for_new_run(), RecordLedger.for_new_run()

        assert first.run_marker != second.run_marker
        assert first.run_marker.startswith("airflow-live-")


class _Upstream(http.server.BaseHTTPRequestHandler):
    """A stand-in API that records what reached it."""

    received: list[tuple[str, str, bytes, str | None]] = []

    def do_GET(self) -> None:  # noqa: N802 (http.server API)
        self._answer()

    def do_POST(self) -> None:  # noqa: N802
        self._answer()

    def _answer(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        self.received.append((self.command, self.path, body, self.headers.get("Authorization")))
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ok": true}')

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 (http.server API)
        del format, args


@pytest.fixture
def upstream() -> Iterator[str]:
    _Upstream.received = []
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Upstream)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


class TestRateLimitProxy:
    def test_answers_429_with_retry_after_then_forwards(self, upstream: str) -> None:
        with RateLimitProxy(upstream=upstream, refusals=2, retry_after_seconds=1) as proxy:
            answers = [httpx.get(f"{proxy.url}/companies/x") for _ in range(3)]

        assert [a.status_code for a in answers] == [429, 429, 200]
        assert answers[0].headers["Retry-After"] == "1"
        assert answers[2].json() == {"ok": True}
        assert answers[2].headers.get_list("content-type") == ["application/json"]
        assert proxy.refused == 2
        assert proxy.forwarded == 1

    def test_forwards_a_post_with_its_body_and_token(self, upstream: str) -> None:
        with RateLimitProxy(upstream=upstream, refusals=0, retry_after_seconds=1) as proxy:
            answer = httpx.post(
                f"{proxy.url}/api/v1/companies/filter?page=2",
                json={"filters": {}},
                headers={"Authorization": f"Bearer {TOKEN}"},
            )

        assert answer.status_code == 200
        [(method, path, body, authorization)] = _Upstream.received
        assert (method, path, authorization) == ("POST", "/api/v1/companies/filter?page=2", f"Bearer {TOKEN}")
        assert json.loads(body) == {"filters": {}}

    @staticmethod
    def _raw_get(proxy: RateLimitProxy, target: str) -> int:
        """Send ``target`` as the request line verbatim (httpx would turn it into a path)."""
        port = int(proxy.url.rsplit(":", 1)[1])
        connection = http.client.HTTPConnection("127.0.0.1", port)
        try:
            connection.request("GET", target, headers={"Authorization": f"Bearer {TOKEN}"})
            return connection.getresponse().status
        finally:
            connection.close()

    def test_refuses_an_absolute_url_aimed_at_another_host(self, upstream: str) -> None:
        with RateLimitProxy(upstream=upstream, refusals=0, retry_after_seconds=1) as proxy:
            status = self._raw_get(proxy, "http://elsewhere.example/steal")

        assert status == 400
        assert _Upstream.received == []

    def test_a_double_slash_path_stays_on_the_upstream(self, upstream: str) -> None:
        # Recent Python releases fold a leading `//` into one `/` before the proxy sees it (a path on the
        # upstream); without that, the proxy refuses it. Either way nothing goes to another host.
        with RateLimitProxy(upstream=upstream, refusals=0, retry_after_seconds=1) as proxy:
            status = self._raw_get(proxy, "//elsewhere.example/steal")

        received = [path for _, path, _, _ in _Upstream.received]
        assert (status, received) in [(200, ["/elsewhere.example/steal"]), (400, [])]

    def test_writes_its_counts_for_the_harness(self, upstream: str, tmp_path: Path) -> None:
        stats = tmp_path / "stats.json"
        with RateLimitProxy(upstream=upstream, refusals=1, retry_after_seconds=1, stats_path=stats) as proxy:
            for _ in range(2):
                httpx.get(f"{proxy.url}/companies/x")

        assert json.loads(stats.read_text()) == {"refused": 1, "forwarded": 1}
        assert list(tmp_path.iterdir()) == [stats]
