"""Where the live harness may send requests, and the API token it sends."""

from __future__ import annotations

import hashlib
import os
import stat
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit

# The only Dreamhub API the harness writes to: Dreamhub's development environment, matched as an
# exact origin (scheme and host). Kept as a SHA-256 so this public repository does not publish
# the environment's address; maintainers get the URL from the team. The same gateway serves other
# names too, so nothing looser than the whole origin is safe.
ALLOWED_ORIGIN_SHA256 = frozenset({"820eb7fa5601286c8a55e7b770e8c259b5cbcb35aa104e0774fe2c896ac96fa3"})
API_PATH = "/api/v1"

TOKEN_FILE_VARIABLE = "DREAMHUB_LIVE_TOKEN_FILE"
TOKEN_VARIABLE = "DREAMHUB_LIVE_TOKEN"
URL_VARIABLE = "DREAMHUB_LIVE_URL"


class HarnessConfigError(Exception):
    """The live harness is not configured to run safely; nothing was sent."""


@dataclass(frozen=True)
class LiveTarget:
    """The Dreamhub API base URL the harness runs against, checked against the allowlist."""

    base_url: str

    @property
    def origin(self) -> str:
        parts = urlsplit(self.base_url)
        return f"{parts.scheme}://{parts.netloc}"

    @classmethod
    def from_url(cls, url: str) -> LiveTarget:
        parts = urlsplit(url.strip().rstrip("/"))
        origin = f"{parts.scheme}://{parts.netloc}"
        if parts.username is not None or parts.password is not None or parts.port is not None:
            raise HarnessConfigError("the live API URL must not carry credentials or a port")
        if not cls.is_allowed_origin(origin):
            raise HarnessConfigError(
                f"the live harness only runs against Dreamhub's development environment; refusing {origin}"
            )
        if parts.path != API_PATH or parts.query or parts.fragment:
            raise HarnessConfigError(f"the live API URL must be {origin}{API_PATH}")
        return cls(base_url=f"{origin}{API_PATH}")

    @staticmethod
    def is_allowed_origin(origin: str) -> bool:
        return hashlib.sha256(origin.encode()).hexdigest() in ALLOWED_ORIGIN_SHA256

    @classmethod
    def from_environment(cls) -> LiveTarget:
        url = os.environ.get(URL_VARIABLE)
        if not url:
            raise HarnessConfigError(f"set {URL_VARIABLE}")
        return cls.from_url(url)


@dataclass(frozen=True)
class ApiToken:
    """A Dreamhub API token. Its value is only ever handed out by :meth:`reveal`."""

    _value: str = field(repr=False)

    def __post_init__(self) -> None:
        if not self._value or any(c.isspace() for c in self._value):
            raise HarnessConfigError("the API token is empty or malformed")

    def __str__(self) -> str:
        return "ApiToken(<hidden>)"

    def __format__(self, format_spec: str) -> str:
        return str(self)

    def reveal(self) -> str:
        return self._value

    @classmethod
    def from_file(cls, path: Path) -> ApiToken:
        """Read the token from a file only its owner can read or write (mode 0600 or stricter)."""
        mode = stat.S_IMODE(path.stat().st_mode)
        if mode & (stat.S_IRWXG | stat.S_IRWXO):
            raise HarnessConfigError(f"the token file must be readable by its owner only (chmod 0600); it is {mode:o}")
        return cls(path.read_text().strip())

    @classmethod
    def from_environment(cls) -> ApiToken:
        """The token from the file named by ``DREAMHUB_LIVE_TOKEN_FILE``, else from ``DREAMHUB_LIVE_TOKEN``."""
        path = os.environ.get(TOKEN_FILE_VARIABLE)
        if path:
            return cls.from_file(Path(path).expanduser())
        value = os.environ.get(TOKEN_VARIABLE)
        if not value:
            raise HarnessConfigError(f"set {TOKEN_FILE_VARIABLE} (a 0600 file) or {TOKEN_VARIABLE}")
        return cls(value.strip())
