"""Find the API token in text the run produced (task logs, container output)."""

from __future__ import annotations

from pathlib import Path

from tests.live.harness.config import ApiToken

# Long enough to identify this token, short enough to catch a partial copy of it.
PREFIX_LENGTH = 12


class SecretScanner:
    """Reports where a token, or the start of it, appears. Findings never quote the token itself."""

    def __init__(self, token: ApiToken) -> None:
        value = token.reveal()
        self._needles = {value, value[:PREFIX_LENGTH]}

    def findings(self, text: str, source: str = "text") -> list[str]:
        return [f"{source}: line {number}" for number, line in enumerate(text.splitlines(), 1) if self._hit(line)]

    def scan_tree(self, root: Path) -> list[str]:
        """Findings in every file under ``root``."""
        found: list[str] = []
        for path in sorted(p for p in root.rglob("*") if p.is_file()):
            found.extend(self.findings(path.read_text(errors="replace"), str(path.relative_to(root))))
        return found

    def _hit(self, line: str) -> bool:
        return any(needle in line for needle in self._needles)
