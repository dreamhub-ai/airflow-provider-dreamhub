"""Collects what the live Airflow harness reports (run markers, cleanup counts, UI links) for the terminal."""

from __future__ import annotations

import pytest

SUMMARY_KEY = pytest.StashKey[list[str]]()


def pytest_configure(config: pytest.Config) -> None:
    config.stash[SUMMARY_KEY] = []


def pytest_terminal_summary(terminalreporter: pytest.TerminalReporter, config: pytest.Config) -> None:
    lines = config.stash.get(SUMMARY_KEY, [])
    if lines:
        terminalreporter.section("Dreamhub live run")
        for line in lines:
            terminalreporter.write_line(line)
