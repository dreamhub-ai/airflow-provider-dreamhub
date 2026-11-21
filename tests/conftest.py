from __future__ import annotations

from collections.abc import Iterator

import pytest

from dreamhub_airflow.hooks import dreamhub
from dreamhub_airflow.hooks.dreamhub import DreamhubHook


@pytest.fixture
def sleeps() -> list[float]:
    """Seconds the hook waited, recorded instead of slept."""
    return []


@pytest.fixture
def hook(sleeps: list[float]) -> Iterator[DreamhubHook]:
    instance = DreamhubHook()
    instance._sleep = sleeps.append
    yield instance
    instance.close()


@pytest.fixture(autouse=True)
def _no_real_sleep(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch, sleeps: list[float]) -> None:
    """Operators build their own hooks; they must not sleep for real either.

    Live end-to-end tests keep real waits, so a rate limit there is actually waited out.
    """
    if request.node.get_closest_marker("e2e") is None and request.node.get_closest_marker("airflow_live") is None:
        monkeypatch.setattr(dreamhub.time, "sleep", sleeps.append)
