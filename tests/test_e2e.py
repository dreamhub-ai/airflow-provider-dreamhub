"""Runs against a real Dreamhub environment when DREAMHUB_E2E_URL and DREAMHUB_E2E_TOKEN are set.

Read-only: it never writes to the environment.
"""

from __future__ import annotations

import json
import os

import pytest

from dreamhub_airflow.hooks.dreamhub import DreamhubHook

pytestmark = pytest.mark.e2e


@pytest.fixture
def live_hook(monkeypatch):
    url, token = os.environ.get("DREAMHUB_E2E_URL"), os.environ.get("DREAMHUB_E2E_TOKEN")
    if not (url and token):
        pytest.skip("DREAMHUB_E2E_URL and DREAMHUB_E2E_TOKEN are not set")
    monkeypatch.setenv(
        "AIRFLOW_CONN_DREAMHUB_E2E", json.dumps({"conn_type": "dreamhub", "host": url, "password": token})
    )
    hook = DreamhubHook(dreamhub_conn_id="dreamhub_e2e")
    yield hook
    hook.close()


def test_connection_succeeds(live_hook):
    assert live_hook.test_connection()[0]


@pytest.mark.parametrize("entity", ["companies", "contracts", "revenue_items"])
def test_reads_a_complete_page_of_each_flow_entity(live_hook, entity):
    rows = live_hook.get_records(entity, {"id": {"not_null": True}}, page_size=5)

    assert all("id" in row for row in rows)
