"""Reads and cleanup the harness does itself, outside Airflow, to check what the DAGs did."""

from __future__ import annotations

import json
import os
from collections.abc import Generator
from contextlib import contextmanager

from dreamhub_airflow.entities import EntityRoute

# The hook's own "nothing matches" reading, imported rather than copied so the two cannot drift apart.
from dreamhub_airflow.hooks.dreamhub import _NOTHING_MATCHES, MAX_PAGE_SIZE, DreamhubHook, _ErrorBody
from tests.live.harness.config import ApiToken, LiveTarget
from tests.live.harness.ledger import RecordLedger

HOST_CONN_ID = "dreamhub_live_host"


class LiveApi:
    """The live Dreamhub account, read and cleaned up from the host through the provider's own hook."""

    def __init__(self, hook: DreamhubHook) -> None:
        self._hook = hook

    @classmethod
    @contextmanager
    def connect(cls, target: LiveTarget, token: ApiToken) -> Generator[LiveApi]:
        variable = f"AIRFLOW_CONN_{HOST_CONN_ID.upper()}"
        os.environ[variable] = json.dumps(
            {"conn_type": "dreamhub", "host": target.base_url, "password": token.reveal()}
        )
        hook = DreamhubHook(dreamhub_conn_id=HOST_CONN_ID)
        try:
            yield cls(hook)
        finally:
            hook.close()
            os.environ.pop(variable, None)

    def total(self, entity: str) -> int:
        """The number of records of ``entity`` the API reports for an unfiltered read.

        Follows the hook's own reading of the API: "nothing matches" is a 404, which means 0.
        """
        path = f"{EntityRoute.for_entity(entity).path}/filter"
        response = self._hook.request(
            "POST", path, params={"page": 1, "size": 1}, json={"filters": {}}, retry=True, allow_not_found=True
        )
        if response.status_code == 404:
            if _NOTHING_MATCHES.fullmatch(_ErrorBody.read(response).detail or ""):
                return 0
            raise AssertionError(f"POST {path} answered 404 for a route the API should serve")
        payload = response.json()
        total = payload.get("total") if isinstance(payload, dict) else None
        if not isinstance(total, int):
            raise AssertionError(f"POST {path} did not report a total")
        return total

    def companies_by_website(self, websites: list[str]) -> dict[str, list[dict[str, object]]]:
        """Website → the companies holding it (normal reads, so deleted companies are not included)."""
        found: dict[str, list[dict[str, object]]] = {website: [] for website in websites}
        for row in self._hook.iter_records("companies", {"website": websites}, include_additionals=True):
            website = row.get("website")
            if isinstance(website, str) and website in found:
                found[website].append(row)
        return found

    def ids_by_website(self, websites: list[str]) -> list[str]:
        return [str(row["id"]) for rows in self.companies_by_website(websites).values() for row in rows]

    def existing_ids(self, entity: str, ids: list[str]) -> list[str]:
        """Those of ``ids`` a normal read still returns (a soft-deleted record is not returned)."""
        found: list[str] = []
        for start in range(0, len(ids), MAX_PAGE_SIZE):
            chunk = ids[start : start + MAX_PAGE_SIZE]
            found.extend(str(row["id"]) for row in self._hook.iter_records(entity, {"id": chunk}))
        return found

    def delete_companies(self, ids: list[str], ledger: RecordLedger) -> None:
        """Soft-delete companies the run left behind, and count each one deleted."""
        if not ids:
            return
        result = self._hook.bulk("companies", "delete", [{"id": record_id} for record_id in ids])
        ledger.deleted("companies", [str(record_id) for record_id in result.succeeded_ids])
