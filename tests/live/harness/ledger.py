"""Count what a live run created in Dreamhub and what it deleted again."""

from __future__ import annotations

import uuid
from collections import defaultdict


class RecordLedger:
    """Every record a run creates carries the run's marker; the ledger checks each one is deleted again."""

    def __init__(self, run_marker: str) -> None:
        self.run_marker = run_marker
        self._created: dict[str, set[str]] = defaultdict(set)
        self._deleted: dict[str, set[str]] = defaultdict(set)

    @classmethod
    def for_new_run(cls) -> RecordLedger:
        return cls(run_marker=f"airflow-live-{uuid.uuid4().hex[:12]}")

    def created(self, entity: str, ids: list[str]) -> None:
        self._created[entity].update(ids)

    def deleted(self, entity: str, ids: list[str]) -> None:
        self._deleted[entity].update(ids)

    def created_ids(self, entity: str) -> list[str]:
        return sorted(self._created[entity])

    def leftovers(self) -> dict[str, list[str]]:
        """Records created but not deleted, by entity."""
        left = {entity: sorted(ids - self._deleted[entity]) for entity, ids in self._created.items()}
        return {entity: ids for entity, ids in left.items() if ids}

    def summary(self) -> dict[str, dict[str, int]]:
        return {
            entity: {"created": len(ids), "deleted": len(ids & self._deleted[entity])}
            for entity, ids in sorted(self._created.items())
        }
