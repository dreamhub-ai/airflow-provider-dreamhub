"""Create, update, upsert or delete Dreamhub records in bulk."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from dreamhub_airflow._compat import BaseOperator
from dreamhub_airflow.files import RecordFile
from dreamhub_airflow.hooks.dreamhub import BULK_OPERATIONS, DEFAULT_BULK_BATCH, BulkResult, DreamhubHook

# XCom is for small values: the task returns samples, and ``results_path`` holds everything.
MAX_FAILURES_IN_XCOM = 100
MAX_IDS_IN_XCOM = 1000
MAX_FAILURES_LOGGED = 20


class DreamhubBulkFailure(Exception):
    """Some records were not written and the operator was asked to fail on that."""


class DreamhubBulkOperator(BaseOperator):
    """Write records to Dreamhub in bulk and report which ones failed and why.

    Records come from ``records`` (a list, e.g. from XCom) or from ``input_path`` (an NDJSON
    or CSV file, local or in object storage).

    :param entity: Entity type, e.g. ``companies``.
    :param operation: ``create``, ``update``, ``upsert`` or ``delete``.
    :param records: The records. Templated.
    :param input_path: A file of records instead of ``records``. Templated.
    :param match_field: For ``update``, ``upsert`` and ``delete``: find each record's Dreamhub
        id by this field (any filterable field, custom fields included) instead of requiring
        an ``id``. A value no record holds fails that record (``update``/``delete``) or creates
        a new one (``upsert``); a value several records hold fails that record.
    :param override: False (default) never clears a stored value with a null. True writes
        nulls over stored values.
    :param batch_size: Records per API request, 1 to 500 (default 200).
    :param fail_on_partial: Fail the task when any record failed (default True).
    :param results_path: Also write the full result, every failure and id included, to this path
        as JSON. The task's return value holds at most 100 failures and 1,000 ids.

    Retries: records written before a task fails stay written. Running ``create`` again creates
    them again; for loads that may be retried, use ``upsert`` with ``match_field``.
    """

    template_fields: Sequence[str] = ("entity", "records", "input_path", "results_path")
    ui_color = "#fdf2e8"

    def __init__(
        self,
        *,
        entity: str,
        operation: str,
        records: list[dict[str, Any]] | None = None,
        input_path: str | None = None,
        input_format: str = "ndjson",
        match_field: str | None = None,
        override: bool = False,
        match_existing: bool = True,
        batch_size: int = DEFAULT_BULK_BATCH,
        fail_on_partial: bool = True,
        results_path: str | None = None,
        dreamhub_conn_id: str = DreamhubHook.default_conn_name,
        storage_conn_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if operation not in BULK_OPERATIONS:
            raise ValueError(f"operation must be one of {', '.join(BULK_OPERATIONS)}")
        if (records is None) == (input_path is None):
            raise ValueError("pass exactly one of records or input_path")
        if match_field and operation == "create":
            raise ValueError("match_field is for update, upsert and delete")
        self.entity = entity
        self.operation = operation
        self.records = records
        self.input_path = input_path
        self.input_format = input_format
        self.match_field = match_field
        self.override = override
        self.match_existing = match_existing
        self.batch_size = batch_size
        self.fail_on_partial = fail_on_partial
        self.results_path = results_path
        self.dreamhub_conn_id = dreamhub_conn_id
        self.storage_conn_id = storage_conn_id

    def execute(self, context: Any) -> dict[str, Any]:
        records = self._load_records()
        hook = DreamhubHook(dreamhub_conn_id=self.dreamhub_conn_id)
        try:
            unmatched: list[dict[str, Any]] = []
            if self.match_field:
                records, unmatched = self._attach_ids(hook, records)
            result = hook.bulk(
                self.entity,
                self.operation,
                records,
                batch_size=self.batch_size,
                override=self.override,
                match_existing=self.match_existing,
            )
        finally:
            hook.close()
        result.failures[:0] = unmatched
        return self._report(result)

    def _load_records(self) -> list[dict[str, Any]]:
        if self.records is not None:
            return list(self.records)
        source = RecordFile(self.input_path, self.input_format, conn_id=self.storage_conn_id)
        return list(source.read())

    def _attach_ids(
        self, hook: DreamhubHook, records: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Give each record the id of the one Dreamhub record holding its ``match_field`` value.

        A record that cannot be matched is reported under ``<field>=<value>``, since it never
        reaches the API and so has no id or batch position.
        """
        field = self.match_field
        if field is None:
            return records, []
        matches = hook.resolve_ids(self.entity, field, (r.get(field) for r in records))
        ready: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        for record in records:
            value = record.get(field)
            ids = matches.get(value, []) if value is not None else []
            key = f"{field}={value!r}"
            if len(ids) == 1:
                ready.append({**record, "id": ids[0]})
            elif len(ids) > 1:
                failed.append({"key": key, "error": f"{len(ids)} records have this value"})
            elif self.operation == "upsert":
                # No record holds the value, so this one is new. An id the caller sent along
                # would make the API update that record instead; the match decides, not the id.
                ready.append({k: v for k, v in record.items() if k != "id"})
            else:
                failed.append({"key": key, "error": "no record has this value"})
        return ready, failed

    def _report(self, result: BulkResult) -> dict[str, Any]:
        summary = result.to_dict()
        if self.results_path:
            RecordFile(self.results_path, "ndjson", conn_id=self.storage_conn_id).write([summary])
        self.log.info(
            "Dreamhub %s %s: %d succeeded, %d failed", self.operation, self.entity, result.succeeded, result.failed
        )
        # With a results file the log shows a sample; without one, the log is the only full record.
        logged = result.failures[:MAX_FAILURES_LOGGED] if self.results_path else result.failures
        for failure in logged:
            self.log.warning("Record %s failed: %s", failure["key"], failure["error"])
        if result.failed and self.fail_on_partial:
            raise DreamhubBulkFailure(
                f"{result.failed} of {result.succeeded + result.failed} records failed; "
                "see the task log" + (f" and {self.results_path}" if self.results_path else "")
            )
        summary["failures"] = summary["failures"][:MAX_FAILURES_IN_XCOM]
        summary["succeeded_ids"] = summary["succeeded_ids"][:MAX_IDS_IN_XCOM]
        return summary
