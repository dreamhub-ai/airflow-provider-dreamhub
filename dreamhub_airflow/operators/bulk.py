"""Create, update, upsert or delete Dreamhub records in bulk."""

from __future__ import annotations

import json
import re
from collections.abc import Sequence
from typing import Any

from dreamhub_airflow._compat import BaseOperator
from dreamhub_airflow.files import FILE_FORMATS, RecordFile
from dreamhub_airflow.hooks.dreamhub import (
    BULK_OPERATIONS,
    DEFAULT_BULK_BATCH,
    MAX_BULK_BATCH,
    NEW_RECORD_KEY,
    BulkResult,
    DreamhubBulkError,
    DreamhubHook,
    new_record_key,
)

# XCom is for small values: the task returns samples, and ``results_path`` holds everything.
MAX_FAILURES_IN_XCOM = 100
MAX_IDS_IN_XCOM = 1000
_NEW_RECORD = re.compile(
    re.escape(NEW_RECORD_KEY).replace(r"\{number\}", r"(?P<number>\d+)").replace(r"\{position\}", r"(?P<position>\d+)")
)


class DreamhubRecordsFailedError(Exception):
    """Some records were not written and the operator was asked to fail on that."""


class DreamhubBulkOperator(BaseOperator):
    """Write records to Dreamhub in bulk and report which ones failed and why.

    Records come from ``records`` (a list of objects, e.g. ``task.output`` of an upstream task,
    or a JSON array as text, e.g. ``"{{ ti.xcom_pull('upstream') | tojson }}"``) or from
    ``input_path`` (an NDJSON or CSV file, local or in object storage). In CSV every value is
    text and an empty cell is null.

    :param entity: Entity type, e.g. ``companies``.
    :param operation: ``create``, ``update``, ``upsert`` or ``delete``. ``delete`` soft-deletes
        one record per request; ``batch_size`` and ``override`` do not apply to it.
    :param records: The records. Templated.
    :param input_path: A file of records instead of ``records``. Templated.
    :param input_format: ``ndjson`` (default) or ``csv``.
    :param match_field: For ``update``, ``upsert`` and ``delete``: find each record's Dreamhub
        id by this field (any filterable field, custom fields included) instead of requiring
        an ``id``. Values are compared as text, so ``"42"`` from a CSV file matches a stored
        ``42``. A value no record holds fails that record (``update``/``delete``) or creates
        a new one (``upsert``); a value several records hold, or an empty value, fails that record.
    :param override: False (default) never clears a stored value with a null. True writes
        nulls over stored values.
    :param batch_size: Records per API request, 1 to 500 (default 200).
    :param fail_on_partial: Fail the task when any record failed (default True).
    :param results_path: Also write the full result, every failure and its reason included, to
        this path as JSON. The task's return value holds at most 100 failures and 1,000 ids,
        and the task log shows only failure keys, never record values or reasons.

    Failures are keyed by the record's Dreamhub id, or by its position (0-based) in the input.

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
        records: list[dict[str, Any]] | str | None = None,
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
        if input_format not in FILE_FORMATS:
            raise ValueError(f"input_format must be one of {', '.join(FILE_FORMATS)}")
        if not 1 <= batch_size <= MAX_BULK_BATCH:
            raise ValueError(f"batch_size must be between 1 and {MAX_BULK_BATCH}")
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
        unmatched: list[dict[str, Any]] = []
        positions = list(range(len(records)))
        try:
            if self.match_field:
                records, positions, unmatched = self._attach_ids(hook, records)
            result = hook.bulk(
                self.entity,
                self.operation,
                records,
                batch_size=self.batch_size,
                override=self.override,
                match_existing=self.match_existing,
            )
        except DreamhubBulkError as error:
            # Keep what was learned before the run stopped, in the caller's own positions, then fail.
            stopped = DreamhubBulkError(
                self._in_input_terms(error.result, positions, unmatched),
                [positions[i] for i in error.unknown],
                [positions[i] for i in error.not_attempted],
                error.cause,
            )
            self._finish(stopped.result, unknown=stopped.unknown, not_attempted=stopped.not_attempted)
            raise stopped from error.cause
        finally:
            hook.close()
        return self._report(self._in_input_terms(result, positions, unmatched))

    def _load_records(self) -> list[dict[str, Any]]:
        if self.records is None:
            source = RecordFile(self.input_path, self.input_format, conn_id=self.storage_conn_id)
            return list(source.read())
        records = self.records
        if isinstance(records, str):
            # A rendered template is text unless the DAG renders native objects.
            try:
                records = json.loads(records)
            except ValueError as error:
                raise TypeError("records must be a list of objects or a JSON array of objects") from error
        if not isinstance(records, (list, tuple)) or not all(isinstance(r, dict) for r in records):
            raise TypeError("records must be a list of objects or a JSON array of objects")
        return list(records)

    def _attach_ids(
        self, hook: DreamhubHook, records: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], list[int], list[dict[str, Any]]]:
        """Give each record the id of the one Dreamhub record holding its ``match_field`` value.

        Returns the records to send, the input position of each, and the failures of records
        that could not be matched. Failures name the input position, never the value, so match
        values (often emails or account numbers) stay out of logs and task results.
        """
        field = self.match_field
        if field is None:
            return records, list(range(len(records))), []
        matches = hook.resolve_ids(self.entity, field, (r.get(field) for r in records))
        ready: list[dict[str, Any]] = []
        positions: list[int] = []
        failed: list[dict[str, Any]] = []
        for position, record in enumerate(records):
            key = hook.match_key(record.get(field))
            ids = matches.get(key, []) if key is not None else []
            if key is None:
                failed.append({"key": position, "error": f"record has no usable {field} value"})
            elif len(ids) == 1:
                ready.append({**record, "id": ids[0]})
                positions.append(position)
            elif len(ids) > 1:
                failed.append({"key": position, "error": f"{len(ids)} records have this {field} value"})
            elif self.operation == "upsert":
                # No record holds the value, so this one is new. An id the caller sent along
                # would make the API update that record instead; the match decides, not the id.
                ready.append({k: v for k, v in record.items() if k != "id"})
                positions.append(position)
            else:
                failed.append({"key": position, "error": f"no record has this {field} value"})
        return ready, positions, failed

    @staticmethod
    def _in_input_terms(result: BulkResult, positions: list[int], unmatched: list[dict[str, Any]]) -> BulkResult:
        """Rewrite position keys from the list sent to the API into positions in the caller's input."""
        failures = list(unmatched)
        for failure in result.failures:
            key = failure["key"]
            if isinstance(key, int) and 0 <= key < len(positions):
                failure = {**failure, "key": positions[key]}
            elif isinstance(key, str):
                found = _NEW_RECORD.fullmatch(key)
                if found and int(found["position"]) < len(positions):
                    failure = {
                        **failure,
                        "key": new_record_key(int(found["number"]), positions[int(found["position"])]),
                    }
            failures.append(failure)
        return BulkResult(operation=result.operation, succeeded_ids=result.succeeded_ids, failures=failures)

    def _finish(
        self, result: BulkResult, *, unknown: list[int] | None = None, not_attempted: list[int] | None = None
    ) -> dict[str, Any]:
        """Write the full result where asked and log what happened, without record values or reasons.

        For a run that stopped part-way, the summary also lists the input positions whose
        outcome is unknown and those never attempted.
        """
        summary = result.to_dict()
        if unknown is not None:
            summary["unknown_positions"] = unknown
            summary["not_attempted_positions"] = not_attempted or []
        if self.results_path:
            RecordFile(self.results_path, "ndjson", conn_id=self.storage_conn_id).write([summary])
        self.log.info(
            "Dreamhub %s %s: %d succeeded, %d failed", self.operation, self.entity, result.succeeded, result.failed
        )
        if result.failures:
            self.log.warning("Failed records (Dreamhub id or input position): %s", [f["key"] for f in result.failures])
        return summary

    def _report(self, result: BulkResult) -> dict[str, Any]:
        summary = self._finish(result)
        if result.failed and self.fail_on_partial:
            where = f"see {self.results_path} and the task log" if self.results_path else "see the task log"
            raise DreamhubRecordsFailedError(
                f"{result.failed} of {result.succeeded + result.failed} records failed; {where}"
            )
        summary["failures"] = summary["failures"][:MAX_FAILURES_IN_XCOM]
        summary["succeeded_ids"] = summary["succeeded_ids"][:MAX_IDS_IN_XCOM]
        return summary
