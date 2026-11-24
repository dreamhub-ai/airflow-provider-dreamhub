"""Create, update, upsert or delete Dreamhub records in bulk."""

from __future__ import annotations

import json
from typing import Any

from dreamhub_airflow._compat import BaseOperator, Context
from dreamhub_airflow.files import FILE_FORMATS, RecordFile
from dreamhub_airflow.hooks.dreamhub import (
    BULK_OPERATIONS,
    DEFAULT_BULK_BATCH,
    MAX_BULK_BATCH,
    BulkResult,
    DreamhubBulkError,
    DreamhubHook,
)
from dreamhub_airflow.records import JsonValue, Record

# XCom is for small values: the task returns samples, and ``results_path`` holds everything.
MAX_FAILURES_IN_XCOM = 100
MAX_IDS_IN_XCOM = 1000


class DreamhubRecordsFailedError(Exception):
    """Some records were not written and the operator was asked to fail on that.

    ``summary`` is the task's full result (every failure and its reason, keyed by the record's id, or
    by its input position when it has none), the value the task would have returned. The reasons
    are Dreamhub's own messages, so they are kept off the message, which ends up in the task log.
    """

    def __init__(self, message: str, summary: Record | None = None) -> None:
        super().__init__(message)
        self.summary = summary or {}

    def __reduce__(self) -> tuple[object, ...]:
        return (type(self), (str(self), self.summary))


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
        An ``upsert`` with ``match_field`` updates each matched record with the values sent, as
        ``update`` does, and upserts only the records no Dreamhub record matched.
    :param override: False (default) never clears a stored value with a null. True writes
        nulls over stored values. An ``upsert`` without ``match_field`` keeps Dreamhub's own
        upsert rule: with False, a record the API matches has only its empty fields filled.
    :param batch_size: Records per API request, 1 to 500 (default 200).
    :param fail_on_partial: Fail the task when any record failed (default True).
    :param results_path: Also write the full result, every failure and its reason included, to
        this path as JSON. The task's return value holds at most 100 failures and 1,000 ids,
        and the task log shows only failure keys, never record values or reasons.

    Failures are keyed by the record's Dreamhub id, or by its position (0-based) in the input.

    Retries: records written before a task fails stay written. Running ``create`` again creates
    them again; for loads that may be retried, use ``upsert`` with ``match_field``.
    """

    # Unannotated: Airflow 2 and 3 declare this attribute with different types.
    template_fields = ("entity", "records", "input_path", "results_path")
    ui_color = "#fdf2e8"

    def __init__(
        self,
        *,
        entity: str,
        operation: str,
        records: list[Record] | str | None = None,
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
        # Airflow's own operator arguments (task_id, retries, ...), forwarded unchanged: they are
        # BaseOperator's to type, so this is the only `Any` in this operator.
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

    def execute(self, context: Context) -> Record:
        records = self._load_records()
        hook = DreamhubHook(dreamhub_conn_id=self.dreamhub_conn_id)
        unmatched: list[Record] = []
        positions = list(range(len(records)))
        matched: int | None = None
        try:
            if self.match_field:
                records, positions, unmatched, matched = self._attach_ids(hook, records)
            result = self._write(hook, records, matched)
        except BaseException as error:
            # Keep what was learned before the run stopped, in the caller's own positions. An error
            # is re-raised with those positions; Airflow stopping the task is re-raised unchanged.
            partial = error if isinstance(error, DreamhubBulkError) else getattr(error, "dreamhub_partial", None)
            if not isinstance(partial, DreamhubBulkError):
                raise
            stopped = DreamhubBulkError(
                self._in_input_terms(partial.result, positions, unmatched),
                [positions[i] for i in partial.unknown],
                [positions[i] for i in partial.not_attempted],
                partial.cause,
            )
            if partial is not error:
                # Airflow's own exception carries the report in the caller's positions too.
                error.dreamhub_partial = stopped  # type: ignore[attr-defined]
            try:
                self._finish(stopped.result, unknown=stopped.unknown, not_attempted=stopped.not_attempted)
            except Exception:
                # The report is secondary: it must never replace the error that stopped the run.
                self.log.exception("Could not write the partial result")
            if partial is error:
                raise stopped from partial.cause
            raise
        finally:
            hook.close()
        return self._report(self._in_input_terms(result, positions, unmatched))

    def _load_records(self) -> list[Record]:
        if self.records is None:
            if self.input_path is None:  # __init__ requires one of the two
                raise ValueError("pass records or input_path")
            source = RecordFile(self.input_path, self.input_format, conn_id=self.storage_conn_id)
            loaded = list(source.read())
            # Checked before anything is sent, as for in-memory records: a bad line found mid-run
            # would leave earlier batches written, and a retried create would write them again.
            if not all(isinstance(record, dict) for record in loaded):
                raise TypeError("input_path must hold one JSON object per record")
            return loaded
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
        self, hook: DreamhubHook, records: list[Record]
    ) -> tuple[list[Record], list[int], list[Record], int]:
        """Give each record the id of the one Dreamhub record holding its ``match_field`` value.

        Returns the records to send, the input position of each, the failures of records that
        could not be matched, and how many of the records to send were matched. The matched
        records come first and, for an upsert, the new ones after them (see :meth:`_parts`).
        Failures name the input position, never the value, so match values (often emails or
        account numbers) stay out of logs and task results.
        """
        field = self.match_field
        if field is None:
            return records, list(range(len(records))), [], len(records)
        matches = hook.resolve_ids(self.entity, field, (r.get(field) for r in records))
        ready: list[Record] = []
        positions: list[int] = []
        new: list[Record] = []
        new_positions: list[int] = []
        failed: list[Record] = []
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
                new.append({k: v for k, v in record.items() if k != "id"})
                new_positions.append(position)
            else:
                failed.append({"key": position, "error": f"no record has this {field} value"})
        return ready + new, positions + new_positions, failed, len(ready)

    def _parts(self, records: list[Record], matched: int | None) -> list[tuple[str, list[Record]]]:
        """The requests that write ``records``: each operation with its records, in the order given.

        An upsert by ``match_field`` updates the records it matched and upserts only the new ones:
        the API's upsert fills just the empty fields of an existing record, and a record the
        pipeline matched itself is meant to take the values sent. ``matched`` is the number of
        matched records, which :meth:`_attach_ids` puts first.
        """
        if self.operation != "upsert" or matched is None:
            return [(self.operation, records)]
        parts = [("update", records[:matched]), ("upsert", records[matched:])]
        # With nothing to send, one empty request still runs the hook's checks (connection, entity),
        # as it does for every other operation.
        return [(operation, part) for operation, part in parts if part] or [(self.operation, records)]

    def _write(self, hook: DreamhubHook, records: list[Record], matched: int | None) -> BulkResult:
        """Write ``records`` and report by their positions in ``records``, whatever requests it took.

        ``matched`` is the number of leading records matched by ``match_field``, or None without it.
        A run that stops in any request raises one report covering all of them: what earlier
        requests wrote, what was in flight, and every record not yet attempted.
        """
        result = BulkResult(operation=self.operation)
        offset = 0
        for index, (operation, part) in enumerate(self._parts(records, matched)):
            in_records = list(range(offset, offset + len(part)))
            try:
                written = hook.bulk(
                    self.entity,
                    operation,
                    part,
                    batch_size=self.batch_size,
                    override=self.override,
                    match_existing=self.match_existing,
                )
            except BaseException as error:
                partial = error if isinstance(error, DreamhubBulkError) else getattr(error, "dreamhub_partial", None)
                if not isinstance(partial, DreamhubBulkError):
                    if index == 0:
                        raise
                    # Stopped before this request reported anything; earlier requests did write.
                    partial = DreamhubBulkError(BulkResult(operation=operation), [], list(range(len(part))), error)
                self._add(result, partial.result, in_records)
                DreamhubBulkError(
                    result,
                    [in_records[i] for i in partial.unknown],
                    [in_records[i] for i in partial.not_attempted] + list(range(offset + len(part), len(records))),
                    partial.cause,
                ).propagate()
            self._add(result, written, in_records)
            offset += len(part)
        return result

    @classmethod
    def _add(cls, result: BulkResult, part: BulkResult, in_records: list[int]) -> None:
        """Add one request's outcome to ``result``, its position keys rewritten to ``in_records``."""
        shifted = cls._in_input_terms(part, in_records, [])
        result.succeeded_ids.extend(shifted.succeeded_ids)
        result.failures.extend(shifted.failures)

    @staticmethod
    def _in_input_terms(result: BulkResult, positions: list[int], unmatched: list[Record]) -> BulkResult:
        """Rewrite position keys from the list sent to the API into positions in the caller's input."""
        failures = list(unmatched)
        for failure in result.failures:
            key = failure["key"]
            if isinstance(key, int) and 0 <= key < len(positions):
                failure = {**failure, "key": positions[key]}
            elif (parsed := BulkResult.parse_new_record_key(key)) is not None and parsed[1] < len(positions):
                failure = {**failure, "key": BulkResult.new_record_key(parsed[0], positions[parsed[1]])}
            failures.append(failure)
        return BulkResult(operation=result.operation, succeeded_ids=result.succeeded_ids, failures=failures)

    def _finish(
        self, result: BulkResult, *, unknown: list[int] | None = None, not_attempted: list[int] | None = None
    ) -> Record:
        """Write the full result where asked and log what happened, without record values or reasons.

        For a run that stopped part-way, the summary also lists the input positions whose
        outcome is unknown and those never attempted.
        """
        summary = result.to_dict()
        if unknown is not None:
            unknown_positions: list[JsonValue] = [*unknown]
            not_attempted_positions: list[JsonValue] = [*(not_attempted or [])]
            summary["unknown_positions"] = unknown_positions
            summary["not_attempted_positions"] = not_attempted_positions
        if self.results_path:
            RecordFile(self.results_path, "ndjson", conn_id=self.storage_conn_id).write([summary])
        self.log.info(
            "Dreamhub %s %s: %d succeeded, %d failed", self.operation, self.entity, result.succeeded, result.failed
        )
        if result.failures:
            self.log.warning("Failed records (Dreamhub id or input position): %s", [f["key"] for f in result.failures])
        if unknown is not None:
            # Positions only, never values: what a rerun must check before writing again.
            self.log.warning(
                "Run stopped. Input positions that may or may not have been written: %s; not attempted: %s",
                unknown,
                not_attempted or [],
            )
        return summary

    def _report(self, result: BulkResult) -> Record:
        summary = self._finish(result)
        if result.failed and self.fail_on_partial:
            where = f"see {self.results_path} and the task log" if self.results_path else "see the task log"
            raise DreamhubRecordsFailedError(
                f"{result.failed} of {result.succeeded + result.failed} records failed; {where}", summary
            )
        failures, ids = summary["failures"], summary["succeeded_ids"]
        if isinstance(failures, list):
            summary["failures"] = failures[:MAX_FAILURES_IN_XCOM]
        if isinstance(ids, list):
            summary["succeeded_ids"] = ids[:MAX_IDS_IN_XCOM]
        return summary
