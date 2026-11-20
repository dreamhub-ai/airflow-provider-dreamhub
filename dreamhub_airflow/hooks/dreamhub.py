"""Talk to the Dreamhub REST API: paged reads, bulk writes, and rate-limit handling."""

from __future__ import annotations

import contextlib
import email.utils
import math
import re
import time
from collections.abc import Hashable, Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, ClassVar, NoReturn
from urllib.parse import quote, unquote, urlsplit

import httpx

from dreamhub_airflow import __version__
from dreamhub_airflow._compat import BaseHook
from dreamhub_airflow.entities import EntityRoute

DEFAULT_BASE_URL = "https://crm.dreamhub.ai/api/v1"
# The API refuses larger pages.
MAX_PAGE_SIZE = 100
# The API refuses larger bulk requests.
MAX_BULK_BATCH = 500
DEFAULT_BULK_BATCH = 200
DEFAULT_MAX_RETRIES = 5
# Longest single wait for a rate limit, unless the connection's ``max_retry_wait`` extra says otherwise.
DEFAULT_MAX_RETRY_WAIT_SECONDS = 300.0
# Longest wait between retries of a gateway error or a dropped connection.
MAX_BACKOFF_SECONDS = 60.0
RETRYABLE_STATUSES = frozenset({502, 503, 504})
BULK_OPERATIONS = ("create", "update", "upsert", "delete")
_LOCAL_HOSTS = ("localhost", "127.0.0.1", "::1")


class DreamhubApiError(Exception):
    """The API answered with an error the hook does not handle itself."""

    def __init__(
        self,
        method: str,
        path: str,
        status_code: int,
        detail: str | None = None,
        item_errors: dict[int, str] | None = None,
    ) -> None:
        self.method = method
        self.path = path
        self.status_code = status_code
        self.detail = detail
        # For a request body that is a list: the reason the API gave for each item, by position.
        self.item_errors = item_errors or {}
        message = f"Dreamhub API {method} {path} failed with HTTP {status_code}"
        super().__init__(f"{message}: {detail}" if detail else message)

    def __reduce__(self) -> tuple[Any, ...]:
        return (type(self), (self.method, self.path, self.status_code, self.detail, self.item_errors))


class DreamhubConsistencyError(Exception):
    """A paged read did not return exactly the rows the API said match, so the result is not trustworthy."""


@dataclass
class BulkResult:
    """What a bulk write did, per record.

    ``failures`` holds one entry per record that was not written. Its ``key`` is the record's
    Dreamhub id when it has one, otherwise its position (an int) in the records passed in.
    An upsert with ``match_existing=True`` can also report a new record the API could not
    create as ``"new record <n> of the batch starting at <position>"``: the API matched some
    records of that batch to existing ones before creating the rest, so the position of a
    record it could not create is not known. ``error`` says why the record was not written.
    """

    NEW_RECORD_KEY: ClassVar[str] = "new record {number} of the batch starting at {position}"

    operation: str
    succeeded_ids: list[str] = field(default_factory=list)
    failures: list[dict[str, Any]] = field(default_factory=list)

    @staticmethod
    def record_key(record: dict[str, Any], position: int) -> Any:
        """A failure key: the record's Dreamhub id as text when it has one, else its input position."""
        return str(record["id"]) if DreamhubHook._has_id(record) else position

    @classmethod
    def new_record_key(cls, number: int, position: int) -> str:
        """The failure key of a new record the API could not create and that cannot be traced to one input."""
        return cls.NEW_RECORD_KEY.format(number=number, position=position)

    @staticmethod
    def parse_new_record_key(key: Any) -> tuple[int, int] | None:
        """``(number, position)`` from a key made by :meth:`new_record_key`, else None."""
        found = _NEW_RECORD_PATTERN.fullmatch(key) if isinstance(key, str) else None
        return (int(found["number"]), int(found["position"])) if found else None

    @property
    def succeeded(self) -> int:
        return len(self.succeeded_ids)

    @property
    def failed(self) -> int:
        return len(self.failures)

    def to_dict(self) -> dict[str, Any]:
        return {
            "operation": self.operation,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "succeeded_ids": list(self.succeeded_ids),
            "failures": list(self.failures),
        }


class DreamhubBulkError(Exception):
    """A bulk write stopped part-way. ``result`` holds what was written and what failed before it stopped.

    ``unknown`` lists the input positions of the batch that was in flight: those records may or
    may not have been written. ``not_attempted`` lists the input positions never sent.
    """

    def __init__(self, result: BulkResult, unknown: list[int], not_attempted: list[int], cause: BaseException) -> None:
        self.result = result
        self.unknown = list(unknown)
        self.not_attempted = list(not_attempted)
        self.cause = cause
        super().__init__(
            f"Dreamhub bulk {result.operation} stopped ({type(cause).__name__}). Before it stopped "
            f"{result.succeeded} records were written and {result.failed} failed; "
            f"{_count_with_positions(self.unknown)} may or may not have been written and "
            f"{_count_with_positions(self.not_attempted)} were not attempted"
        )

    def __reduce__(self) -> tuple[Any, ...]:
        return (type(self), (self.result, self.unknown, self.not_attempted, self.cause))

    def propagate(self) -> NoReturn:
        """Raise this report for an error; for Airflow stopping the task, re-raise its own exception
        with this report attached, so the timeout or kill is still what Airflow sees."""
        cause = self.cause
        if isinstance(cause, Exception):
            raise self from cause
        with contextlib.suppress(AttributeError, TypeError):
            cause.dreamhub_partial = self  # type: ignore[attr-defined]
        raise cause


class DreamhubHook(BaseHook):
    """Interact with the Dreamhub REST API.

    The connection's host is the API base URL (``https://crm.dreamhub.ai/api/v1`` when empty)
    and its password is a Dreamhub API token. Optional extras: ``timeout`` (seconds, default 60),
    ``max_retries`` (default 5) and ``max_retry_wait`` (longest single wait for a rate limit,
    seconds, default 300).
    """

    conn_name_attr = "dreamhub_conn_id"
    default_conn_name = "dreamhub_default"
    conn_type = "dreamhub"
    hook_name = "Dreamhub"

    def __init__(self, dreamhub_conn_id: str = default_conn_name, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.dreamhub_conn_id = dreamhub_conn_id
        self._client: httpx.Client | None = None
        self._max_retries = DEFAULT_MAX_RETRIES
        self._max_retry_wait = DEFAULT_MAX_RETRY_WAIT_SECONDS
        self._sleep = time.sleep

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        return {
            "hidden_fields": ["schema", "port", "login"],
            "relabeling": {"host": "API base URL", "password": "API token"},
            "placeholders": {
                "host": DEFAULT_BASE_URL,
                "extra": '{"timeout": 60, "max_retries": 5, "max_retry_wait": 300}',
            },
        }

    def get_conn(self) -> httpx.Client:
        if self._client is None:
            conn = self.get_connection(self.dreamhub_conn_id)
            if not conn.password:
                raise ValueError(f"Connection {self.dreamhub_conn_id!r} has no API token in its password field")
            extras = conn.extra_dejson or {}
            self._max_retries = int(extras.get("max_retries", DEFAULT_MAX_RETRIES))
            self._max_retry_wait = float(extras.get("max_retry_wait", DEFAULT_MAX_RETRY_WAIT_SECONDS))
            base_url = (conn.host or DEFAULT_BASE_URL).rstrip("/")
            if "://" not in base_url:
                base_url = f"https://{base_url}"
            parts = urlsplit(base_url)
            if parts.scheme != "https" and parts.hostname not in _LOCAL_HOSTS:
                self.log.warning(
                    "Connection %s does not use https; the API token is sent unencrypted", self.dreamhub_conn_id
                )
            self._client = httpx.Client(
                base_url=base_url,
                timeout=float(extras.get("timeout", 60)),
                headers={
                    "Authorization": f"Bearer {conn.password}",
                    "Content-Type": "application/json",
                    "User-Agent": f"airflow-provider-dreamhub/{__version__}",
                },
            )
        return self._client

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    def test_connection(self) -> tuple[bool, str]:
        try:
            self.request(
                "POST",
                "companies/filter",
                params={"page": 1, "size": 1},
                json={"filters": {}},
                retry=True,
                allow_not_found=True,
            )
        except Exception as error:  # noqa: BLE001 - the UI shows the message
            return False, str(error)
        finally:
            self.close()
        return True, "Connection to Dreamhub succeeded"

    # ------------------------------------------------------------------ HTTP

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any = None,
        retry: bool = False,
        allow_not_found: bool = False,
    ) -> httpx.Response:
        """Send one request to a path under the API base URL, waiting out rate limits.

        ``path`` must be relative to the base URL: an absolute URL or a ``..`` segment is
        refused, so the API token is only ever sent to the configured API.

        A 429 is retried after the server's ``Retry-After`` (each wait at most
        ``max_retry_wait`` seconds, at most ``max_retries`` times), because the server refused
        the request before doing anything; after that the request fails. A connection that
        could not be opened is retried for every method, since nothing was sent. Gateway
        errors, timeouts and dropped connections are retried only when ``retry`` is true: a
        write the server may already have applied is never resent.
        """
        url = _relative_path(path)
        client = self.get_conn()
        attempt = 0
        while True:
            try:
                response = client.request(method, url, params=params, json=json)
            except httpx.TransportError as error:
                safe_to_resend = retry or isinstance(error, httpx.ConnectError)
                if safe_to_resend and attempt < self._max_retries:
                    attempt += 1
                    self._wait(self._backoff(attempt), method, url, type(error).__name__)
                    continue
                raise
            status = response.status_code
            if status == 429 and attempt < self._max_retries:
                attempt += 1
                self._wait(self._retry_after(response, attempt), method, url, "rate limited")
                continue
            if retry and status in RETRYABLE_STATUSES and attempt < self._max_retries:
                attempt += 1
                self._wait(self._backoff(attempt), method, url, f"HTTP {status}")
                continue
            if status == 404 and allow_not_found:
                return response
            if status >= 400:
                raise DreamhubApiError(method, url, status, _short_detail(response), _item_errors(response))
            return response

    def _wait(self, seconds: float, method: str, path: str, reason: str) -> None:
        self.log.warning("Dreamhub %s %s %s; retrying in %.1fs", method, path, reason, seconds)
        self._sleep(seconds)

    @staticmethod
    def _backoff(attempt: int) -> float:
        return min(2.0**attempt, MAX_BACKOFF_SECONDS)

    def _retry_after(self, response: httpx.Response, attempt: int) -> float:
        """The server's ``Retry-After`` (seconds or an HTTP date), capped; our own backoff when absent or unreadable."""
        value = response.headers.get("Retry-After")
        if not value:
            return self._backoff(attempt)
        try:
            seconds = float(value)
        except ValueError:
            try:
                parsed = email.utils.parsedate_to_datetime(value)
            except (TypeError, ValueError):
                return self._backoff(attempt)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            seconds = (parsed - datetime.now(timezone.utc)).total_seconds()
        if math.isnan(seconds):
            return self._backoff(attempt)
        return min(max(seconds, 0.0), self._max_retry_wait)

    # ------------------------------------------------------------------ reads

    def iter_records(
        self,
        entity: str,
        filters: dict[str, Any] | None = None,
        *,
        sort_field: str = "id",
        page_size: int = MAX_PAGE_SIZE,
        include_additionals: bool = False,
    ) -> Iterator[dict[str, Any]]:
        """Yield every record of ``entity`` matching ``filters``, one page at a time.

        Pages are ordered by ``sort_field`` (``id`` by default, which is unique and stable).
        A record seen on an earlier page is not yielded again. When the read ends, the number
        of records yielded must equal the total the API reported on every page; otherwise the
        data changed under the read and :class:`DreamhubConsistencyError` is raised, so a caller
        never loads a partial table believing it complete.

        Activities cannot be read yet: the API has no read of activities by filter that is
        scoped correctly, so ``entity="activities"`` is refused rather than returning wrong rows.
        """
        if not 1 <= page_size <= MAX_PAGE_SIZE:
            raise ValueError(f"page_size must be between 1 and {MAX_PAGE_SIZE}")
        route = self._route(entity)
        body: dict[str, Any] = {"filters": filters or {}, "sort": {"field": sort_field, "ascending": True}}
        params: dict[str, Any] = {"size": page_size}
        if include_additionals:
            params["include_additionals"] = "true"

        seen: set[str] = set()
        without_id = 0
        totals: set[int] = set()
        page = 1
        while True:
            response = self.request(
                "POST",
                f"{route.path}/filter",
                params={**params, "page": page},
                json=body,
                retry=True,
                allow_not_found=True,
            )
            if response.status_code == 404:
                # The API answers "nothing matches" with 404. On a later page it only means
                # there is nothing more; the count check below still catches a short read.
                if page == 1:
                    totals.add(0)
                break
            payload = response.json()
            rows = _rows(payload, route.items_key)
            # A bare list carries no total or page count: such a read is not count-checked.
            meta = payload if isinstance(payload, dict) else {}
            total = meta.get("total")
            if isinstance(total, int):
                totals.add(total)
            new_on_page = 0
            for row in rows:
                row_id = row.get("id")
                key = str(row_id) if row_id is not None else None
                if key is not None and key in seen:
                    continue
                if key is None:
                    without_id += 1
                else:
                    seen.add(key)
                new_on_page += 1
                yield row
            pages = meta.get("pages")
            # Stop on a short page, on the last page, and on a page that brought nothing new
            # (a server that ignores the page number would otherwise be read forever).
            if len(rows) < page_size or (isinstance(pages, int) and page >= pages) or new_on_page == 0:
                break
            page += 1

        if len(totals) > 1:
            raise DreamhubConsistencyError(
                f"{route.name}: the number of matching records changed during the read ({sorted(totals)}); "
                "run the task again"
            )
        read = len(seen) + without_id
        if totals and read != next(iter(totals)):
            raise DreamhubConsistencyError(
                f"{route.name}: read {read} distinct records but the API reported {next(iter(totals))}; "
                "run the task again"
            )

    def get_records(self, entity: str, filters: dict[str, Any] | None = None, **kwargs: Any) -> list[dict[str, Any]]:
        """All matching records in memory. Prefer :meth:`iter_records` for large reads."""
        return list(self.iter_records(entity, filters, **kwargs))

    def resolve_ids(self, entity: str, match_field: str, values: Iterable[Any]) -> dict[str, list[str]]:
        """Map each value of ``match_field`` to the ids of the records holding it.

        Works for any field the API can filter on, custom fields included. The result is keyed
        by :meth:`match_key` of each value, so ``"42"`` read from a CSV file and ``42`` returned
        by the API are the same value. A value no record holds maps to an empty list; a value
        several records hold maps to all their ids. Values with no key (empty, or a list) are
        left out.
        """
        wanted: dict[str, Any] = {}
        for value in values:
            key = self.match_key(value)
            if key is not None and key not in wanted:
                # Query with the cleaned value: the API compares what it is sent.
                wanted[key] = _query_value(value)
        found: dict[str, list[str]] = {key: [] for key in wanted}
        keys = list(wanted)
        for start in range(0, len(keys), MAX_PAGE_SIZE):
            chunk = [wanted[key] for key in keys[start : start + MAX_PAGE_SIZE]]
            for row in self.iter_records(entity, {match_field: chunk}):
                key = self.match_key(_field_value(row, match_field))
                if key in found:
                    found[key].append(str(row["id"]))
        return found

    @staticmethod
    def match_key(value: Any) -> str | None:
        """The form two values of a match field are compared in, or None when the value cannot be matched.

        Text is compared as written, apart from surrounding spaces; a whole number compares equal
        to its plain text form (``42``, ``42.0`` and ``"42"``); ``true``/``false`` in any case
        compare equal to booleans.
        An empty value, a list or an object has no key.
        """
        if value is None or isinstance(value, (list, dict, set)) or not isinstance(value, Hashable):
            return None
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, float) and value.is_integer():
            return str(int(value))
        text = str(value).strip()
        if text.lower() in ("true", "false"):
            return text.lower()
        return text or None

    @staticmethod
    def _has_id(record: dict[str, Any]) -> bool:
        """Whether the API treats the record as naming an existing one: the same test the API applies
        (an empty or false value, ``"None"`` and blank text name no record)."""
        value = record.get("id")
        return bool(value) and value != "None" and bool(str(value).strip())

    @staticmethod
    def _route(entity: str) -> EntityRoute:
        if "activities" in unquote(entity).strip("/").split("/"):
            raise ValueError(
                "activities cannot be read yet: the API has no correctly scoped read of activities by filter"
            )
        return EntityRoute.for_entity(entity)

    # ------------------------------------------------------------------ writes

    def bulk(
        self,
        entity: str,
        operation: str,
        records: Sequence[dict[str, Any]],
        *,
        batch_size: int = DEFAULT_BULK_BATCH,
        override: bool = False,
        match_existing: bool = True,
    ) -> BulkResult:
        """Create, update, upsert or delete ``records`` in batches.

        ``override=False`` (the default) never clears a stored value with a null; ``True``
        writes nulls over stored values. For ``update`` and ``upsert``, every record that
        names an existing record carries its Dreamhub ``id``. ``delete`` takes records with
        an ``id`` and soft-deletes them one request at a time (the API has no bulk delete
        yet; ``batch_size`` and ``override`` do not apply).

        A batch is never resent after an error the server may already have applied. If the run
        stops on an error, :class:`DreamhubBulkError` is raised carrying everything reported so
        far, the positions whose outcome is unknown and those never attempted. If Airflow stops
        the task (a timeout or a kill), that exception is re-raised unchanged, with the same
        report attached as its ``dreamhub_partial`` attribute.
        """
        if operation not in BULK_OPERATIONS:
            raise ValueError(f"operation must be one of {', '.join(BULK_OPERATIONS)}")
        if not 1 <= batch_size <= MAX_BULK_BATCH:
            raise ValueError(f"batch_size must be between 1 and {MAX_BULK_BATCH}")
        route = EntityRoute.for_entity(entity)
        # A broken connection is a configuration error, raised before anything is attempted.
        self.get_conn()
        result = BulkResult(operation=operation)
        if operation == "delete":
            self._delete_each(route, records, result)
            return result
        for start in range(0, len(records), batch_size):
            batch = list(records[start : start + batch_size])
            try:
                self._write_batch(
                    route, operation, batch, start, result, override=override, match_existing=match_existing
                )
            except BaseException as error:
                end = start + len(batch)
                DreamhubBulkError(result, list(range(start, end)), list(range(end, len(records))), error).propagate()
        return result

    def _write_batch(
        self,
        route: EntityRoute,
        operation: str,
        batch: list[dict[str, Any]],
        offset: int,
        result: BulkResult,
        *,
        override: bool,
        match_existing: bool,
    ) -> None:
        if operation == "update":
            # The API skips update items without an id without reporting them; report them here.
            missing = {i for i, record in enumerate(batch) if not self._has_id(record)}
            for i in sorted(missing):
                result.failures.append({"key": offset + i, "error": "record has no id"})
            positions = [i for i in range(len(batch)) if i not in missing]
        else:
            positions = list(range(len(batch)))
        sent = [batch[i] for i in positions]
        if not sent:
            return

        method, params = {
            "create": ("POST", {}),
            "update": ("PUT", {"override": _flag(override)}),
            "upsert": ("PUT", {"override": _flag(override), "match_existing": _flag(match_existing)}),
        }[operation]
        try:
            response = self.request(method, f"{route.path}/bulk/{operation}", params=params or None, json=sent)
        except DreamhubApiError as error:
            if error.status_code == 500 and error.detail and "failed for all entities" in error.detail:
                # Every record in the batch was rejected; the server reports it as a 500
                # without a reason per record.
                for position, record in zip(positions, sent, strict=True):
                    result.failures.append(
                        {"key": BulkResult.record_key(record, offset + position), "error": error.detail}
                    )
                return
            if error.status_code == 422:
                # The request failed validation, which runs before anything is written, so
                # every record of the batch failed: with its own reason, or because of another's.
                for item, (position, record) in enumerate(zip(positions, sent, strict=True)):
                    if item in error.item_errors:
                        reason = error.item_errors[item]
                    elif error.item_errors:
                        reason = "not written: another record in the same request is invalid"
                    else:
                        reason = error.detail or "HTTP 422"
                    result.failures.append({"key": BulkResult.record_key(record, offset + position), "error": reason})
                return
            raise
        payload = response.json()
        result.succeeded_ids.extend(str(i) for i in payload.get("successfulIds") or [])
        keys = _BatchKeys(sent, positions, offset, server_matched=operation == "upsert" and match_existing)
        reported: set[str] = set()
        for key, message in (payload.get("errors") or {}).items():
            input_key = keys.resolve(str(key))
            reported.add(str(input_key))
            result.failures.append({"key": input_key, "error": str(message)})
        for failed_id in payload.get("failedIds") or []:
            if str(failed_id) not in reported:
                result.failures.append({"key": str(failed_id), "error": "failed"})

    def _delete_each(self, route: EntityRoute, records: Sequence[dict[str, Any]], result: BulkResult) -> None:
        for position, record in enumerate(records):
            key = BulkResult.record_key(record, position)
            if not self._has_id(record):
                result.failures.append({"key": key, "error": "record has no id"})
                continue
            try:
                path = _relative_path(f"{route.path}/{quote(str(key), safe='')}")
            except ValueError:
                result.failures.append({"key": key, "error": "record id is not a valid id"})
                continue
            try:
                self.request("DELETE", path)
            except DreamhubApiError as error:
                if error.status_code == 401:
                    # The token was refused: every later record would fail the same way.
                    result.failures.append({"key": key, "error": error.detail or "HTTP 401"})
                    DreamhubBulkError(result, [], list(range(position + 1, len(records))), error).propagate()
                result.failures.append({"key": key, "error": error.detail or f"HTTP {error.status_code}"})
                continue
            except BaseException as error:
                DreamhubBulkError(result, [position], list(range(position + 1, len(records))), error).propagate()
            result.succeeded_ids.append(key)


class _BatchKeys:
    """Turns the API's error keys for one batch into keys the caller can find in its own input.

    The API keys an error by record id, by the record's position in the list it was sent,
    as ``entity_<id>``, and, for upsert, as ``update_<id>`` or ``create_<n>`` where ``n`` counts
    only the records it created. The list sent can be shorter than the caller's batch (update
    records without an id are held back), so a position is mapped back to the record's id when
    it has one, else to its input position.
    """

    def __init__(self, sent: list[dict[str, Any]], positions: list[int], offset: int, *, server_matched: bool) -> None:
        self.sent = sent
        self.positions = positions
        self.offset = offset
        # With match_existing the API may turn some id-less records into updates before
        # creating the rest, so ``create_<n>`` cannot be traced to one input record.
        self.server_matched = server_matched
        self.new_records = [i for i, record in enumerate(sent) if not DreamhubHook._has_id(record)]
        self.sent_ids = {str(record["id"]) for record in sent if DreamhubHook._has_id(record)}

    def resolve(self, key: str) -> Any:
        for prefix in ("update_", "create_entity_", "entity_"):
            if key.startswith(prefix):
                return key[len(prefix) :]
        if key in self.sent_ids:
            return key
        if key.startswith("create_"):
            number = key[len("create_") :]
            if not number.isdigit():
                return key
            if self.server_matched or int(number) >= len(self.new_records):
                return BulkResult.new_record_key(int(number), self.offset)
            return self._at(self.new_records[int(number)])
        if key.isdigit() and int(key) < len(self.sent):
            return self._at(int(key))
        return key

    def _at(self, index: int) -> Any:
        return BulkResult.record_key(self.sent[index], self.offset + self.positions[index])


# Built from the key format itself, so the two cannot drift apart.
_NEW_RECORD_PATTERN = re.compile(
    re.escape(BulkResult.NEW_RECORD_KEY)
    .replace(re.escape("{number}"), r"(?P<number>\d+)")
    .replace(re.escape("{position}"), r"(?P<position>\d+)")
)


def _query_value(value: Any) -> Any:
    """The form a match value is sent in: text without surrounding spaces, whole floats as ints."""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


def _count_with_positions(positions: list[int]) -> str:
    if not positions:
        return "no records"
    if positions == list(range(positions[0], positions[-1] + 1)):
        where = str(positions[0]) if len(positions) == 1 else f"{positions[0]} to {positions[-1]}"
    else:
        where = ", ".join(str(p) for p in positions[:20]) + (", ..." if len(positions) > 20 else "")
    return f"{len(positions)} records (input positions {where})"


def _relative_path(path: str) -> str:
    """``path`` as a path under the API base URL, refusing anything that would leave it."""
    relative = path.lstrip("/")
    parts = urlsplit(relative)
    if (
        parts.scheme
        or parts.netloc
        or parts.query
        or parts.fragment
        or "?" in relative
        or "#" in relative
        or "\\" in relative
    ):
        raise ValueError(f"path must be relative to the API base URL: {path!r}")
    if any(segment in (".", "..") for segment in parts.path.split("/")):
        raise ValueError(f"path must not contain '.' or '..' segments: {path!r}")
    return relative


def _flag(value: bool) -> str:
    return "true" if value else "false"


def _rows(payload: Any, items_key: str) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return payload
    for key in (items_key, _camel(items_key), "items"):
        rows = payload.get(key)
        if isinstance(rows, list):
            return rows
    raise DreamhubApiError("POST", items_key, 200, "response has no list of records")


def _camel(name: str) -> str:
    head, *rest = name.split("_")
    return head + "".join(part.title() for part in rest)


def _field_value(row: dict[str, Any], name: str) -> Any:
    if name in row:
        return row[name]
    camel = _camel(name)
    if camel in row:
        return row[camel]
    for container in ("additionals", "customFields", "custom_fields"):
        nested = row.get(container)
        if isinstance(nested, dict) and name in nested:
            return nested[name]
    return None


def _short_detail(response: httpx.Response) -> str | None:
    """The API's own short error text, never the request or a full body."""
    try:
        detail = response.json().get("detail")
    except (ValueError, AttributeError):
        return None
    if isinstance(detail, str):
        return detail[:200]
    reasons = list(dict.fromkeys(reason for _, reason in _validation_errors(detail)))
    return "; ".join(reasons)[:200] or None


def _item_errors(response: httpx.Response) -> dict[int, str]:
    """The API's validation reasons for a list body, by the position of the item they concern."""
    try:
        detail = response.json().get("detail")
    except (ValueError, AttributeError):
        return {}
    by_item: dict[int, list[str]] = {}
    for item, reason in _validation_errors(detail):
        if item is not None:
            by_item.setdefault(item, []).append(reason)
    return {item: "; ".join(reasons)[:200] for item, reasons in by_item.items()}


def _validation_errors(detail: Any) -> list[tuple[int | None, str]]:
    """``(item position, "field: message")`` for each entry of a request-validation error.

    Only the field and the message are kept: the entry's ``input`` holds the rejected value,
    which can be personal data, so it is never read.
    """
    if not isinstance(detail, list):
        return []
    errors = []
    for entry in detail:
        if not isinstance(entry, dict) or not isinstance(entry.get("msg"), str):
            continue
        loc = list(entry.get("loc") or [])
        if loc and loc[0] == "body":
            loc = loc[1:]
        item = None
        if loc and isinstance(loc[0], int) and not isinstance(loc[0], bool):
            item, loc = loc[0], loc[1:]
        field = ".".join(str(part) for part in loc)
        errors.append((item, f"{field}: {entry['msg']}" if field else entry["msg"]))
    return errors
