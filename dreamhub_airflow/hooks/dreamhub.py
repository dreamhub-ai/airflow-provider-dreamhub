"""Talk to the Dreamhub REST API: paged reads, bulk writes, and rate-limit handling."""

from __future__ import annotations

import email.utils
import time
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

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
MAX_RETRY_WAIT_SECONDS = 60.0
RETRYABLE_STATUSES = frozenset({502, 503, 504})
BULK_OPERATIONS = ("create", "update", "upsert", "delete")


class DreamhubApiError(Exception):
    """The API answered with an error the hook does not handle itself."""

    def __init__(self, method: str, path: str, status_code: int, detail: str | None = None) -> None:
        self.method = method
        self.path = path
        self.status_code = status_code
        self.detail = detail
        message = f"Dreamhub API {method} {path} failed with HTTP {status_code}"
        super().__init__(f"{message}: {detail}" if detail else message)


class DreamhubConsistencyError(Exception):
    """A paged read did not return exactly the rows the API said match, so the result is not trustworthy."""


@dataclass
class BulkResult:
    """What a bulk write did, per record.

    ``failures`` holds one entry per record that was not written: its ``key`` is the record's
    Dreamhub id when known, otherwise its position in the input, and ``error`` says why.
    """

    operation: str
    succeeded_ids: list[str] = field(default_factory=list)
    failures: list[dict[str, Any]] = field(default_factory=list)

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


class DreamhubHook(BaseHook):
    """Interact with the Dreamhub REST API.

    The connection's host is the API base URL (``https://crm.dreamhub.ai/api/v1`` when empty)
    and its password is a Dreamhub API token. Optional extras: ``timeout`` (seconds, default 60)
    and ``max_retries`` (default 5).
    """

    conn_name_attr = "dreamhub_conn_id"
    default_conn_name = "dreamhub_default"
    conn_type = "dreamhub"
    hook_name = "Dreamhub"

    def __init__(self, dreamhub_conn_id: str = default_conn_name, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.dreamhub_conn_id = dreamhub_conn_id
        self._client: httpx.Client | None = None
        self._max_retries = 5
        self._sleep = time.sleep

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        return {
            "hidden_fields": ["schema", "port", "login"],
            "relabeling": {"host": "API base URL", "password": "API token"},
            "placeholders": {
                "host": DEFAULT_BASE_URL,
                "extra": '{"timeout": 60, "max_retries": 5}',
            },
        }

    def get_conn(self) -> httpx.Client:
        if self._client is None:
            conn = self.get_connection(self.dreamhub_conn_id)
            if not conn.password:
                raise ValueError(f"Connection {self.dreamhub_conn_id!r} has no API token in its password field")
            extras = conn.extra_dejson or {}
            self._max_retries = int(extras.get("max_retries", 5))
            base_url = (conn.host or DEFAULT_BASE_URL).rstrip("/")
            if "://" not in base_url:
                base_url = f"https://{base_url}"
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
        """Send one request, waiting out rate limits.

        A 429 is always retried after the server's ``Retry-After`` (capped), because the server
        refused the request before doing anything. Gateway errors and timeouts are retried only
        when ``retry`` is true: a write the server may already have applied is never resent.
        """
        client = self.get_conn()
        url = path.lstrip("/")
        attempt = 0
        while True:
            try:
                response = client.request(method, url, params=params, json=json)
            except httpx.TimeoutException:
                if retry and attempt < self._max_retries:
                    attempt += 1
                    self._wait(self._backoff(attempt), method, path, "timed out")
                    continue
                raise
            status = response.status_code
            if status == 429 and attempt < self._max_retries:
                attempt += 1
                self._wait(self._retry_after(response, attempt), method, path, "rate limited")
                continue
            if retry and status in RETRYABLE_STATUSES and attempt < self._max_retries:
                attempt += 1
                self._wait(self._backoff(attempt), method, path, f"HTTP {status}")
                continue
            if status == 404 and allow_not_found:
                return response
            if status >= 400:
                raise DreamhubApiError(method, path, status, _short_detail(response))
            return response

    def _wait(self, seconds: float, method: str, path: str, reason: str) -> None:
        self.log.warning("Dreamhub %s %s %s; retrying in %.1fs", method, path, reason, seconds)
        self._sleep(seconds)

    @staticmethod
    def _backoff(attempt: int) -> float:
        return min(2.0**attempt, MAX_RETRY_WAIT_SECONDS)

    @classmethod
    def _retry_after(cls, response: httpx.Response, attempt: int) -> float:
        """The server's ``Retry-After`` (seconds or an HTTP date), capped; our own backoff when absent or unreadable."""
        value = response.headers.get("Retry-After")
        if not value:
            return cls._backoff(attempt)
        try:
            return min(max(float(value), 0.0), MAX_RETRY_WAIT_SECONDS)
        except ValueError:
            pass
        try:
            parsed = email.utils.parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return cls._backoff(attempt)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        seconds = (parsed - datetime.now(timezone.utc)).total_seconds()
        return min(max(seconds, 0.0), MAX_RETRY_WAIT_SECONDS)

    # ------------------------------------------------------------------ reads

    def iter_records(
        self,
        entity: str,
        filters: dict[str, Any] | None = None,
        *,
        sort_field: str = "id",
        page_size: int = MAX_PAGE_SIZE,
        include_additionals: bool = False,
        parent: tuple[str, str] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Yield every record of ``entity`` matching ``filters``, one page at a time.

        Pages are ordered by ``sort_field`` (``id`` by default, which is unique and stable).
        A record seen on an earlier page is not yielded again. When the read ends, the number
        of records yielded must equal the total the API reported on every page; otherwise the
        data changed under the read and :class:`DreamhubConsistencyError` is raised, so a caller
        never loads a partial table believing it complete.

        ``parent=("companies", "<id>")`` reads activities of one record (``entity`` must be
        ``"activities"``); the API has no account-wide activity read yet.
        """
        if not 1 <= page_size <= MAX_PAGE_SIZE:
            raise ValueError(f"page_size must be between 1 and {MAX_PAGE_SIZE}")
        route = self._route(entity, parent)
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
            for row in rows:
                row_id = row.get("id")
                key = str(row_id) if row_id is not None else None
                if key is not None and key in seen:
                    continue
                if key is None:
                    without_id += 1
                else:
                    seen.add(key)
                yield row
            pages = meta.get("pages")
            if len(rows) < page_size or (isinstance(pages, int) and page >= pages):
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

    def resolve_ids(self, entity: str, match_field: str, values: Iterable[Any]) -> dict[Any, list[str]]:
        """Map each value of ``match_field`` to the ids of the records holding it.

        Works for any field the API can filter on, custom fields included. A value no record
        holds maps to an empty list; a value several records hold maps to all their ids.
        """
        wanted = list(dict.fromkeys(v for v in values if v is not None))
        found: dict[Any, list[str]] = {value: [] for value in wanted}
        for start in range(0, len(wanted), MAX_PAGE_SIZE):
            chunk = wanted[start : start + MAX_PAGE_SIZE]
            for row in self.iter_records(entity, {match_field: chunk}):
                value = _field_value(row, match_field)
                if value in found:
                    found[value].append(str(row["id"]))
        return found

    @staticmethod
    def _route(entity: str, parent: tuple[str, str] | None) -> EntityRoute:
        if parent is not None:
            if entity != "activities":
                raise ValueError("parent is only supported for entity='activities'")
            return EntityRoute.activities_of(*parent)
        if entity == "activities":
            raise ValueError("activities are read per parent record: pass parent=('companies', '<id>')")
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
        an ``id`` and soft-deletes them one by one (the API has no bulk delete yet).
        A batch is never resent after an error the server may already have applied.
        """
        if operation not in BULK_OPERATIONS:
            raise ValueError(f"operation must be one of {', '.join(BULK_OPERATIONS)}")
        if not 1 <= batch_size <= MAX_BULK_BATCH:
            raise ValueError(f"batch_size must be between 1 and {MAX_BULK_BATCH}")
        route = EntityRoute.for_entity(entity)
        result = BulkResult(operation=operation)
        if operation == "delete":
            self._delete_each(route, records, result)
            return result
        for start in range(0, len(records), batch_size):
            batch = list(records[start : start + batch_size])
            self._write_batch(route, operation, batch, start, result, override=override, match_existing=match_existing)
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
            missing = [i for i, record in enumerate(batch) if not record.get("id")]
            for i in missing:
                result.failures.append({"key": offset + i, "error": "record has no id"})
            batch_positions = [i for i in range(len(batch)) if i not in set(missing)]
            batch = [batch[i] for i in batch_positions]
            if not batch:
                return
        else:
            batch_positions = list(range(len(batch)))

        method, params = {
            "create": ("POST", {}),
            "update": ("PUT", {"override": _flag(override)}),
            "upsert": ("PUT", {"override": _flag(override), "match_existing": _flag(match_existing)}),
        }[operation]
        try:
            response = self.request(method, f"{route.path}/bulk/{operation}", params=params or None, json=batch)
        except DreamhubApiError as error:
            if error.status_code == 500 and error.detail and "failed for all entities" in error.detail:
                # Every record in the batch was rejected; the server reports it as a 500.
                for position, record in zip(batch_positions, batch):
                    result.failures.append({"key": record.get("id") or offset + position, "error": error.detail})
                return
            raise
        payload = response.json()
        result.succeeded_ids.extend(str(i) for i in payload.get("successfulIds") or [])
        errors = payload.get("errors") or {}
        reported = set()
        for key, message in errors.items():
            reported.add(str(key))
            result.failures.append({"key": _input_key(key, batch, batch_positions, offset), "error": str(message)})
        for failed_id in payload.get("failedIds") or []:
            if str(failed_id) not in reported:
                result.failures.append({"key": str(failed_id), "error": "failed"})

    def _delete_each(self, route: EntityRoute, records: Sequence[dict[str, Any]], result: BulkResult) -> None:
        for position, record in enumerate(records):
            record_id = record.get("id")
            if not record_id:
                result.failures.append({"key": position, "error": "record has no id"})
                continue
            try:
                self.request("DELETE", f"{route.path}/{record_id}")
            except DreamhubApiError as error:
                result.failures.append({"key": str(record_id), "error": error.detail or f"HTTP {error.status_code}"})
                continue
            result.succeeded_ids.append(str(record_id))


def _flag(value: bool) -> str:
    return "true" if value else "false"


def _input_key(key: Any, sent: list[dict[str, Any]], positions: list[int], offset: int) -> Any:
    """Turn the API's error key into one the caller can find in its own input.

    The API keys an error by record id, or by the record's position in the list it was sent.
    That list can be shorter than the caller's batch (records without an id are not sent), so
    a position is mapped back: to the record's id when it has one, else its input position.
    """
    text = str(key)
    if not text.isdigit() or int(text) >= len(sent):
        return text
    record = sent[int(text)]
    return str(record["id"]) if record.get("id") else offset + positions[int(text)]


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
    return None
