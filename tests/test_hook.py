from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

import httpx
import pytest
import respx

from dreamhub_airflow.hooks.dreamhub import (
    MAX_BULK_BATCH,
    DreamhubApiError,
    DreamhubBulkError,
    DreamhubConsistencyError,
    DreamhubHook,
)
from tests.helpers import BASE_URL


def _page(key, rows, total, page, size=2):
    pages = max(1, -(-total // size))
    return httpx.Response(200, json={key: rows, "total": total, "page": page, "size": size, "pages": pages})


def _pages_by_number(responses):
    """A side effect answering each page request with the response for its page number."""

    def answer(request):
        return responses[int(request.url.params["page"])]

    return answer


class TestReading:
    @respx.mock
    def test_reads_every_page_sorted_by_id(self, hook):
        route = respx.post(f"{BASE_URL}/companies/filter").mock(
            side_effect=_pages_by_number(
                {
                    1: _page("companies", [{"id": "c-1"}, {"id": "c-2"}], total=3, page=1),
                    2: _page("companies", [{"id": "c-3"}], total=3, page=2),
                }
            )
        )

        rows = hook.get_records("companies", {"status": 1}, page_size=2)

        assert [r["id"] for r in rows] == ["c-1", "c-2", "c-3"]
        first = route.calls[0].request
        assert json.loads(first.content) == {"filters": {"status": 1}, "sort": {"field": "id", "ascending": True}}
        assert first.url.params["size"] == "2"
        assert first.headers["authorization"] == "Bearer pat_test_token"

    @respx.mock
    def test_a_record_repeated_on_a_later_page_is_yielded_once(self, hook):
        respx.post(f"{BASE_URL}/deals/filter").mock(
            side_effect=_pages_by_number(
                {
                    1: _page("deals", [{"id": "d-1"}, {"id": "d-2"}], total=3, page=1),
                    2: _page("deals", [{"id": "d-2"}, {"id": "d-3"}], total=3, page=2),
                    3: _page("deals", [], total=3, page=3),
                }
            )
        )

        rows = hook.get_records("deals", page_size=2)

        assert [r["id"] for r in rows] == ["d-1", "d-2", "d-3"]

    @respx.mock
    def test_no_match_is_an_empty_read_not_an_error(self, hook):
        respx.post(f"{BASE_URL}/leads/filter").mock(return_value=httpx.Response(404, json={"detail": "Not found"}))

        assert hook.get_records("leads", {"status": 99}) == []

    @respx.mock
    def test_a_total_that_changes_during_the_read_fails_it(self, hook):
        respx.post(f"{BASE_URL}/companies/filter").mock(
            side_effect=_pages_by_number(
                {
                    1: _page("companies", [{"id": "c-1"}, {"id": "c-2"}], total=3, page=1),
                    2: _page("companies", [{"id": "c-3"}, {"id": "c-4"}], total=4, page=2),
                    3: _page("companies", [], total=4, page=3),
                }
            )
        )

        with pytest.raises(DreamhubConsistencyError, match="changed during the read"):
            hook.get_records("companies", page_size=2)

    @respx.mock
    def test_fewer_distinct_records_than_the_total_fails_the_read(self, hook):
        # A row moved between pages and another was skipped: the total says 3, we saw 2.
        respx.post(f"{BASE_URL}/companies/filter").mock(
            side_effect=_pages_by_number(
                {
                    1: _page("companies", [{"id": "c-1"}, {"id": "c-2"}], total=3, page=1),
                    2: _page("companies", [{"id": "c-2"}], total=3, page=2),
                }
            )
        )

        with pytest.raises(DreamhubConsistencyError, match="read 2 distinct records but the API reported 3"):
            hook.get_records("companies", page_size=2)

    @respx.mock
    def test_revenue_items_are_read_from_the_contracts_service(self, hook):
        respx.post(f"{BASE_URL}/contracts/revenue_items/filter").mock(
            return_value=_page("revenueItems", [{"id": "ri-1"}], total=1, page=1)
        )

        assert hook.get_records("revenue_items") == [{"id": "ri-1"}]

    def test_activities_are_refused_until_the_api_can_scope_them(self, hook):
        with pytest.raises(ValueError, match="activities cannot be read yet"):
            hook.get_records("activities")

    def test_a_page_larger_than_the_api_allows_is_refused(self, hook):
        with pytest.raises(ValueError, match="page_size"):
            hook.get_records("companies", page_size=101)


class TestRateLimitsAndRetries:
    @respx.mock
    def test_waits_for_retry_after_then_continues(self, hook, sleeps):
        respx.post(f"{BASE_URL}/companies/filter").mock(
            side_effect=[
                httpx.Response(429, headers={"Retry-After": "7"}),
                _page("companies", [{"id": "c-1"}], total=1, page=1),
            ]
        )

        assert hook.get_records("companies") == [{"id": "c-1"}]
        assert sleeps == [7.0]

    @respx.mock
    def test_reads_an_http_date_retry_after(self, hook, sleeps):
        later = datetime.now(timezone.utc) + timedelta(seconds=20)
        respx.post(f"{BASE_URL}/companies/filter").mock(
            side_effect=[
                httpx.Response(429, headers={"Retry-After": format_datetime(later, usegmt=True)}),
                _page("companies", [], total=0, page=1),
            ]
        )

        hook.get_records("companies")

        assert len(sleeps) == 1 and 10 <= sleeps[0] <= 20

    @respx.mock
    def test_never_waits_longer_than_the_cap(self, hook, sleeps):
        respx.post(f"{BASE_URL}/companies/filter").mock(
            side_effect=[
                httpx.Response(429, headers={"Retry-After": "3600"}),
                _page("companies", [], total=0, page=1),
            ]
        )

        hook.get_records("companies")

        assert sleeps == [300.0]

    @respx.mock
    def test_gives_up_after_max_retries(self, hook, sleeps):
        respx.post(f"{BASE_URL}/companies/filter").mock(return_value=httpx.Response(429))

        with pytest.raises(DreamhubApiError) as raised:
            hook.get_records("companies")

        assert raised.value.status_code == 429
        assert len(sleeps) == 3

    @respx.mock
    def test_a_gateway_error_on_a_read_is_retried(self, hook):
        respx.post(f"{BASE_URL}/companies/filter").mock(
            side_effect=[
                httpx.Response(503),
                _page("companies", [{"id": "c-1"}], total=1, page=1),
            ]
        )

        assert hook.get_records("companies") == [{"id": "c-1"}]

    @respx.mock
    def test_a_gateway_error_on_a_write_is_never_resent(self, hook):
        route = respx.post(f"{BASE_URL}/companies/bulk/create").mock(return_value=httpx.Response(504))

        with pytest.raises(DreamhubBulkError):
            hook.bulk("companies", "create", [{"name": "Acme"}])

        assert route.call_count == 1

    @respx.mock
    def test_errors_carry_the_api_detail_only(self, hook):
        respx.get(f"{BASE_URL}/deals/stages").mock(
            return_value=httpx.Response(403, json={"detail": "Missing required permissions: deals:read"})
        )

        with pytest.raises(DreamhubApiError, match="HTTP 403: Missing required permissions"):
            hook.request("GET", "deals/stages")


class TestBulk:
    @respx.mock
    def test_splits_records_into_batches(self, hook):
        route = respx.post(f"{BASE_URL}/companies/bulk/create").mock(
            side_effect=[
                httpx.Response(201, json={"successfulIds": ["c-1", "c-2"], "failedIds": [], "errors": {}}),
                httpx.Response(201, json={"successfulIds": ["c-3"], "failedIds": [], "errors": {}}),
            ]
        )

        result = hook.bulk("companies", "create", [{"name": "A"}, {"name": "B"}, {"name": "C"}], batch_size=2)

        assert route.call_count == 2
        assert json.loads(route.calls[1].request.content) == [{"name": "C"}]
        assert result.succeeded_ids == ["c-1", "c-2", "c-3"]
        assert result.failures == []

    @respx.mock
    def test_update_never_clears_stored_values_unless_asked(self, hook):
        route = respx.put(f"{BASE_URL}/companies/bulk/update").mock(
            return_value=httpx.Response(200, json={"successfulIds": ["c-1"], "errors": {}})
        )

        hook.bulk("companies", "update", [{"id": "c-1", "industry": None}])
        hook.bulk("companies", "update", [{"id": "c-1", "industry": None}], override=True)

        assert route.calls[0].request.url.params["override"] == "false"
        assert route.calls[1].request.url.params["override"] == "true"

    @respx.mock
    def test_reports_each_failed_record_of_a_partial_batch(self, hook):
        respx.post(f"{BASE_URL}/companies/bulk/create").mock(
            side_effect=[
                httpx.Response(201, json={"successfulIds": ["c-1", "c-2"], "errors": {}}),
                httpx.Response(
                    206, json={"successfulIds": ["c-3"], "failedIds": [], "errors": {"1": "name is required"}}
                ),
            ]
        )

        result = hook.bulk("companies", "create", [{"name": "A"}, {"name": "B"}, {"name": "C"}, {}], batch_size=2)

        # Position 1 of the second batch is record 3 of the input.
        assert result.failures == [{"key": 3, "error": "name is required"}]
        assert result.succeeded == 3

    @respx.mock
    def test_a_batch_where_every_record_failed_is_reported_per_record(self, hook):
        respx.put(f"{BASE_URL}/deals/bulk/update").mock(
            return_value=httpx.Response(500, json={"detail": "Bulk update failed for all entities"})
        )

        result = hook.bulk("deals", "update", [{"id": "d-1"}, {"id": "d-2"}])

        assert result.failures == [
            {"key": "d-1", "error": "Bulk update failed for all entities"},
            {"key": "d-2", "error": "Bulk update failed for all entities"},
        ]

    @respx.mock
    def test_an_unexpected_server_error_still_fails_the_write(self, hook):
        respx.put(f"{BASE_URL}/deals/bulk/update").mock(
            return_value=httpx.Response(500, json={"detail": "Internal server error"})
        )

        with pytest.raises(DreamhubBulkError):
            hook.bulk("deals", "update", [{"id": "d-1"}])

    @respx.mock
    def test_an_update_record_without_an_id_is_reported_not_dropped(self, hook):
        route = respx.put(f"{BASE_URL}/deals/bulk/update").mock(
            return_value=httpx.Response(200, json={"successfulIds": ["d-1"], "errors": {}})
        )

        result = hook.bulk("deals", "update", [{"id": "d-1", "name": "x"}, {"name": "no id"}])

        assert json.loads(route.calls[0].request.content) == [{"id": "d-1", "name": "x"}]
        assert result.failures == [{"key": 1, "error": "record has no id"}]

    @respx.mock
    def test_upsert_passes_natural_key_matching_through(self, hook):
        route = respx.put(f"{BASE_URL}/people/bulk/upsert").mock(
            return_value=httpx.Response(200, json={"successfulIds": ["p-1"], "errors": {}})
        )

        hook.bulk("people", "upsert", [{"email": "dana@example.com"}], match_existing=False)

        params = route.calls[0].request.url.params
        assert (params["override"], params["match_existing"]) == ("false", "false")

    @respx.mock
    def test_delete_soft_deletes_each_record_and_reports_failures(self, hook):
        respx.delete(f"{BASE_URL}/tasks/t-1").mock(return_value=httpx.Response(204))
        respx.delete(f"{BASE_URL}/tasks/t-2").mock(return_value=httpx.Response(404, json={"detail": "Not found"}))

        result = hook.bulk("tasks", "delete", [{"id": "t-1"}, {"id": "t-2"}, {"name": "no id"}])

        assert result.succeeded_ids == ["t-1"]
        assert result.failures == [{"key": "t-2", "error": "Not found"}, {"key": 2, "error": "record has no id"}]

    def test_a_batch_larger_than_the_api_accepts_is_refused(self, hook):
        with pytest.raises(ValueError, match="batch_size"):
            hook.bulk("companies", "create", [{}], batch_size=MAX_BULK_BATCH + 1)

    def test_an_unknown_operation_is_refused(self, hook):
        with pytest.raises(ValueError, match="operation"):
            hook.bulk("companies", "merge", [{}])


class TestResolveIds:
    @respx.mock
    def test_maps_each_value_to_the_records_holding_it(self, hook):
        route = respx.post(f"{BASE_URL}/companies/filter").mock(
            return_value=_page(
                "companies",
                [
                    {"id": "c-1", "externalKey": "A"},
                    {"id": "c-2", "externalKey": "B"},
                    {"id": "c-3", "externalKey": "B"},
                ],
                total=3,
                page=1,
                size=100,
            )
        )

        found = hook.resolve_ids("companies", "external_key", ["A", "B", "C", None])

        assert found == {"A": ["c-1"], "B": ["c-2", "c-3"], "C": []}
        assert json.loads(route.calls[0].request.content)["filters"] == {"external_key": ["A", "B", "C"]}

    @respx.mock
    def test_reads_custom_field_values_from_additionals(self, hook):
        respx.post(f"{BASE_URL}/companies/filter").mock(
            return_value=_page(
                "companies", [{"id": "c-1", "additionals": {"acct_no": "77"}}], total=1, page=1, size=100
            )
        )

        assert hook.resolve_ids("companies", "acct_no", ["77"]) == {"77": ["c-1"]}


class TestConnection:
    @respx.mock
    def test_test_connection_reports_success(self, hook):
        respx.post(f"{BASE_URL}/companies/filter").mock(return_value=_page("companies", [], total=0, page=1))

        assert hook.test_connection() == (True, "Connection to Dreamhub succeeded")

    @respx.mock
    def test_test_connection_reports_a_rejected_token(self, hook):
        respx.post(f"{BASE_URL}/companies/filter").mock(return_value=httpx.Response(401, json={"detail": "Invalid"}))

        ok, message = hook.test_connection()

        assert not ok and "HTTP 401" in message

    def test_a_connection_without_a_token_is_refused(self, monkeypatch):
        monkeypatch.setenv("AIRFLOW_CONN_DREAMHUB_EMPTY", json.dumps({"conn_type": "dreamhub", "host": BASE_URL}))

        with pytest.raises(ValueError, match="no API token"):
            DreamhubHook(dreamhub_conn_id="dreamhub_empty").get_conn()

    def test_the_connection_form_hides_fields_dreamhub_does_not_use(self):
        behaviour = DreamhubHook.get_ui_field_behaviour()

        assert set(behaviour["hidden_fields"]) == {"schema", "port", "login"}
        assert behaviour["relabeling"]["password"] == "API token"
