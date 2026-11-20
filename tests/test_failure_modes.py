"""Answers the API gives that are not the plain success or failure the rest of the suite covers."""

from __future__ import annotations

import json
import os
import pickle
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from pathlib import Path

import httpx
import pytest
import respx

from dreamhub_airflow import files
from dreamhub_airflow.entities import EntityRoute
from dreamhub_airflow.files import RecordFile
from dreamhub_airflow.hooks.dreamhub import DreamhubApiError, DreamhubBulkError, DreamhubHook, _UnreportedRecords
from dreamhub_airflow.operators.api import DreamhubApiOperator
from dreamhub_airflow.operators.bulk import DreamhubBulkOperator, DreamhubRecordsFailedError
from tests.helpers import BASE_URL


def _company(**fields: object) -> dict[str, object]:
    """A company carrying every field the API requires today, plus the given ones."""
    return {"name": "Acme", "status": 1, **fields}


class TestRoutes:
    def test_an_alias_with_surrounding_slashes_reaches_the_same_endpoint(self) -> None:
        assert EntityRoute.for_entity("/revenue_items/") == EntityRoute.for_entity("revenue_items")

    @respx.mock
    def test_a_redirect_is_an_error_not_a_success(self, hook: DreamhubHook) -> None:
        respx.get(f"{BASE_URL}/deals/stages").mock(
            return_value=httpx.Response(302, headers={"Location": "https://elsewhere.test/"})
        )

        with pytest.raises(DreamhubApiError) as raised:
            hook.request("GET", "deals/stages")

        assert raised.value.status_code == 302

    @respx.mock
    def test_a_path_the_api_does_not_serve_is_an_error_not_an_empty_read(self, hook: DreamhubHook) -> None:
        respx.post(f"{BASE_URL}/compnies/filter").mock(return_value=httpx.Response(404, json={"detail": "Not Found"}))

        with pytest.raises(DreamhubApiError):
            hook.get_records("compnies")

    @respx.mock
    def test_a_proxy_page_answering_404_is_an_error_not_an_empty_read(self, hook: DreamhubHook) -> None:
        respx.post(f"{BASE_URL}/companies/filter").mock(return_value=httpx.Response(404, text="<html>Not Found</html>"))

        with pytest.raises(DreamhubApiError):
            hook.get_records("companies")

    @respx.mock
    def test_a_base_url_that_does_not_serve_the_api_fails_the_connection_test(self, hook: DreamhubHook) -> None:
        respx.post(f"{BASE_URL}/companies/filter").mock(return_value=httpx.Response(404, json={"detail": "Not Found"}))

        assert hook.test_connection() == (False, "The API base URL does not serve the Dreamhub API")

    @respx.mock
    def test_nothing_matching_is_still_an_empty_read(self, hook: DreamhubHook) -> None:
        respx.post(f"{BASE_URL}/companies/filter").mock(
            return_value=httpx.Response(404, json={"detail": "No companies found"})
        )

        assert hook.get_records("companies") == []

    @respx.mock
    def test_a_server_repeating_the_same_page_is_not_read_forever(self, hook: DreamhubHook) -> None:
        rows = [{"name": f"row {i}"} for i in range(2)]
        route = respx.post(f"{BASE_URL}/companies/filter").mock(
            return_value=httpx.Response(200, json={"companies": rows})
        )

        assert list(hook.iter_records("companies", page_size=2)) == rows
        assert route.call_count == 2

    @respx.mock
    def test_matching_asks_for_custom_fields(self, hook: DreamhubHook) -> None:
        route = respx.post(f"{BASE_URL}/companies/filter").mock(
            return_value=httpx.Response(
                200, json={"companies": [{"id": "c-1", "additionals": {"account_number": "A-1"}}], "total": 1}
            )
        )

        assert hook.resolve_ids("companies", "account_number", ["A-1"]) == {"A-1": ["c-1"]}
        assert route.calls[0].request.url.params["include_additionals"] == "true"

    @pytest.mark.parametrize(
        "endpoint", ["companies/c-1/activities/filter", "companies/c-1/activities/filter?page=1&size=100"]
    )
    def test_activity_reads_are_refused_by_the_generic_operator(self, endpoint: str) -> None:
        with pytest.raises(ValueError, match="activities"):
            DreamhubApiOperator(task_id="call", endpoint=endpoint, method="post").execute({})

    @respx.mock
    def test_activity_writes_still_pass_through_the_generic_operator(self) -> None:
        route = respx.post(f"{BASE_URL}/activities").mock(return_value=httpx.Response(201, json={"id": "a-1"}))

        DreamhubApiOperator(task_id="call", endpoint="activities", method="post", data={"type": 1}).execute({})

        assert route.call_count == 1


class TestRetryWait:
    @respx.mock
    def test_the_connection_cap_also_limits_the_hooks_own_backoff(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv(
            "AIRFLOW_CONN_DREAMHUB_CAPPED",
            json.dumps({"conn_type": "dreamhub", "host": BASE_URL, "password": "t", "extra": {"max_retry_wait": 1}}),
        )
        sleeps: list[float] = []
        hook = DreamhubHook(dreamhub_conn_id="dreamhub_capped")
        hook._sleep = sleeps.append
        respx.post(f"{BASE_URL}/companies/filter").mock(
            side_effect=[
                httpx.Response(429),
                httpx.Response(429),
                httpx.Response(404, json={"detail": "No companies found"}),
            ]
        )
        try:
            hook.get_records("companies")
        finally:
            hook.close()

        assert sleeps == [1.0, 1.0]


class TestBulkOutcomes:
    @respx.mock
    def test_records_held_back_before_sending_are_not_listed_as_unknown(self, hook: DreamhubHook) -> None:
        def dropped(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("no answer", request=request)

        respx.put(f"{BASE_URL}/deals/bulk/update").mock(side_effect=dropped)

        with pytest.raises(DreamhubBulkError) as raised:
            hook.bulk("deals", "update", [{"name": "no id"}, {"id": "d-2", "name": "x"}])

        assert raised.value.unknown == [1]
        assert raised.value.result.failures == [{"key": 0, "error": "record has no id"}]

    @respx.mock
    def test_an_existing_record_the_response_leaves_out_fails(self, hook: DreamhubHook) -> None:
        respx.put(f"{BASE_URL}/deals/bulk/update").mock(
            return_value=httpx.Response(200, json={"successfulIds": ["d-1"], "errors": {}})
        )

        result = hook.bulk("deals", "update", [{"id": "d-1"}, {"id": "d-2"}])

        assert result.failures == [
            {"key": "d-2", "error": "outcome not reported by Dreamhub; it may have been written"}
        ]

    @respx.mock
    def test_new_records_the_response_leaves_out_stop_the_run_as_unknown(self, hook: DreamhubHook) -> None:
        respx.post(f"{BASE_URL}/companies/bulk/create").mock(
            return_value=httpx.Response(201, json={"successfulIds": ["c-1"], "errors": {}})
        )

        with pytest.raises(DreamhubBulkError) as raised:
            hook.bulk("companies", "create", [_company(), _company(), _company()])

        assert raised.value.unknown == [0, 1, 2]
        assert raised.value.result.succeeded_ids == ["c-1"]

    @respx.mock
    def test_a_gateway_error_on_delete_stops_with_that_record_unknown(self, hook: DreamhubHook) -> None:
        respx.delete(f"{BASE_URL}/companies/c-1").mock(return_value=httpx.Response(200))
        respx.delete(f"{BASE_URL}/companies/c-2").mock(return_value=httpx.Response(502))

        with pytest.raises(DreamhubBulkError) as raised:
            hook.bulk("companies", "delete", [{"id": "c-1"}, {"id": "c-2"}, {"id": "c-3"}])

        assert raised.value.result.succeeded_ids == ["c-1"]
        assert (raised.value.unknown, raised.value.not_attempted) == ([1], [2])

    @respx.mock
    def test_a_delete_answered_by_a_gateway_error_is_sent_again(self, hook: DreamhubHook) -> None:
        route = respx.delete(f"{BASE_URL}/companies/c-1").mock(side_effect=[httpx.Response(502), httpx.Response(204)])

        result = hook.bulk("companies", "delete", [{"id": "c-1"}])

        assert result.succeeded_ids == ["c-1"]
        assert route.call_count == 2

    @respx.mock
    def test_an_id_listed_twice_still_accounts_for_two_new_records(self, hook: DreamhubHook) -> None:
        respx.put(f"{BASE_URL}/companies/bulk/upsert").mock(
            return_value=httpx.Response(200, json={"successfulIds": ["c-1", "c-1"], "errors": {}})
        )

        result = hook.bulk("companies", "upsert", [_company(), _company()])

        assert result.succeeded_ids == ["c-1", "c-1"]
        assert result.failures == []


class TestOperatorReports:
    @respx.mock
    def test_a_partial_failure_carries_the_full_result(self) -> None:
        respx.post(f"{BASE_URL}/companies/bulk/create").mock(
            return_value=httpx.Response(206, json={"successfulIds": ["c-1"], "errors": {"1": "name is required"}})
        )

        with pytest.raises(DreamhubRecordsFailedError) as raised:
            DreamhubBulkOperator(
                task_id="load", entity="companies", operation="create", records=[_company(), {"status": 1}]
            ).execute({})

        copy = pickle.loads(pickle.dumps(raised.value))
        assert copy.summary["failures"] == [{"key": 1, "error": "name is required"}]
        assert "name is required" not in str(raised.value)

    @respx.mock
    def test_a_stopped_run_logs_the_positions_to_check(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def dropped(request: httpx.Request) -> httpx.Response:
            raise httpx.ReadTimeout("no answer", request=request)

        class Recorder:
            def __init__(self) -> None:
                self.lines: list[str] = []

            def info(self, message: str, *args: object) -> None:
                self.lines.append(message % args)

            warning = exception = info

        recorder = Recorder()
        monkeypatch.setattr(DreamhubBulkOperator, "log", recorder)
        respx.post(f"{BASE_URL}/companies/bulk/create").mock(side_effect=dropped)

        with pytest.raises(DreamhubBulkError):
            DreamhubBulkOperator(
                task_id="load", entity="companies", operation="create", records=[_company(), _company()], batch_size=1
            ).execute({})

        assert any("may or may not have been written: [0]; not attempted: [1]" in line for line in recorder.lines)


class TestLocalFiles:
    def test_records_are_staged_in_a_file_only_the_owner_can_read(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        modes: list[int] = []
        real_open = os.open

        def recording_open(path: str, flags: int, mode: int = 0o777) -> int:
            if path.endswith(".partial"):
                modes.append(mode)
            return real_open(path, flags, mode)

        monkeypatch.setattr(files.os, "open", recording_open)
        target = tmp_path / "out.ndjson"

        RecordFile(str(target), "ndjson").write([{"id": "c-1"}])

        assert modes == [0o600]
        umask = os.umask(0o077)
        os.umask(umask)
        assert target.stat().st_mode & 0o777 == 0o666 & ~umask

    def test_concurrent_writes_leave_the_process_umask_as_it_was(self, tmp_path: Path) -> None:
        before = os.umask(0o022)
        try:
            with ThreadPoolExecutor(max_workers=8) as pool:
                list(
                    pool.map(
                        lambda i: RecordFile(str(tmp_path / f"out-{i}.ndjson"), "ndjson").write([{"id": i}]), range(64)
                    )
                )
            assert os.umask(0o022) == 0o022
        finally:
            os.umask(before)


class TestShapes:
    @respx.mock
    def test_a_row_that_is_not_a_record_is_an_error(self, hook: DreamhubHook) -> None:
        respx.post(f"{BASE_URL}/companies/filter").mock(
            return_value=httpx.Response(200, json={"companies": [{"id": "c-1"}, "oops"], "total": 2})
        )

        with pytest.raises(DreamhubApiError, match="not a record"):
            hook.get_records("companies")

    @respx.mock
    def test_a_repeated_id_cannot_hide_a_record_sent_with_its_id(self, hook: DreamhubHook) -> None:
        respx.put(f"{BASE_URL}/deals/bulk/update").mock(
            return_value=httpx.Response(200, json={"successfulIds": ["d-1", "d-1"], "errors": {}})
        )

        result = hook.bulk("deals", "update", [{"id": "d-1"}, {"id": "d-2"}])

        assert result.failures == [
            {"key": "d-2", "error": "outcome not reported by Dreamhub; it may have been written"}
        ]

    @respx.mock
    def test_a_match_value_that_is_not_json_is_sent_as_text(self, hook: DreamhubHook) -> None:
        route = respx.post(f"{BASE_URL}/companies/filter").mock(
            return_value=httpx.Response(404, json={"detail": "No companies found"})
        )

        hook.resolve_ids("companies", "account_number", [Decimal("12.5")])

        assert json.loads(route.calls[0].request.content)["filters"] == {"account_number": ["12.5"]}

    def test_the_unreported_records_stop_survives_pickling(self) -> None:
        copy = pickle.loads(pickle.dumps(_UnreportedRecords([3, 4])))

        assert (copy.positions, str(copy)) == ([3, 4], "2 new records were neither confirmed nor reported as failed")

    @respx.mock
    def test_a_delete_whose_connection_dropped_is_sent_again(self, hook: DreamhubHook) -> None:
        calls: list[int] = []

        def flaky(request: httpx.Request) -> httpx.Response:
            calls.append(1)
            if len(calls) == 1:
                raise httpx.ReadTimeout("no answer", request=request)
            return httpx.Response(204)

        respx.delete(f"{BASE_URL}/companies/c-1").mock(side_effect=flaky)

        assert hook.bulk("companies", "delete", [{"id": "c-1"}]).succeeded_ids == ["c-1"]
        assert len(calls) == 2

    def test_a_percent_encoded_activity_read_is_refused(self) -> None:
        with pytest.raises(ValueError, match="activities"):
            DreamhubApiOperator(task_id="call", endpoint="companies/c-1/activities/fil%74er", method="post").execute({})

    @respx.mock
    def test_a_rate_limit_that_outlasts_the_retries_stops_a_delete(self, hook: DreamhubHook) -> None:
        respx.delete(f"{BASE_URL}/companies/c-1").mock(return_value=httpx.Response(429))

        with pytest.raises(DreamhubBulkError) as raised:
            hook.bulk("companies", "delete", [{"id": "c-1"}, {"id": "c-2"}])

        assert raised.value.result.failures == [{"key": "c-1", "error": "HTTP 429"}]
        assert raised.value.not_attempted == [1]

    @respx.mock
    def test_a_file_with_a_line_that_is_not_an_object_is_refused_before_any_write(self, tmp_path: Path) -> None:
        source = tmp_path / "in.ndjson"
        source.write_text('{"name": "Acme", "status": 1}\nnull\n')
        route = respx.post(f"{BASE_URL}/companies/bulk/create").mock(
            return_value=httpx.Response(201, json={"successfulIds": ["c-1"], "errors": {}})
        )

        with pytest.raises(TypeError, match="one JSON object per record"):
            DreamhubBulkOperator(
                task_id="load", entity="companies", operation="create", input_path=str(source), batch_size=1
            ).execute({})

        assert route.call_count == 0

    def test_a_local_file_is_staged_even_with_a_storage_connection(self, tmp_path: Path) -> None:
        elsewhere = tmp_path / "elsewhere.ndjson"
        elsewhere.write_text("keep\n")
        target = tmp_path / "results.ndjson"
        target.symlink_to(elsewhere)

        RecordFile(str(target), "ndjson", conn_id="any_storage").write([{"id": "c-1"}])

        assert not target.is_symlink()
        assert elsewhere.read_text() == "keep\n"

    @respx.mock
    def test_a_delete_refused_after_an_attempt_that_may_have_applied_is_unknown(self, hook: DreamhubHook) -> None:
        def dropped_then_limited(request: httpx.Request) -> httpx.Response:
            if not getattr(dropped_then_limited, "dropped", False):
                dropped_then_limited.dropped = True  # type: ignore[attr-defined]
                raise httpx.ReadTimeout("no answer", request=request)
            return httpx.Response(429)

        respx.delete(f"{BASE_URL}/companies/c-1").mock(side_effect=dropped_then_limited)

        with pytest.raises(DreamhubBulkError) as raised:
            hook.bulk("companies", "delete", [{"id": "c-1"}, {"id": "c-2"}])

        assert (raised.value.unknown, raised.value.not_attempted) == ([0], [1])
        assert raised.value.result.failures == []

    def test_the_ambiguity_of_an_api_error_survives_pickling(self) -> None:
        error = DreamhubApiError("DELETE", "companies/c-1", 429, after_ambiguous_attempt=True)

        assert pickle.loads(pickle.dumps(error)).after_ambiguous_attempt is True
