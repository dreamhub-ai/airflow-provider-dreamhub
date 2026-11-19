"""Behaviour that keeps credentials, data and results safe: where the token can go, what a
failure reports, how values are matched, and what survives a run that stops part-way."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

import httpx
import pytest
import respx
from airflow import DAG

from dreamhub_airflow.hooks.dreamhub import DreamhubApiError, DreamhubBulkError, DreamhubHook
from dreamhub_airflow.operators.api import DreamhubApiOperator
from dreamhub_airflow.operators.bulk import DreamhubBulkOperator
from dreamhub_airflow.operators.extract import DreamhubExtractOperator
from tests.helpers import BASE_URL

try:  # Airflow 3
    from airflow.serialization.serialized_objects import DagSerialization as _Serializer
except ImportError:  # Airflow 2
    from airflow.serialization.serialized_objects import SerializedDAG as _Serializer


def _page(key: str, rows: list[dict], total: int | None = None) -> httpx.Response:
    return httpx.Response(200, json={key: rows, "total": len(rows) if total is None else total})


class TestDagsWithEveryOperatorLoad:
    def test_a_dag_using_all_three_operators_serializes(self) -> None:
        # The scheduler and UI only see DAGs that serialize; a reserved operator field breaks that.
        with DAG("all_operators", start_date=datetime(2026, 1, 1), schedule=None) as dag:
            DreamhubApiOperator(task_id="call", endpoint="deals/stages", query_params={"limit": 5})
            DreamhubExtractOperator(task_id="extract", entity="companies", output_path="/tmp/companies.ndjson")
            DreamhubBulkOperator(task_id="load", entity="companies", operation="update", records=[])

        serialized = _Serializer.to_dict(dag)

        assert {t["task_id"] for t in serialized["dag"]["tasks"] for t in [t.get("__var", t)]} == {
            "call",
            "extract",
            "load",
        }


class TestTheTokenOnlyGoesToTheApi:
    @pytest.mark.parametrize("path", ["https://elsewhere.example/x", "companies/1/../../../x", "./companies"])
    def test_a_path_that_would_leave_the_api_is_refused_before_sending(self, hook: DreamhubHook, path: str) -> None:
        with respx.mock(assert_all_called=False) as router:
            route = router.route().mock(return_value=httpx.Response(200, json={}))

            with pytest.raises(ValueError, match="path must"):
                hook.request("GET", path)

        assert route.call_count == 0

    def test_a_path_with_extra_leading_slashes_stays_on_the_api(self, hook: DreamhubHook) -> None:
        with respx.mock(assert_all_called=False) as router:
            route = router.route().mock(return_value=httpx.Response(200, json={}))

            hook.request("GET", "//elsewhere.example/x")

        assert str(route.calls[0].request.url) == f"{BASE_URL}/elsewhere.example/x"

    def test_the_api_operator_refuses_an_absolute_endpoint(self) -> None:
        with respx.mock(assert_all_called=False) as router:
            route = router.route().mock(return_value=httpx.Response(200, json={}))

            with pytest.raises(ValueError):
                DreamhubApiOperator(task_id="call", endpoint="https://elsewhere.example/steal").execute({})

        assert route.call_count == 0

    def test_ids_in_paths_are_encoded(self, hook: DreamhubHook) -> None:
        with respx.mock(assert_all_called=False) as router:
            route = router.delete(url__startswith=f"{BASE_URL}/tasks/").mock(return_value=httpx.Response(204))

            result = hook.bulk("tasks", "delete", [{"id": "a/b?c"}, {"id": ".."}])

        assert route.call_count == 1
        assert route.calls[0].request.url.raw_path == b"/api/v1/tasks/a%2Fb%3Fc"
        assert result.succeeded_ids == ["a/b?c"]
        assert result.failures == [{"key": "..", "error": "record id is not a valid id"}]


class TestActivitiesOfAMissingParent:
    @respx.mock
    def test_a_parent_that_does_not_exist_fails_the_read(self, hook: DreamhubHook) -> None:
        respx.post(f"{BASE_URL}/companies/c-gone/activities/filter").mock(
            return_value=httpx.Response(404, json={"detail": "Entity with id c-gone not found"})
        )

        with pytest.raises(DreamhubApiError) as raised:
            hook.get_records("activities", parent=("companies", "c-gone"))

        assert raised.value.status_code == 404

    @pytest.mark.parametrize("parent_id", [None, "", "  "])
    def test_a_blank_parent_id_is_refused(self, hook: DreamhubHook, parent_id: str | None) -> None:
        with pytest.raises(ValueError, match="parent_id"):
            hook.get_records("activities", parent=("companies", parent_id))


class TestUpsertFailureKeys:
    @respx.mock
    def test_each_failed_record_is_reported_once_under_a_key_from_the_input(self, hook: DreamhubHook) -> None:
        respx.put(f"{BASE_URL}/companies/bulk/upsert").mock(
            return_value=httpx.Response(
                206,
                json={
                    "successfulIds": ["c-1"],
                    "failedIds": ["c-9"],
                    "errors": {"create_1": "name is required", "update_c-9": "stage is invalid"},
                },
            )
        )

        result = hook.bulk(
            "companies",
            "upsert",
            [{"name": "A"}, {"id": "c-9", "stage": 99}, {"name": ""}],
            match_existing=False,
        )

        # create_1 is the second record without an id: input position 2.
        assert result.failures == [
            {"key": 2, "error": "name is required"},
            {"key": "c-9", "error": "stage is invalid"},
        ]

    @respx.mock
    def test_a_new_record_after_server_side_matching_is_named_by_its_batch(self, hook: DreamhubHook) -> None:
        respx.put(f"{BASE_URL}/people/bulk/upsert").mock(
            return_value=httpx.Response(206, json={"successfulIds": ["p-1"], "errors": {"create_0": "email invalid"}})
        )

        result = hook.bulk("people", "upsert", [{"email": "a@example.com"}, {"email": "bad"}])

        assert result.failures == [{"key": "new record 0 of the batch starting at 0", "error": "email invalid"}]


class TestMatching:
    @respx.mock
    def test_text_from_a_csv_matches_a_stored_number(self, tmp_path: Path) -> None:
        source = tmp_path / "usage.csv"
        source.write_text("account_number,seats\n42,7\n", encoding="utf-8")
        respx.post(f"{BASE_URL}/companies/filter").mock(
            return_value=_page("companies", [{"id": "c-1", "accountNumber": 42}])
        )
        update = respx.put(f"{BASE_URL}/companies/bulk/update").mock(
            return_value=httpx.Response(200, json={"successfulIds": ["c-1"], "errors": {}})
        )

        DreamhubBulkOperator(
            task_id="load",
            entity="companies",
            operation="update",
            input_path=str(source),
            input_format="csv",
            match_field="account_number",
        ).execute({})

        assert json.loads(update.calls[0].request.content) == [{"account_number": "42", "seats": "7", "id": "c-1"}]

    @respx.mock
    def test_an_unmatchable_value_fails_that_record_only(self) -> None:
        respx.post(f"{BASE_URL}/companies/filter").mock(
            return_value=_page("companies", [{"id": "c-1", "accountNumber": "A"}])
        )
        respx.put(f"{BASE_URL}/companies/bulk/update").mock(
            return_value=httpx.Response(200, json={"successfulIds": ["c-1"], "errors": {}})
        )

        summary = DreamhubBulkOperator(
            task_id="load",
            entity="companies",
            operation="update",
            match_field="account_number",
            fail_on_partial=False,
            records=[{"account_number": ["A", "B"]}, {"account_number": "A"}],
        ).execute({})

        assert summary["failures"] == [{"key": 0, "error": "record has no usable account_number value"}]

    @respx.mock
    def test_match_values_never_reach_the_task_log(self, caplog: pytest.LogCaptureFixture) -> None:
        respx.post(f"{BASE_URL}/people/filter").mock(return_value=httpx.Response(404))

        with caplog.at_level(logging.INFO), pytest.raises(Exception, match="1 of 1 records failed"):
            DreamhubBulkOperator(
                task_id="load",
                entity="people",
                operation="update",
                match_field="email",
                records=[{"email": "dana.private@example.com", "title": "CFO"}],
            ).execute({})

        assert "dana.private@example.com" not in caplog.text


class TestARunThatStopsPartWay:
    @respx.mock
    def test_what_was_written_before_the_stop_is_kept_and_reported(self, tmp_path: Path) -> None:
        respx.post(f"{BASE_URL}/companies/bulk/create").mock(
            side_effect=[
                httpx.Response(201, json={"successfulIds": ["c-1", "c-2"], "errors": {}}),
                httpx.Response(504),
            ]
        )
        results = tmp_path / "results.ndjson"

        with pytest.raises(DreamhubBulkError) as raised:
            DreamhubBulkOperator(
                task_id="load",
                entity="companies",
                operation="create",
                records=[{"name": n} for n in "ABCD"],
                batch_size=2,
                results_path=str(results),
            ).execute({})

        assert raised.value.result.succeeded_ids == ["c-1", "c-2"]
        assert (raised.value.unknown_from, raised.value.unknown_to) == (2, 4)
        written = json.loads(results.read_text(encoding="utf-8"))
        assert written["succeeded_ids"] == ["c-1", "c-2"]


class TestRetries:
    @respx.mock
    def test_an_unreadable_wait_falls_back_to_backoff(self, hook: DreamhubHook, sleeps: list[float]) -> None:
        respx.post(f"{BASE_URL}/companies/filter").mock(
            side_effect=[httpx.Response(429, headers={"Retry-After": "nan"}), _page("companies", [])]
        )

        hook.get_records("companies")

        assert sleeps == [2.0]

    @respx.mock
    def test_a_connection_that_never_opened_is_retried_even_for_a_write(self, hook: DreamhubHook) -> None:
        route = respx.post(f"{BASE_URL}/companies/bulk/create").mock(
            side_effect=[
                httpx.ConnectError("refused"),
                httpx.Response(201, json={"successfulIds": ["c-1"], "errors": {}}),
            ]
        )

        assert hook.bulk("companies", "create", [{"name": "A"}]).succeeded_ids == ["c-1"]
        assert route.call_count == 2

    @respx.mock
    def test_a_write_that_timed_out_is_never_resent(self, hook: DreamhubHook) -> None:
        route = respx.post(f"{BASE_URL}/companies/bulk/create").mock(side_effect=httpx.ReadTimeout("slow"))

        with pytest.raises(DreamhubBulkError):
            hook.bulk("companies", "create", [{"name": "A"}])

        assert route.call_count == 1

    @respx.mock
    def test_a_dropped_connection_on_a_read_is_retried(self, hook: DreamhubHook) -> None:
        respx.post(f"{BASE_URL}/companies/filter").mock(
            side_effect=[httpx.RemoteProtocolError("dropped"), _page("companies", [{"id": "c-1"}])]
        )

        assert hook.get_records("companies") == [{"id": "c-1"}]


class TestPaging:
    @respx.mock
    def test_a_page_with_nothing_new_ends_the_read(self, hook: DreamhubHook) -> None:
        # A server that ignores the page number keeps returning the first page.
        route = respx.post(f"{BASE_URL}/companies/filter").mock(
            return_value=httpx.Response(200, json={"companies": [{"id": "c-1"}, {"id": "c-2"}], "total": 2})
        )

        assert [r["id"] for r in hook.get_records("companies", page_size=2)] == ["c-1", "c-2"]
        assert route.call_count == 2


class TestRecordsFromATemplate:
    @respx.mock
    def test_a_rendered_json_string_is_read_as_records(self) -> None:
        create = respx.post(f"{BASE_URL}/companies/bulk/create").mock(
            return_value=httpx.Response(201, json={"successfulIds": ["c-1"], "errors": {}})
        )

        DreamhubBulkOperator(
            task_id="load", entity="companies", operation="create", records='[{"name": "Acme"}]'
        ).execute({})

        assert json.loads(create.calls[0].request.content) == [{"name": "Acme"}]

    @pytest.mark.parametrize("records", ["not json", '{"name": "Acme"}', "[1, 2]"])
    def test_anything_but_a_list_of_objects_is_refused(self, records: str) -> None:
        with pytest.raises(TypeError, match="list of objects"):
            DreamhubBulkOperator(task_id="load", entity="companies", operation="create", records=records).execute({})


class TestLocalFiles:
    @respx.mock
    def test_the_destination_folder_is_created_and_no_staging_file_is_left(self, tmp_path: Path) -> None:
        respx.post(f"{BASE_URL}/companies/filter").mock(return_value=_page("companies", [{"id": "c-1"}]))
        target = tmp_path / "new" / "folder" / "companies.ndjson"

        DreamhubExtractOperator(task_id="extract", entity="companies", output_path=str(target)).execute({})

        assert target.read_text(encoding="utf-8") == '{"id": "c-1"}\n'
        assert sorted(p.name for p in target.parent.iterdir()) == ["companies.ndjson"]
