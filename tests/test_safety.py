"""Behaviour that keeps credentials, data and results safe: where the token can go, what a
failure reports, how values are matched, and what survives a run that stops part-way."""

from __future__ import annotations

import json
import os
import pickle
import stat
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import httpx
import pytest
import respx
from airflow import DAG

from dreamhub_airflow.hooks.dreamhub import BulkResult, DreamhubApiError, DreamhubBulkError, DreamhubHook
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
    @pytest.mark.parametrize(
        "path", ["https://elsewhere.example/x", "companies/1/../../../x", "./companies", "deals?limit=1", "deals#x"]
    )
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
    def test_match_values_never_reach_the_task_log(self, monkeypatch: pytest.MonkeyPatch) -> None:
        respx.post(f"{BASE_URL}/people/filter").mock(return_value=httpx.Response(404))
        operator = DreamhubBulkOperator(
            task_id="load",
            entity="people",
            operation="update",
            match_field="email",
            records=[{"email": "dana.private@example.com", "title": "CFO"}],
        )
        # Spy on the operator's own logger: Airflow 3 does not route it through `logging`.
        spy = MagicMock()
        monkeypatch.setattr(type(operator), "log", property(lambda _self: spy))

        with pytest.raises(Exception, match="1 of 1 records failed"):
            operator.execute({})

        logged = str(spy.mock_calls)
        assert "Failed records" in logged  # positive control: the failure was logged
        assert "dana.private@example.com" not in logged

    @respx.mock
    def test_the_lookup_query_sends_the_cleaned_value(self) -> None:
        lookup = respx.post(f"{BASE_URL}/companies/filter").mock(return_value=httpx.Response(404))

        DreamhubBulkOperator(
            task_id="load",
            entity="companies",
            operation="update",
            match_field="account_number",
            fail_on_partial=False,
            records=[{"account_number": " 42 "}, {"account_number": 7.0}],
        ).execute({})

        assert json.loads(lookup.calls[0].request.content)["filters"] == {"account_number": ["42", 7]}


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
                records=[{"name": n} for n in "ABCDE"],
                batch_size=2,
                results_path=str(results),
            ).execute({})

        assert raised.value.result.succeeded_ids == ["c-1", "c-2"]
        assert (raised.value.unknown, raised.value.not_attempted) == ([2, 3], [4])
        written = json.loads(results.read_text(encoding="utf-8"))
        assert written["succeeded_ids"] == ["c-1", "c-2"]
        assert (written["unknown_positions"], written["not_attempted_positions"]) == ([2, 3], [4])

    @respx.mock
    def test_positions_are_the_callers_own_after_matching(self, tmp_path: Path) -> None:
        respx.post(f"{BASE_URL}/companies/filter").mock(
            return_value=_page("companies", [{"id": "c-1", "accountNumber": "B"}, {"id": "c-2", "accountNumber": "C"}])
        )
        respx.put(f"{BASE_URL}/companies/bulk/update").mock(
            side_effect=[httpx.Response(200, json={"successfulIds": ["c-1"], "errors": {}}), httpx.Response(504)]
        )
        results = tmp_path / "results.ndjson"

        with pytest.raises(DreamhubBulkError) as raised:
            DreamhubBulkOperator(
                task_id="load",
                entity="companies",
                operation="update",
                match_field="account_number",
                batch_size=1,
                results_path=str(results),
                records=[{"account_number": "A"}, {"account_number": "B"}, {"account_number": "C"}],
            ).execute({})

        # Input 0 matched nothing, input 1 was written, input 2 was in flight.
        assert raised.value.unknown == [2]
        assert raised.value.result.failures == [{"key": 0, "error": "no record has this account_number value"}]
        assert "1 records were written and 1 failed" in str(raised.value)

    @respx.mock
    def test_an_unexpected_response_still_keeps_the_partial_result(self, tmp_path: Path) -> None:
        respx.post(f"{BASE_URL}/companies/bulk/create").mock(
            side_effect=[
                httpx.Response(201, json={"successfulIds": ["c-1"], "errors": {}}),
                httpx.Response(200, text="<html>gateway</html>"),
            ]
        )
        results = tmp_path / "results.ndjson"

        with pytest.raises(DreamhubBulkError):
            DreamhubBulkOperator(
                task_id="load",
                entity="companies",
                operation="create",
                records=[{"name": "A"}, {"name": "B"}],
                batch_size=1,
                results_path=str(results),
            ).execute({})

        assert json.loads(results.read_text(encoding="utf-8"))["succeeded_ids"] == ["c-1"]

    def test_a_delete_on_a_connection_without_a_token_fails_instead_of_blaming_records(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("AIRFLOW_CONN_DREAMHUB_NOTOKEN", json.dumps({"conn_type": "dreamhub", "host": BASE_URL}))
        hook = DreamhubHook(dreamhub_conn_id="dreamhub_notoken")

        with pytest.raises(DreamhubBulkError) as raised:
            hook.bulk("tasks", "delete", [{"id": "t-1"}, {"id": "t-2"}])

        assert isinstance(raised.value.cause, ValueError)
        assert raised.value.result.failures == []

    @respx.mock
    def test_a_rejected_token_stops_a_delete(self, hook: DreamhubHook) -> None:
        route = respx.delete(url__startswith=f"{BASE_URL}/tasks/").mock(return_value=httpx.Response(401))

        with pytest.raises(DreamhubBulkError) as raised:
            hook.bulk("tasks", "delete", [{"id": "t-1"}, {"id": "t-2"}])

        assert route.call_count == 1
        assert raised.value.not_attempted == [1]

    def test_the_stop_error_survives_pickling(self) -> None:
        error = DreamhubBulkError(BulkResult("create", ["c-1"]), [1], [2], DreamhubApiError("POST", "x", 504))

        copy = pickle.loads(pickle.dumps(error))

        assert (copy.unknown, copy.not_attempted, copy.result.succeeded_ids) == ([1], [2], ["c-1"])
        assert copy.cause.status_code == 504


class TestErrorKeysTheApiAlsoSends:
    @respx.mock
    def test_entity_keys_and_text_none_ids_are_handled_like_the_api(self, hook: DreamhubHook) -> None:
        respx.put(f"{BASE_URL}/companies/bulk/upsert").mock(
            return_value=httpx.Response(
                206,
                json={
                    "successfulIds": [],
                    "failedIds": ["c-5"],
                    "errors": {"create_entity_c-5": "custom field failed", "create_1": "name is required"},
                },
            )
        )

        result = hook.bulk("companies", "upsert", [{"id": "None", "name": "A"}, {"name": ""}], match_existing=False)

        # "None" counts as no id, as the API treats it, so create_1 is input position 1.
        assert result.failures == [
            {"key": "c-5", "error": "custom field failed"},
            {"key": 1, "error": "name is required"},
        ]


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

    @respx.mock
    def test_a_failed_read_leaves_no_staging_file(self, tmp_path: Path) -> None:
        respx.post(f"{BASE_URL}/companies/filter").mock(
            side_effect=[
                httpx.Response(200, json={"companies": [{"id": "c-1"}, {"id": "c-2"}], "total": 3}),
                httpx.Response(200, json={"companies": [], "total": 3}),
            ]
        )
        target = tmp_path / "companies.ndjson"
        target.write_text("previous\n", encoding="utf-8")

        with pytest.raises(Exception, match="read 2 distinct records"):
            DreamhubExtractOperator(
                task_id="extract", entity="companies", output_path=str(target), page_size=2
            ).execute({})

        assert target.read_text(encoding="utf-8") == "previous\n"
        assert sorted(p.name for p in tmp_path.iterdir()) == ["companies.ndjson"]

    @respx.mock
    def test_an_existing_files_permissions_are_kept(self, tmp_path: Path) -> None:
        respx.post(f"{BASE_URL}/companies/filter").mock(return_value=_page("companies", [{"id": "c-1"}]))
        target = tmp_path / "companies.ndjson"
        target.write_text("old\n", encoding="utf-8")
        os.chmod(target, 0o644)

        DreamhubExtractOperator(task_id="extract", entity="companies", output_path=str(target)).execute({})

        assert stat.S_IMODE(target.stat().st_mode) == 0o644

    @respx.mock
    def test_a_symlinked_destination_is_written_through(self, tmp_path: Path) -> None:
        respx.post(f"{BASE_URL}/companies/filter").mock(return_value=_page("companies", [{"id": "c-1"}]))
        real = tmp_path / "real.ndjson"
        real.write_text("old\n", encoding="utf-8")
        link = tmp_path / "link.ndjson"
        link.symlink_to(real)

        DreamhubExtractOperator(task_id="extract", entity="companies", output_path=str(link)).execute({})

        assert link.is_symlink()
        assert real.read_text(encoding="utf-8") == '{"id": "c-1"}\n'
