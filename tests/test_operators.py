from __future__ import annotations

import csv
import importlib
import json
from importlib.metadata import entry_points

import httpx
import pytest
import respx
from airflow.exceptions import AirflowTaskTimeout

from dreamhub_airflow.get_provider_info import get_provider_info
from dreamhub_airflow.hooks.dreamhub import DreamhubBulkError, DreamhubHook
from dreamhub_airflow.operators.api import DreamhubApiOperator
from dreamhub_airflow.operators.bulk import DreamhubBulkOperator, DreamhubRecordsFailedError
from dreamhub_airflow.operators.extract import DreamhubExtractOperator
from tests.helpers import BASE_URL


def _page(key, rows):
    return httpx.Response(200, json={key: rows, "total": len(rows), "page": 1, "size": 100, "pages": 1})


def _lines(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


class TestExtract:
    @respx.mock
    def test_writes_ndjson_and_returns_only_the_path_and_count(self, tmp_path):
        respx.post(f"{BASE_URL}/contracts/filter").mock(
            return_value=_page("contracts", [{"id": "k-1", "endDate": "2027-01-31"}, {"id": "k-2", "endDate": None}])
        )
        target = tmp_path / "contracts.ndjson"

        result = DreamhubExtractOperator(task_id="extract", entity="contracts", output_path=str(target)).execute({})

        assert result == {"path": str(target), "count": 2}
        assert _lines(target) == [{"id": "k-1", "endDate": "2027-01-31"}, {"id": "k-2", "endDate": None}]

    @respx.mock
    def test_keeps_only_the_requested_fields_in_order(self, tmp_path):
        respx.post(f"{BASE_URL}/companies/filter").mock(
            return_value=_page("companies", [{"id": "c-1", "name": "Acme", "secret": "x"}])
        )
        target = tmp_path / "companies.ndjson"

        DreamhubExtractOperator(
            task_id="extract", entity="companies", output_path=str(target), fields=["name", "id"]
        ).execute({})

        assert target.read_text(encoding="utf-8") == '{"name": "Acme", "id": "c-1"}\n'

    @respx.mock
    def test_csv_columns_cover_every_field_and_nested_values_are_json(self, tmp_path):
        respx.post(f"{BASE_URL}/deals/filter").mock(
            return_value=_page("deals", [{"id": "d-1", "tags": ["a", "b"]}, {"id": "d-2", "amount": 5}])
        )
        target = tmp_path / "deals.csv"

        DreamhubExtractOperator(task_id="extract", entity="deals", output_path=str(target), file_format="csv").execute(
            {}
        )

        with target.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.reader(handle))
        assert rows == [["id", "tags", "amount"], ["d-1", '["a", "b"]', ""], ["d-2", "", "5"]]

    @respx.mock
    def test_csv_neutralizes_formulas_unless_told_not_to(self, tmp_path):
        respx.post(f"{BASE_URL}/companies/filter").mock(
            return_value=_page("companies", [{"name": '=HYPERLINK("x")', "balance": -5}])
        )
        guarded, raw = tmp_path / "guarded.csv", tmp_path / "raw.csv"

        DreamhubExtractOperator(task_id="a", entity="companies", output_path=str(guarded), file_format="csv").execute(
            {}
        )
        DreamhubExtractOperator(
            task_id="b", entity="companies", output_path=str(raw), file_format="csv", neutralize_formulas=False
        ).execute({})

        def data_row(path):
            with path.open(newline="", encoding="utf-8") as handle:
                return list(csv.reader(handle))[1]

        # A number keeps its sign: only text a spreadsheet would run as a formula is prefixed.
        assert data_row(guarded) == ['\'=HYPERLINK("x")', "-5"]
        assert data_row(raw) == ['=HYPERLINK("x")', "-5"]

    def test_the_output_path_and_filters_are_templated(self):
        assert {"output_path", "filters"} <= set(DreamhubExtractOperator.template_fields)


class TestBulk:
    @respx.mock
    def test_returns_the_per_record_result(self):
        respx.post(f"{BASE_URL}/companies/bulk/create").mock(
            return_value=httpx.Response(201, json={"successfulIds": ["c-1"], "errors": {}})
        )

        result = DreamhubBulkOperator(
            task_id="load", entity="companies", operation="create", records=[{"name": "Acme"}]
        ).execute({})

        assert result == {"operation": "create", "succeeded": 1, "failed": 0, "succeeded_ids": ["c-1"], "failures": []}

    @respx.mock
    def test_fails_the_task_when_a_record_failed(self):
        respx.post(f"{BASE_URL}/companies/bulk/create").mock(
            return_value=httpx.Response(206, json={"successfulIds": ["c-1"], "errors": {"1": "bad"}})
        )

        with pytest.raises(DreamhubRecordsFailedError, match="1 of 2 records failed"):
            DreamhubBulkOperator(
                task_id="load", entity="companies", operation="create", records=[{"name": "A"}, {"name": ""}]
            ).execute({})

    @respx.mock
    def test_can_finish_with_failures_and_write_them_all(self, tmp_path):
        respx.post(f"{BASE_URL}/companies/bulk/create").mock(
            return_value=httpx.Response(206, json={"successfulIds": [], "errors": {"0": "bad"}})
        )
        results = tmp_path / "results.ndjson"

        summary = DreamhubBulkOperator(
            task_id="load",
            entity="companies",
            operation="create",
            records=[{}],
            fail_on_partial=False,
            results_path=str(results),
        ).execute({})

        assert summary["failed"] == 1
        assert _lines(results)[0]["failures"] == [{"key": 0, "error": "bad"}]

    @respx.mock
    def test_updates_records_matched_on_a_field_the_pipeline_names(self):
        respx.post(f"{BASE_URL}/companies/filter").mock(
            return_value=_page(
                "companies",
                [
                    {"id": "c-1", "externalKey": "A"},
                    {"id": "c-2", "externalKey": "B"},
                    {"id": "c-3", "externalKey": "B"},
                ],
            )
        )
        update = respx.put(f"{BASE_URL}/companies/bulk/update").mock(
            return_value=httpx.Response(200, json={"successfulIds": ["c-1"], "errors": {}})
        )

        summary = DreamhubBulkOperator(
            task_id="load",
            entity="companies",
            operation="update",
            match_field="external_key",
            fail_on_partial=False,
            records=[
                {"external_key": "A", "name": "Acme", "status": 1, "seats": 10},
                {"external_key": "B", "name": "Beta", "status": 1, "seats": 3},
                {"external_key": "Z", "name": "Zeta", "status": 1, "seats": 1},
            ],
        ).execute({})

        assert json.loads(update.calls[0].request.content) == [
            {"external_key": "A", "name": "Acme", "status": 1, "seats": 10, "id": "c-1"}
        ]
        assert summary["failures"] == [
            {"key": 1, "error": "2 records have this external_key value"},
            {"key": 2, "error": "no record has this external_key value"},
        ]

    @respx.mock
    def test_upsert_creates_records_no_match_was_found_for(self):
        respx.post(f"{BASE_URL}/companies/filter").mock(
            return_value=httpx.Response(404, json={"detail": "No companies found"})
        )
        upsert = respx.put(f"{BASE_URL}/companies/bulk/upsert").mock(
            return_value=httpx.Response(200, json={"successfulIds": ["c-9"], "errors": {}})
        )

        DreamhubBulkOperator(
            task_id="load",
            entity="companies",
            operation="upsert",
            match_field="external_key",
            records=[{"external_key": "NEW", "name": "New Co"}],
        ).execute({})

        assert json.loads(upsert.calls[0].request.content) == [{"external_key": "NEW", "name": "New Co"}]

    @respx.mock
    def test_reads_records_from_a_file(self, tmp_path):
        source = tmp_path / "usage.ndjson"
        source.write_text('{"id": "c-1", "seats": 4}\n\n{"id": "c-2", "seats": 9}\n', encoding="utf-8")
        update = respx.put(f"{BASE_URL}/companies/bulk/update").mock(
            return_value=httpx.Response(200, json={"successfulIds": ["c-1", "c-2"], "errors": {}})
        )

        DreamhubBulkOperator(task_id="load", entity="companies", operation="update", input_path=str(source)).execute({})

        assert json.loads(update.calls[0].request.content) == [{"id": "c-1", "seats": 4}, {"id": "c-2", "seats": 9}]

    @respx.mock
    def test_reads_records_from_a_csv_file(self, tmp_path):
        source = tmp_path / "people.csv"
        source.write_text("email,title\ndana@example.com,CFO\nlee@example.com,\n", encoding="utf-8")
        create = respx.post(f"{BASE_URL}/people/bulk/create").mock(
            return_value=httpx.Response(201, json={"successfulIds": ["p-1", "p-2"], "errors": {}})
        )

        DreamhubBulkOperator(
            task_id="load", entity="people", operation="create", input_path=str(source), input_format="csv"
        ).execute({})

        assert json.loads(create.calls[0].request.content) == [
            {"email": "dana@example.com", "title": "CFO"},
            {"email": "lee@example.com", "title": None},
        ]

    def test_needs_exactly_one_source_of_records(self):
        with pytest.raises(ValueError, match="exactly one"):
            DreamhubBulkOperator(task_id="load", entity="companies", operation="create")

    def test_match_field_is_not_for_create(self):
        with pytest.raises(ValueError, match="match_field"):
            DreamhubBulkOperator(
                task_id="load", entity="companies", operation="create", records=[], match_field="external_key"
            )


class TestApi:
    @respx.mock
    def test_returns_the_json_response(self):
        respx.get(f"{BASE_URL}/deals/stages").mock(return_value=httpx.Response(200, json=[{"id": 1}]))

        assert DreamhubApiOperator(task_id="call", endpoint="deals/stages").execute({}) == [{"id": 1}]

    @respx.mock
    def test_a_write_is_not_resent_after_a_gateway_error(self):
        route = respx.post(f"{BASE_URL}/deals").mock(return_value=httpx.Response(502))

        with pytest.raises(Exception, match="HTTP 502"):
            DreamhubApiOperator(task_id="call", endpoint="deals", method="post", data={"name": "x"}).execute({})

        assert route.call_count == 1


class TestProviderRegistration:
    def test_airflow_finds_the_provider_and_its_connection_type(self):
        found = [ep for ep in entry_points(group="apache_airflow_provider") if ep.value.startswith("dreamhub_airflow")]

        assert len(found) == 1
        info = found[0].load()()
        assert info == get_provider_info()
        assert info["connection-types"][0]["connection-type"] == "dreamhub"
        module, _, name = info["connection-types"][0]["hook-class-name"].rpartition(".")
        assert getattr(importlib.import_module(module), name) is DreamhubHook


def _matched(*pairs):
    """A companies filter page holding (id, externalKey) pairs."""
    return _page("companies", [{"id": record_id, "externalKey": key} for record_id, key in pairs])


class TestUpsertByMatchField:
    """An upsert by match_field updates the records it matched and creates only the rest.

    The API's own upsert fills just the empty fields of an existing record, so a matched record
    sent through it would keep its old values.
    """

    @respx.mock
    def test_matched_records_are_updated_and_only_new_ones_upserted(self):
        respx.post(f"{BASE_URL}/companies/filter").mock(return_value=_matched(("c-1", "A")))
        update = respx.put(f"{BASE_URL}/companies/bulk/update").mock(
            return_value=httpx.Response(200, json={"successfulIds": ["c-1"], "errors": {}})
        )
        upsert = respx.put(f"{BASE_URL}/companies/bulk/upsert").mock(
            return_value=httpx.Response(200, json={"successfulIds": ["c-9"], "errors": {}})
        )

        summary = DreamhubBulkOperator(
            task_id="load",
            entity="companies",
            operation="upsert",
            match_field="external_key",
            records=[{"external_key": "NEW", "name": "New Co"}, {"external_key": "A", "name": "Acme", "phone": None}],
        ).execute({})

        assert json.loads(update.calls[0].request.content) == [
            {"external_key": "A", "name": "Acme", "phone": None, "id": "c-1"}
        ]
        # The default override=False: values sent overwrite, a null never clears a stored value.
        assert update.calls[0].request.url.params["override"] == "false"
        assert json.loads(upsert.calls[0].request.content) == [{"external_key": "NEW", "name": "New Co"}]
        assert summary["operation"] == "upsert"
        assert summary["succeeded_ids"] == ["c-1", "c-9"]
        assert summary["failures"] == []

    @respx.mock
    def test_override_writes_nulls_over_matched_records(self):
        respx.post(f"{BASE_URL}/companies/filter").mock(return_value=_matched(("c-1", "A")))
        update = respx.put(f"{BASE_URL}/companies/bulk/update").mock(
            return_value=httpx.Response(200, json={"successfulIds": ["c-1"], "errors": {}})
        )
        upsert = respx.put(f"{BASE_URL}/companies/bulk/upsert")

        DreamhubBulkOperator(
            task_id="load",
            entity="companies",
            operation="upsert",
            match_field="external_key",
            override=True,
            records=[{"external_key": "A", "phone": None}],
        ).execute({})

        assert update.calls[0].request.url.params["override"] == "true"
        assert upsert.call_count == 0

    @respx.mock
    def test_an_upsert_without_match_field_is_one_api_upsert(self):
        upsert = respx.put(f"{BASE_URL}/companies/bulk/upsert").mock(
            return_value=httpx.Response(200, json={"successfulIds": ["c-1", "c-2"], "errors": {}})
        )
        update = respx.put(f"{BASE_URL}/companies/bulk/update")

        DreamhubBulkOperator(
            task_id="load",
            entity="companies",
            operation="upsert",
            records=[{"id": "c-1", "name": "Acme"}, {"name": "Beta"}],
        ).execute({})

        assert json.loads(upsert.calls[0].request.content) == [{"id": "c-1", "name": "Acme"}, {"name": "Beta"}]
        assert upsert.calls[0].request.url.params["override"] == "false"
        assert update.call_count == 0

    @respx.mock
    def test_failures_from_both_requests_are_keyed_in_the_callers_input(self):
        respx.post(f"{BASE_URL}/companies/filter").mock(return_value=_matched(("c-1", "A"), ("c-2", "B")))
        respx.put(f"{BASE_URL}/companies/bulk/update").mock(
            return_value=httpx.Response(206, json={"successfulIds": ["c-2"], "errors": {"c-1": "stage is invalid"}})
        )
        respx.put(f"{BASE_URL}/companies/bulk/upsert").mock(
            return_value=httpx.Response(206, json={"successfulIds": ["c-9"], "errors": {"create_1": "name required"}})
        )

        summary = DreamhubBulkOperator(
            task_id="load",
            entity="companies",
            operation="upsert",
            match_field="external_key",
            fail_on_partial=False,
            records=[
                {"external_key": "N1", "name": "One"},
                {"external_key": "A"},
                {"external_key": "N2"},
                {"external_key": "B"},
            ],
        ).execute({})

        assert summary["succeeded_ids"] == ["c-2", "c-9"]
        # The second new record (input 2) is named by the batch the API saw, in input terms: the
        # upsert carried inputs 0 and 2, so its batch starts at input 0.
        assert summary["failures"] == [
            {"key": "c-1", "error": "stage is invalid"},
            {"key": "new record 1 of the batch starting at 0", "error": "name required"},
        ]

    @respx.mock
    def test_a_stop_in_the_update_leaves_the_new_records_not_attempted(self, tmp_path):
        respx.post(f"{BASE_URL}/companies/filter").mock(return_value=_matched(("c-1", "A"), ("c-2", "B")))
        respx.put(f"{BASE_URL}/companies/bulk/update").mock(
            side_effect=[httpx.Response(200, json={"successfulIds": ["c-1"], "errors": {}}), httpx.Response(504)]
        )
        upsert = respx.put(f"{BASE_URL}/companies/bulk/upsert")
        results = tmp_path / "results.ndjson"

        with pytest.raises(DreamhubBulkError) as raised:
            DreamhubBulkOperator(
                task_id="load",
                entity="companies",
                operation="upsert",
                match_field="external_key",
                batch_size=1,
                results_path=str(results),
                records=[{"external_key": "NEW"}, {"external_key": "A"}, {"external_key": "B"}],
            ).execute({})

        assert upsert.call_count == 0
        assert raised.value.result.succeeded_ids == ["c-1"]
        # Input 1 written, input 2 in flight, input 0 (new) never sent.
        assert (raised.value.unknown, raised.value.not_attempted) == ([2], [0])
        written = _lines(results)[0]
        assert (written["unknown_positions"], written["not_attempted_positions"]) == ([2], [0])

    @respx.mock
    def test_a_stop_in_the_upsert_still_reports_what_the_update_wrote(self, tmp_path):
        respx.post(f"{BASE_URL}/companies/filter").mock(return_value=_matched(("c-1", "A")))
        respx.put(f"{BASE_URL}/companies/bulk/update").mock(
            return_value=httpx.Response(200, json={"successfulIds": ["c-1"], "errors": {}})
        )
        respx.put(f"{BASE_URL}/companies/bulk/upsert").mock(
            side_effect=[httpx.Response(201, json={"successfulIds": ["c-8"], "errors": {}}), httpx.Response(504)]
        )
        results = tmp_path / "results.ndjson"

        with pytest.raises(DreamhubBulkError) as raised:
            DreamhubBulkOperator(
                task_id="load",
                entity="companies",
                operation="upsert",
                match_field="external_key",
                batch_size=1,
                results_path=str(results),
                records=[{"external_key": "N1"}, {"external_key": "A"}, {"external_key": "N2"}, {"external_key": "N3"}],
            ).execute({})

        assert raised.value.result.succeeded_ids == ["c-1", "c-8"]
        # Inputs 1 (update) and 0 (first new) written, 2 in flight, 3 never sent.
        assert (raised.value.unknown, raised.value.not_attempted) == ([2], [3])
        assert "2 records were written" in str(raised.value)
        written = _lines(results)[0]
        assert written["succeeded_ids"] == ["c-1", "c-8"]

    @respx.mock
    def test_a_timeout_in_the_upsert_keeps_airflows_exception_and_the_whole_report(self, tmp_path):
        respx.post(f"{BASE_URL}/companies/filter").mock(return_value=_matched(("c-1", "A")))
        respx.put(f"{BASE_URL}/companies/bulk/update").mock(
            return_value=httpx.Response(200, json={"successfulIds": ["c-1"], "errors": {}})
        )

        def timeout(request):
            raise AirflowTaskTimeout("execution_timeout reached")

        respx.put(f"{BASE_URL}/companies/bulk/upsert").mock(side_effect=timeout)
        results = tmp_path / "results.ndjson"

        with pytest.raises(AirflowTaskTimeout) as raised:
            DreamhubBulkOperator(
                task_id="load",
                entity="companies",
                operation="upsert",
                match_field="external_key",
                results_path=str(results),
                records=[{"external_key": "N1"}, {"external_key": "A"}],
            ).execute({})

        partial = raised.value.dreamhub_partial
        assert partial.result.succeeded_ids == ["c-1"]
        assert (partial.unknown, partial.not_attempted) == ([0], [])
        written = _lines(results)[0]
        assert (written["succeeded_ids"], written["unknown_positions"]) == (["c-1"], [0])

    @respx.mock
    def test_an_error_before_the_upsert_sends_anything_still_reports_the_update(self, monkeypatch):
        respx.post(f"{BASE_URL}/companies/filter").mock(return_value=_matched(("c-1", "A")))
        respx.put(f"{BASE_URL}/companies/bulk/update").mock(
            return_value=httpx.Response(200, json={"successfulIds": ["c-1"], "errors": {}})
        )
        bulk = DreamhubHook.bulk

        def refuse_upsert(self, entity, operation, records, **kwargs):
            if operation == "upsert":
                raise RuntimeError("connection lost")
            return bulk(self, entity, operation, records, **kwargs)

        monkeypatch.setattr(DreamhubHook, "bulk", refuse_upsert)

        with pytest.raises(DreamhubBulkError) as raised:
            DreamhubBulkOperator(
                task_id="load",
                entity="companies",
                operation="upsert",
                match_field="external_key",
                records=[{"external_key": "N1"}, {"external_key": "A"}],
            ).execute({})

        assert raised.value.result.succeeded_ids == ["c-1"]
        assert (raised.value.unknown, raised.value.not_attempted) == ([], [0])
        assert isinstance(raised.value.__cause__, RuntimeError)

    @respx.mock
    def test_unmatched_values_and_a_stop_in_the_upsert_are_reported_by_input_position(self):
        respx.post(f"{BASE_URL}/companies/filter").mock(return_value=_matched(("c-1", "A"), ("c-2", "B"), ("c-3", "B")))
        respx.put(f"{BASE_URL}/companies/bulk/update").mock(
            return_value=httpx.Response(200, json={"successfulIds": ["c-1"], "errors": {}})
        )
        respx.put(f"{BASE_URL}/companies/bulk/upsert").mock(
            side_effect=[httpx.Response(201, json={"successfulIds": ["c-8"], "errors": {}}), httpx.Response(504)]
        )

        with pytest.raises(DreamhubBulkError) as raised:
            DreamhubBulkOperator(
                task_id="load",
                entity="companies",
                operation="upsert",
                match_field="external_key",
                batch_size=1,
                records=[
                    {"external_key": "A"},
                    {"external_key": "B"},
                    {"external_key": None},
                    {"external_key": "N1"},
                    {"external_key": "N2"},
                ],
            ).execute({})

        # Input 0 updated, 1 ambiguous, 2 unusable, 3 created, 4 in flight.
        assert raised.value.result.succeeded_ids == ["c-1", "c-8"]
        assert raised.value.result.failures == [
            {"key": 1, "error": "2 records have this external_key value"},
            {"key": 2, "error": "record has no usable external_key value"},
        ]
        assert (raised.value.unknown, raised.value.not_attempted) == ([4], [])

    @pytest.mark.parametrize("records", [[], [{"external_key": None}]])
    def test_nothing_to_send_still_checks_the_connection(self, monkeypatch, records):
        monkeypatch.setenv("AIRFLOW_CONN_DREAMHUB_NOTOKEN", json.dumps({"conn_type": "dreamhub", "host": BASE_URL}))

        with pytest.raises(ValueError, match="no API token"):
            DreamhubBulkOperator(
                task_id="load",
                entity="companies",
                operation="upsert",
                match_field="external_key",
                dreamhub_conn_id="dreamhub_notoken",
                records=records,
            ).execute({})
