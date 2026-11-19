from __future__ import annotations

import csv
import json
from importlib.metadata import entry_points

import httpx
import pytest
import respx

from dreamhub_airflow.get_provider_info import get_provider_info
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
                {"external_key": "A", "seats": 10},
                {"external_key": "B", "seats": 3},
                {"external_key": "Z", "seats": 1},
            ],
        ).execute({})

        assert json.loads(update.calls[0].request.content) == [{"external_key": "A", "seats": 10, "id": "c-1"}]
        assert summary["failures"] == [
            {"key": 1, "error": "2 records have this external_key value"},
            {"key": 2, "error": "no record has this external_key value"},
        ]

    @respx.mock
    def test_upsert_creates_records_no_match_was_found_for(self):
        respx.post(f"{BASE_URL}/companies/filter").mock(return_value=httpx.Response(404))
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
