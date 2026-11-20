"""Edge cases where a read or write could silently lose, misreport or corrupt data."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
import respx

from dreamhub_airflow.hooks.dreamhub import DreamhubConsistencyError, DreamhubHook
from dreamhub_airflow.operators.bulk import MAX_IDS_IN_XCOM, DreamhubBulkOperator
from dreamhub_airflow.operators.extract import DreamhubExtractOperator
from tests.helpers import BASE_URL


def _rows(key: str, rows: list[dict], total: int) -> httpx.Response:
    # No "pages" field, so the reader must decide when to stop from the rows alone.
    return httpx.Response(200, json={key: rows, "total": total})


class TestReadEnd:
    @respx.mock
    def test_a_404_after_a_full_last_page_ends_a_complete_read(self, hook: DreamhubHook) -> None:
        respx.post(f"{BASE_URL}/companies/filter").mock(
            side_effect=[
                _rows("companies", [{"id": "c-1"}, {"id": "c-2"}], total=2),
                httpx.Response(404, json={"detail": "No companies found"}),
            ]
        )

        assert [r["id"] for r in hook.get_records("companies", page_size=2)] == ["c-1", "c-2"]

    @respx.mock
    def test_a_404_after_a_short_read_still_fails_the_count_check(self, hook: DreamhubHook) -> None:
        respx.post(f"{BASE_URL}/companies/filter").mock(
            side_effect=[
                _rows("companies", [{"id": "c-1"}, {"id": "c-2"}], total=3),
                httpx.Response(404, json={"detail": "No companies found"}),
            ]
        )

        with pytest.raises(DreamhubConsistencyError, match="read 2 distinct records but the API reported 3"):
            hook.get_records("companies", page_size=2)

    @respx.mock
    def test_a_bare_list_response_is_read_without_a_count_check(self, hook: DreamhubHook) -> None:
        respx.post(f"{BASE_URL}/tasks/filter").mock(return_value=httpx.Response(200, json=[{"id": "t-1"}]))

        assert hook.get_records("tasks") == [{"id": "t-1"}]


class TestUnreadableRetryAfter:
    @respx.mock
    def test_falls_back_to_backoff(self, hook: DreamhubHook, sleeps: list[float]) -> None:
        respx.post(f"{BASE_URL}/companies/filter").mock(
            side_effect=[
                httpx.Response(429, headers={"Retry-After": "soon"}),
                _rows("companies", [], total=0),
            ]
        )

        hook.get_records("companies")

        assert sleeps == [2.0]


class TestErrorKeys:
    @respx.mock
    def test_a_position_is_mapped_back_after_records_without_an_id_are_held_back(self, hook: DreamhubHook) -> None:
        # The first record has no id, so the API is sent one record; its "0" is that record.
        respx.put(f"{BASE_URL}/deals/bulk/update").mock(
            return_value=httpx.Response(206, json={"successfulIds": [], "errors": {"0": "stage is invalid"}})
        )

        result = hook.bulk("deals", "update", [{"name": "no id"}, {"id": "d-7", "stage": 99}])

        assert result.failures == [
            {"key": 0, "error": "record has no id"},
            {"key": "d-7", "error": "stage is invalid"},
        ]


class TestExtractLeavesOutputOnFailure:
    @respx.mock
    @pytest.mark.parametrize("file_format", ["ndjson", "csv"])
    def test_a_failed_read_keeps_the_previous_file(self, tmp_path: Path, file_format: str) -> None:
        respx.post(f"{BASE_URL}/companies/filter").mock(
            side_effect=[
                _rows("companies", [{"id": "c-1"}, {"id": "c-2"}], total=3),
                _rows("companies", [{"id": "c-3"}, {"id": "c-4"}], total=4),
                _rows("companies", [], total=4),
            ]
        )
        target = tmp_path / f"companies.{file_format}"
        target.write_text("yesterday's complete extract\n", encoding="utf-8")

        with pytest.raises(DreamhubConsistencyError):
            DreamhubExtractOperator(
                task_id="extract",
                entity="companies",
                output_path=str(target),
                file_format=file_format,
                fields=["id"],
                page_size=2,
            ).execute({})

        assert target.read_text(encoding="utf-8") == "yesterday's complete extract\n"


class TestUpsertByMatchField:
    @respx.mock
    def test_an_unmatched_record_is_created_even_if_it_carries_an_id(self) -> None:
        respx.post(f"{BASE_URL}/companies/filter").mock(
            return_value=httpx.Response(404, json={"detail": "No companies found"})
        )
        upsert = respx.put(f"{BASE_URL}/companies/bulk/upsert").mock(
            return_value=httpx.Response(200, json={"successfulIds": ["c-new"], "errors": {}})
        )

        DreamhubBulkOperator(
            task_id="load",
            entity="companies",
            operation="upsert",
            match_field="external_key",
            records=[{"id": "c-unrelated", "external_key": "NEW", "name": "New Co"}],
        ).execute({})

        assert json.loads(upsert.calls[0].request.content) == [{"external_key": "NEW", "name": "New Co"}]


class TestReturnValueSize:
    @respx.mock
    def test_the_returned_ids_are_capped_and_the_counts_are_not(self) -> None:
        total = MAX_IDS_IN_XCOM + 5
        created = iter(range(total))

        def create(request: httpx.Request) -> httpx.Response:
            batch = json.loads(request.content)
            return httpx.Response(201, json={"successfulIds": [f"c-{next(created)}" for _ in batch], "errors": {}})

        respx.post(f"{BASE_URL}/companies/bulk/create").mock(side_effect=create)

        summary = DreamhubBulkOperator(
            task_id="load",
            entity="companies",
            operation="create",
            records=[{"name": f"Company {i}", "status": 1} for i in range(total)],
            batch_size=500,
        ).execute({})

        assert summary["succeeded"] == MAX_IDS_IN_XCOM + 5
        assert len(summary["succeeded_ids"]) == MAX_IDS_IN_XCOM
