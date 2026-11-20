"""Live harness: create test companies, then update, upsert and fully overwrite them by a match field."""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from live_records import MATCH_FIELD, CompanyFixture

from dreamhub_airflow.operators.bulk import DreamhubBulkOperator

try:
    from airflow.sdk import task
except ImportError:  # Airflow 2
    from airflow.decorators import task


@task
def records(step: str, params: dict | None = None) -> list[dict[str, object]]:
    fixture = CompanyFixture((params or {}).get("run_marker", ""))
    return getattr(fixture, f"to_{step}")()


with DAG(
    dag_id="dreamhub_live_bulk",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    params={"run_marker": ""},
    default_args={"retries": 0},
    tags=["dreamhub", "live-test"],
) as dag:
    create = DreamhubBulkOperator(
        task_id="create",
        entity="companies",
        operation="create",
        records=records.override(task_id="to_create")("create"),
    )
    update = DreamhubBulkOperator(
        task_id="update_by_match_field",
        entity="companies",
        operation="update",
        match_field=MATCH_FIELD,
        records=records.override(task_id="to_update")("update"),
    )
    upsert = DreamhubBulkOperator(
        task_id="upsert_by_match_field",
        entity="companies",
        operation="upsert",
        match_field=MATCH_FIELD,
        records=records.override(task_id="to_upsert")("upsert"),
    )
    overwrite = DreamhubBulkOperator(
        task_id="full_overwrite",
        entity="companies",
        operation="update",
        match_field=MATCH_FIELD,
        override=True,
        records=records.override(task_id="to_overwrite")("overwrite"),
    )
    create >> update >> upsert >> overwrite
