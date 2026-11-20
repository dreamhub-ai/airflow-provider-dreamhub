"""Live harness: soft-delete the run's test companies by a match field (deletes are opt-in in a pipeline)."""

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
def to_delete(params: dict | None = None) -> list[dict[str, object]]:
    return CompanyFixture((params or {}).get("run_marker", "")).to_delete()


with DAG(
    dag_id="dreamhub_live_delete",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    params={"run_marker": ""},
    default_args={"retries": 0},
    tags=["dreamhub", "live-test"],
) as dag:
    DreamhubBulkOperator(
        task_id="delete_by_match_field",
        entity="companies",
        operation="delete",
        match_field=MATCH_FIELD,
        records=to_delete(),
    )
