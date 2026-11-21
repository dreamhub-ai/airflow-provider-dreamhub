"""Live harness: two tasks that must fail cleanly: an invalid record and a refused API token."""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from live_records import OUT_DIR, CompanyFixture, Records

from dreamhub_airflow.operators.bulk import DreamhubBulkOperator
from dreamhub_airflow.operators.extract import DreamhubExtractOperator

try:
    from airflow.sdk import task
except ImportError:  # Airflow 2
    from airflow.decorators import task


@task
def invalid_records(params: dict[str, object] | None = None) -> Records:
    return CompanyFixture(str((params or {}).get("run_marker", ""))).invalid()


with DAG(
    dag_id="dreamhub_live_failure",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    params={"run_marker": "", "out_dir": OUT_DIR},
    default_args={"retries": 0},
    tags=["dreamhub", "live-test"],
) as dag:
    DreamhubBulkOperator(
        task_id="create_invalid_record",
        entity="companies",
        operation="create",
        records=invalid_records(),
        results_path="{{ params.out_dir }}/invalid_results.ndjson",
    )
    DreamhubExtractOperator(
        task_id="extract_with_refused_token",
        entity="companies",
        output_path="{{ params.out_dir }}/never_written.ndjson",
        dreamhub_conn_id="dreamhub_refused_token",
    )
