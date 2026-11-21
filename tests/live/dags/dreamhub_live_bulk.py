"""Live harness: create test companies, then update, upsert and fully overwrite them by a match field.

Each step writes its full result to ``{out_dir}/bulk_<step>.ndjson``; the harness counts every id
those files report as written, so cleanup does not depend on reading the records back.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from live_records import MATCH_FIELD, OUT_DIR, CompanyFixture, Records

from dreamhub_airflow.operators.bulk import DreamhubBulkOperator

try:
    from airflow.sdk import task
except ImportError:  # Airflow 2
    from airflow.decorators import task


@task
def records(step: str, params: dict[str, object] | None = None) -> Records:
    fixture = CompanyFixture(str((params or {}).get("run_marker", "")))
    return fixture.steps()[step]()


def bulk_step(step: str, operation: str, *, override: bool = False) -> DreamhubBulkOperator:
    return DreamhubBulkOperator(
        task_id=step,
        entity="companies",
        operation=operation,
        match_field=None if operation == "create" else MATCH_FIELD,
        override=override,
        records=records.override(task_id=f"to_{step}")(step),
        results_path="{{ params.out_dir }}/bulk_" + step + ".ndjson",
    )


with DAG(
    dag_id="dreamhub_live_bulk",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    params={"run_marker": "", "out_dir": OUT_DIR},
    default_args={"retries": 0},
    tags=["dreamhub", "live-test"],
) as dag:
    (
        bulk_step("create", "create")
        >> bulk_step("update", "update")
        >> bulk_step("upsert", "upsert")
        >> bulk_step("overwrite", "update", override=True)
    )
