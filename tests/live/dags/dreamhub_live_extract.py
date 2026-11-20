"""Live harness: extract companies and revenue items to NDJSON files. The harness counts the lines."""

from __future__ import annotations

from datetime import datetime

from airflow import DAG

from dreamhub_airflow.operators.extract import DreamhubExtractOperator

with DAG(
    dag_id="dreamhub_live_extract",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    params={"run_marker": "", "out_dir": "/tmp/dreamhub-live"},
    tags=["dreamhub", "live-test"],
) as dag:
    for entity in ("companies", "revenue_items"):
        DreamhubExtractOperator(
            task_id=f"extract_{entity}",
            entity=entity,
            output_path="{{ params.out_dir }}/" + entity + ".ndjson",
        )
