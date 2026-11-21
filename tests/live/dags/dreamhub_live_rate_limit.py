"""Live harness: an extract through a proxy that answers 429 first. The task must wait and finish."""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from live_records import OUT_DIR

from dreamhub_airflow.operators.extract import DreamhubExtractOperator

with DAG(
    dag_id="dreamhub_live_rate_limit",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    params={"run_marker": "", "out_dir": OUT_DIR},
    default_args={"retries": 0},
    tags=["dreamhub", "live-test"],
) as dag:
    DreamhubExtractOperator(
        task_id="extract_through_rate_limit",
        entity="revenue_items",
        output_path="{{ params.out_dir }}/rate_limited_revenue_items.ndjson",
        # A connection to the local proxy, which forwards to the same Dreamhub API.
        dreamhub_conn_id="dreamhub_rate_limited",
    )
