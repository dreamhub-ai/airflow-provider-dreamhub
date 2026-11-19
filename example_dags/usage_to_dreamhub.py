"""Update Dreamhub companies from a daily usage file, matching each row to a company by a key field.

The file is NDJSON, one row per account, for example::

    {"account_number": "A-1001", "active_seats": 42, "last_login_at": "2026-11-18"}

``account_number`` must be a field on the Dreamhub company (a custom field works) holding the same value.
The other keys are the Dreamhub fields to update. Rows whose key matches no company, or more than one,
fail the task; their input positions are in the task log and, with their reasons, in the results file.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG

from dreamhub_airflow.operators.bulk import DreamhubBulkOperator

with DAG(
    dag_id="usage_to_dreamhub",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["dreamhub", "example"],
) as dag:
    DreamhubBulkOperator(
        task_id="update_companies",
        entity="companies",
        operation="update",
        input_path="s3://my-bucket/usage/{{ ds }}.ndjson",
        storage_conn_id="aws_default",
        match_field="account_number",
        results_path="s3://my-bucket/usage/results/{{ ds }}.ndjson",
    )
