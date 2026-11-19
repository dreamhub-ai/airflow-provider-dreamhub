"""Extract contracts and revenue items from Dreamhub to object storage, then load them into a warehouse.

Set the Airflow Variable ``dreamhub_export_base`` to where the files go (e.g. ``s3://my-bucket/dreamhub``),
and ``STORAGE_CONN`` below to the Airflow connection for that storage.

The load step is a placeholder: replace it with your warehouse's operator (for Snowflake, a
``SQLExecuteQueryOperator`` running ``COPY INTO <table> FROM @<stage>/... FILE_FORMAT = (TYPE = JSON)``).
Each run replaces the target tables with a full extract.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG

try:  # Airflow 3
    from airflow.providers.standard.operators.empty import EmptyOperator
except ImportError:  # Airflow 2
    from airflow.operators.empty import EmptyOperator

from dreamhub_airflow.operators.extract import DreamhubExtractOperator

BASE = "{{ var.value.get('dreamhub_export_base', 's3://my-bucket/dreamhub') }}"
STORAGE_CONN = "aws_default"

with DAG(
    dag_id="dreamhub_to_warehouse",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["dreamhub", "example"],
) as dag:
    contracts = DreamhubExtractOperator(
        task_id="extract_contracts",
        entity="contracts",
        output_path=f"{BASE}/contracts/{{{{ ds }}}}.ndjson",
        storage_conn_id=STORAGE_CONN,
    )
    revenue_items = DreamhubExtractOperator(
        task_id="extract_revenue_items",
        entity="revenue_items",
        output_path=f"{BASE}/revenue_items/{{{{ ds }}}}.ndjson",
        storage_conn_id=STORAGE_CONN,
    )
    load = EmptyOperator(task_id="load_into_warehouse")

    [contracts, revenue_items] >> load
