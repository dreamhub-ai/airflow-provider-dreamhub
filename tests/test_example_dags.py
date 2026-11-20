from __future__ import annotations

from pathlib import Path

import pytest

try:  # Airflow 3
    from airflow.dag_processing.dagbag import DagBag
except ImportError:  # Airflow 2
    from airflow.models import DagBag

EXAMPLES = Path(__file__).resolve().parent.parent / "example_dags"


@pytest.fixture(scope="module")
def dag_bag():
    return DagBag(dag_folder=str(EXAMPLES))


def test_example_dags_load_without_errors(dag_bag):
    assert dag_bag.import_errors == {}
    assert set(dag_bag.dag_ids) == {"dreamhub_to_warehouse", "usage_to_dreamhub"}


def test_the_warehouse_example_extracts_both_entities_before_loading(dag_bag):
    dag = dag_bag.dags["dreamhub_to_warehouse"]

    assert dag.get_task("load_into_warehouse").upstream_task_ids == {"extract_contracts", "extract_revenue_items"}
