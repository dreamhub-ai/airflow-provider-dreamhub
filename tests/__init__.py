"""Airflow reads its home, examples flag and connections from the environment on first import.

This package module runs before any test module or conftest imports Airflow, so the settings
below apply to every test.
"""

import json
import os
import tempfile

from tests.helpers import BASE_URL

os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
os.environ.setdefault("AIRFLOW_HOME", tempfile.mkdtemp(prefix="airflow-home-"))
os.environ["AIRFLOW_CONN_DREAMHUB_DEFAULT"] = json.dumps(
    {"conn_type": "dreamhub", "host": BASE_URL, "password": "pat_test_token", "extra": {"max_retries": 3}}
)
