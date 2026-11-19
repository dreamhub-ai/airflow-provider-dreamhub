"""Provider metadata Airflow reads through the ``apache_airflow_provider`` entry point."""

from __future__ import annotations

from dreamhub_airflow import __version__


def get_provider_info() -> dict:
    return {
        "package-name": "airflow-provider-dreamhub",
        "name": "Dreamhub",
        "description": "Extract Dreamhub records to files or object storage, and load records into Dreamhub in bulk.",
        "versions": [__version__],
        "connection-types": [
            {
                "connection-type": "dreamhub",
                "hook-class-name": "dreamhub_airflow.hooks.dreamhub.DreamhubHook",
            }
        ],
    }
