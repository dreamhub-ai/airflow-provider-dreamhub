"""Import the Airflow base classes from where each supported Airflow version keeps them.

Airflow 3 moved the authoring interface to ``airflow.sdk``; Airflow 2 keeps it in the
core modules. Everything else in this package imports from here.
"""

from __future__ import annotations

try:  # Airflow 3
    from airflow.sdk import BaseHook, BaseOperator, Context, ObjectStoragePath
except ImportError:  # Airflow 2.9 / 2.10
    from airflow.hooks.base import BaseHook  # type: ignore[no-redef]
    from airflow.io.path import ObjectStoragePath  # type: ignore[no-redef]
    from airflow.models import BaseOperator  # type: ignore[no-redef]
    from airflow.utils.context import Context  # type: ignore[no-redef]

__all__ = ["BaseHook", "BaseOperator", "Context", "ObjectStoragePath"]
