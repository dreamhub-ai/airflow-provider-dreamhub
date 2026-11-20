"""Extract Dreamhub records to a file, locally or in object storage."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from dreamhub_airflow._compat import BaseOperator
from dreamhub_airflow.files import RecordFile
from dreamhub_airflow.hooks.dreamhub import MAX_PAGE_SIZE, DreamhubHook


class DreamhubExtractOperator(BaseOperator):
    """Read every record of one entity matching a filter and write it to a file.

    The file can be local or in object storage (``s3://bucket/key.ndjson``, ``gs://...``);
    ``storage_conn_id`` names the Airflow connection for the storage. The task returns
    ``{"path": ..., "count": ...}``; the records themselves never go through XCom.

    :param entity: ``companies``, ``deals``, ``leads``, ``people``, ``contracts``,
        ``revenue_items``, ``tasks``, ``partners``, ``users``, or an API path.
    :param output_path: Where to write the file. Templated.
    :param filters: Dreamhub filter object, e.g. ``{"status": 1}``. Templated.
    :param file_format: ``ndjson`` (default) or ``csv``.
    :param fields: Keep only these fields, in this order.
    :param include_additionals: Also return fields from the additional fields tab.
    :param neutralize_formulas: CSV only. Prefix values a spreadsheet would run as a formula
        with ``'`` (default). Set False when loading the file into a warehouse.
    """

    template_fields: Sequence[str] = ("entity", "output_path", "filters")
    template_fields_renderers = {"filters": "json"}
    ui_color = "#e8f4fd"

    def __init__(
        self,
        *,
        entity: str,
        output_path: str,
        filters: dict[str, Any] | None = None,
        file_format: str = "ndjson",
        fields: Sequence[str] | None = None,
        include_additionals: bool = False,
        page_size: int = MAX_PAGE_SIZE,
        neutralize_formulas: bool = True,
        dreamhub_conn_id: str = DreamhubHook.default_conn_name,
        storage_conn_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.entity = entity
        self.output_path = output_path
        self.filters = filters
        self.file_format = file_format
        self.fields = list(fields) if fields is not None else None
        self.include_additionals = include_additionals
        self.page_size = page_size
        self.neutralize_formulas = neutralize_formulas
        self.dreamhub_conn_id = dreamhub_conn_id
        self.storage_conn_id = storage_conn_id

    def execute(self, context: Any) -> dict[str, Any]:
        hook = DreamhubHook(dreamhub_conn_id=self.dreamhub_conn_id)
        records = hook.iter_records(
            self.entity,
            self.filters,
            page_size=self.page_size,
            include_additionals=self.include_additionals,
        )
        target = RecordFile(self.output_path, self.file_format, conn_id=self.storage_conn_id)
        try:
            count = target.write(records, self.fields, neutralize_formulas=self.neutralize_formulas)
        finally:
            hook.close()
        self.log.info("Wrote %d %s records to %s", count, self.entity, self.output_path)
        return {"path": str(self.output_path), "count": count}
