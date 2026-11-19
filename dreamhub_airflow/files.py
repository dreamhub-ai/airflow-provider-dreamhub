"""Write records to, and read records from, NDJSON or CSV files, locally or in object storage."""

from __future__ import annotations

import contextlib
import csv
import json
import os
import tempfile
from collections.abc import Iterable, Iterator, Sequence
from typing import Any, TextIO

from dreamhub_airflow._compat import ObjectStoragePath

FILE_FORMATS = ("ndjson", "csv")
_COPY_CHUNK = 1 << 20
# Spreadsheet applications run a cell starting with one of these as a formula.
FORMULA_PREFIXES = ("=", "+", "-", "@", "\t", "\r")


class RecordFile:
    """A file of records at a local path or an object-storage URI (``s3://``, ``gs://``, ...)."""

    def __init__(self, path: str, file_format: str = "ndjson", conn_id: str | None = None) -> None:
        if file_format not in FILE_FORMATS:
            raise ValueError(f"file_format must be one of {', '.join(FILE_FORMATS)}")
        self.path = ObjectStoragePath(path, conn_id=conn_id) if conn_id else ObjectStoragePath(path)
        self.local_path = _local_path(path) if conn_id is None else None
        self.file_format = file_format

    def write(
        self, records: Iterable[dict[str, Any]], fields: Sequence[str] | None = None, neutralize_formulas: bool = True
    ) -> int:
        """Write ``records`` and return how many were written.

        The file is built in a staging file and published only once ``records`` is exhausted
        without error. A read that fails part-way therefore leaves the destination exactly as it
        was. A local destination is replaced in one rename, so it is never half-written either;
        object storage commits the upload when the file is closed.

        ``fields`` keeps only those keys, in that order (and fixes the CSV columns); without
        it, CSV columns are every key seen. Nested values are written as JSON text in CSV.
        """
        if self.local_path is not None:
            return self._write_local(self.local_path, records, fields, neutralize_formulas)
        with tempfile.TemporaryFile("w+", encoding="utf-8", newline="") as staged:
            count = self._write_records(staged, records, fields, neutralize_formulas)
            staged.seek(0)
            with self.path.open("w", encoding="utf-8", newline="") as handle:
                for chunk in iter(lambda: staged.read(_COPY_CHUNK), ""):
                    handle.write(chunk)
        return count

    def _write_local(
        self, target: str, records: Iterable[dict[str, Any]], fields: Sequence[str] | None, neutralize_formulas: bool
    ) -> int:
        directory = os.path.dirname(os.path.abspath(target))
        os.makedirs(directory, exist_ok=True)
        handle, staged = tempfile.mkstemp(dir=directory, prefix=".dreamhub-", suffix=".partial")
        try:
            with os.fdopen(handle, "w", encoding="utf-8", newline="") as staged_file:
                count = self._write_records(staged_file, records, fields, neutralize_formulas)
            os.replace(staged, target)
        except BaseException:
            with contextlib.suppress(FileNotFoundError):
                os.unlink(staged)
            raise
        return count

    def _write_records(
        self, handle: TextIO, records: Iterable[dict[str, Any]], fields: Sequence[str] | None, neutralize_formulas: bool
    ) -> int:
        if self.file_format == "ndjson":
            return self._write_ndjson(handle, records, fields)
        return self._write_csv(handle, records, fields, neutralize_formulas)

    def read(self) -> Iterator[dict[str, Any]]:
        """Read the records back. In CSV every value is text, and an empty cell is read as null
        (``None``), so a blank cell never overwrites a stored value when ``override`` is off."""
        with self.path.open("r", encoding="utf-8", newline="") as handle:
            # Airflow 2 wraps the file in an object that forwards method calls but cannot be
            # iterated, so read line by line through readline.
            lines = iter(handle.readline, "")
            if self.file_format == "ndjson":
                for line in lines:
                    if line.strip():
                        yield json.loads(line)
            else:
                for row in csv.DictReader(lines):
                    yield {key: (value if value != "" else None) for key, value in row.items()}

    @staticmethod
    def _write_ndjson(handle: TextIO, records: Iterable[dict[str, Any]], fields: Sequence[str] | None) -> int:
        count = 0
        for record in records:
            handle.write(json.dumps(_select(record, fields), default=str, ensure_ascii=False))
            handle.write("\n")
            count += 1
        return count

    @classmethod
    def _write_csv(
        cls, handle: TextIO, records: Iterable[dict[str, Any]], fields: Sequence[str] | None, neutralize_formulas: bool
    ) -> int:
        if fields is not None:
            return cls._write_csv_rows(handle, records, list(fields), neutralize_formulas)
        # The columns are every key seen, so the records are held in a second local file
        # until all of them have been read.
        columns: dict[str, None] = {}
        with tempfile.TemporaryFile("w+", encoding="utf-8") as raw:
            for record in records:
                columns.update(dict.fromkeys(record))
                raw.write(json.dumps(record, default=str, ensure_ascii=False))
                raw.write("\n")
            raw.seek(0)
            return cls._write_csv_rows(handle, (json.loads(line) for line in raw), list(columns), neutralize_formulas)

    @staticmethod
    def _write_csv_rows(
        handle: TextIO, records: Iterable[dict[str, Any]], columns: list[str], neutralize_formulas: bool
    ) -> int:
        count = 0
        writer = csv.writer(handle)
        writer.writerow([_cell(c, neutralize_formulas) for c in columns])
        for record in records:
            writer.writerow([_cell(record.get(c), neutralize_formulas) for c in columns])
            count += 1
        return count


def _local_path(path: str) -> str | None:
    """The filesystem path when ``path`` names a local file, None for object storage."""
    if path.startswith("file://"):
        return path[len("file://") :]
    return None if "://" in path else path


def _select(record: dict[str, Any], fields: Sequence[str] | None) -> dict[str, Any]:
    if fields is None:
        return record
    return {name: record.get(name) for name in fields}


def _cell(value: Any, neutralize_formulas: bool) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        text = json.dumps(value, default=str, ensure_ascii=False)
    elif isinstance(value, bool):
        text = "true" if value else "false"
    else:
        text = str(value)
    if neutralize_formulas and text.startswith(FORMULA_PREFIXES) and not _is_number(value):
        return "'" + text
    return text


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)
