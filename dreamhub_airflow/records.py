"""The shape of the data the API sends and receives.

Records carry tenant-defined custom fields, so their keys are not known in advance; their
values are JSON.
"""

from __future__ import annotations

from typing import TypeAlias

JsonValue: TypeAlias = str | int | float | bool | None | list["JsonValue"] | dict[str, "JsonValue"]
# One record (or any JSON object) as the API sends and receives it.
Record: TypeAlias = dict[str, JsonValue]
# Query-string parameters sent to the API.
QueryParams: TypeAlias = dict[str, str | int]

__all__ = ["JsonValue", "QueryParams", "Record"]
