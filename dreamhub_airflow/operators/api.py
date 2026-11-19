"""Call any Dreamhub API endpoint."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from dreamhub_airflow._compat import BaseOperator
from dreamhub_airflow.hooks.dreamhub import DreamhubHook

READ_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})


class DreamhubApiOperator(BaseOperator):
    """Send one request to the Dreamhub API and return the JSON response.

    For endpoints the other operators do not cover. Rate limits are waited out; gateway
    errors are retried only for read methods, or when ``retry_writes=True`` for an endpoint
    that is safe to repeat.

    :param endpoint: Path under the API base URL, e.g. ``deals/stages``. Templated.
    :param method: HTTP method (default ``GET``).
    :param data: JSON body. Templated.
    :param params: Query parameters. Templated.
    """

    template_fields: Sequence[str] = ("endpoint", "data", "params")
    template_fields_renderers = {"data": "json", "params": "json"}

    def __init__(
        self,
        *,
        endpoint: str,
        method: str = "GET",
        data: Any = None,
        params: dict[str, Any] | None = None,
        retry_writes: bool = False,
        dreamhub_conn_id: str = DreamhubHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.method = method.upper()
        self.data = data
        self.params = params
        self.retry_writes = retry_writes
        self.dreamhub_conn_id = dreamhub_conn_id

    def execute(self, context: Any) -> Any:
        hook = DreamhubHook(dreamhub_conn_id=self.dreamhub_conn_id)
        try:
            response = hook.request(
                self.method,
                self.endpoint,
                params=self.params,
                json=self.data,
                retry=self.method in READ_METHODS or self.retry_writes,
            )
        finally:
            hook.close()
        if not response.content:
            return None
        return response.json()
