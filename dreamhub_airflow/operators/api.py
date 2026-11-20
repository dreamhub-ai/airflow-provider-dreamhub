"""Call any Dreamhub API endpoint."""

from __future__ import annotations

from typing import Any
from urllib.parse import unquote

from dreamhub_airflow._compat import BaseOperator, Context
from dreamhub_airflow.hooks.dreamhub import DreamhubHook
from dreamhub_airflow.records import JsonValue, QueryParams

READ_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})


class DreamhubApiOperator(BaseOperator):
    """Send one request to the Dreamhub API and return the JSON response.

    For endpoints the other operators do not cover. Rate limits are waited out; gateway
    errors are retried only for read methods, or when ``retry_writes=True`` for an endpoint
    that is safe to repeat.

    The whole JSON response is returned, and so pushed to XCom: use it for small responses.

    :param endpoint: Path under the API base URL, e.g. ``deals/stages``. Templated.
    :param method: HTTP method (default ``GET``).
    :param data: JSON body. Templated.
    :param query_params: Query parameters. Templated.
    """

    # Unannotated: Airflow 2 and 3 declare this attribute with different types.
    template_fields = ("endpoint", "data", "query_params")
    template_fields_renderers = {"data": "json", "query_params": "json"}

    def __init__(
        self,
        *,
        endpoint: str,
        method: str = "GET",
        data: JsonValue = None,
        query_params: QueryParams | None = None,
        retry_writes: bool = False,
        dreamhub_conn_id: str = DreamhubHook.default_conn_name,
        # Airflow's own operator arguments (task_id, retries, ...), forwarded unchanged: they are
        # BaseOperator's to type, so this is the only `Any` in this operator.
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.method = method.upper()
        self.data = data
        self.query_params = query_params
        self.retry_writes = retry_writes
        self.dreamhub_conn_id = dreamhub_conn_id

    def execute(self, context: Context) -> JsonValue:
        path = unquote(self.endpoint.partition("?")[0]).rstrip("/")
        if self.method in READ_METHODS or path.endswith(("/filter", "/fetch")):
            # Reads of activities are refused here as in the extract: the API returns rows beyond
            # the requested record. Activity writes are not affected.
            DreamhubHook.check_readable(path)
        hook = DreamhubHook(dreamhub_conn_id=self.dreamhub_conn_id)
        try:
            response = hook.request(
                self.method,
                self.endpoint,
                params=self.query_params,
                json=self.data,
                retry=self.method in READ_METHODS or self.retry_writes,
            )
        finally:
            hook.close()
        if not response.content:
            return None
        return response.json()
