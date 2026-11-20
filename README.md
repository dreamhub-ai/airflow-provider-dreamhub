# airflow-provider-dreamhub

Apache Airflow provider for [Dreamhub](https://dreamhub.ai). Move Dreamhub data in and out of your pipelines:

- **Extract** records of any entity, with a filter, to NDJSON or CSV, locally or in object storage
  (S3, GCS, Azure, through Airflow's `ObjectStoragePath`), then load the file into your warehouse.
- **Load** records in bulk: create, update, upsert or delete, with a per-record report of what failed and why.
- **Call** any other Dreamhub API endpoint.

Rate limits are handled for you: when Dreamhub answers `429`, the task waits as long as the API asks (up to
5 minutes per wait, 5 times by default) and continues instead of failing.

Works with Airflow 2.9+ and Airflow 3.

## Install

```bash
pip install airflow-provider-dreamhub
```

Or pin a release straight from GitHub:

```bash
pip install "airflow-provider-dreamhub @ git+https://github.com/dreamhub-ai/airflow-provider-dreamhub@v0.1.0"
```

To write to S3 or GCS, also install the matching Airflow provider (`apache-airflow-providers-amazon` or
`apache-airflow-providers-google`) or the `s3fs` / `gcsfs` filesystem.

## Connect to Dreamhub

1. Create a Dreamhub user for the integration and give it a role with only the permissions your pipelines need
   (read-only for extracts).
2. Sign in to Dreamhub as that user and open **Settings → API Keys**. Generate a key and copy it.
3. In Airflow, create a connection of type **Dreamhub** (`dreamhub_default` is the name the operators use by
   default):
   - **API base URL**: `https://crm.dreamhub.ai/api/v1` (leave empty for this default)
   - **API token**: the key from step 2

   Or as an environment variable:

   ```bash
   export AIRFLOW_CONN_DREAMHUB_DEFAULT='{"conn_type": "dreamhub", "password": "<your key>"}'
   ```

   Optional extras: `{"timeout": 60, "max_retries": 5, "max_retry_wait": 300}`: request timeout in seconds,
   how many times a rate-limited or failed request is retried, and the longest single wait for a rate limit
   in seconds.

The API token is only ever sent to the API base URL: operators refuse an endpoint or entity that is an
absolute URL or contains `..`.

Keys expire. Note the expiry date shown on the API Keys page and replace the key in the Airflow connection
before then.

## Extract

```python
from dreamhub_airflow.operators.extract import DreamhubExtractOperator

DreamhubExtractOperator(
    task_id="extract_contracts",
    entity="contracts",
    filters={"status": [1, 2]},
    output_path="s3://my-bucket/dreamhub/contracts/{{ ds }}.ndjson",
    storage_conn_id="aws_default",
)
```

The task returns `{"path": ..., "count": ...}`. Records are streamed to the file page by page and never pass
through XCom.

| Parameter | Meaning |
|---|---|
| `entity` | `companies`, `deals`, `leads`, `people`, `contracts`, `revenue_items`, `tasks`, `partners` or `users` |
| `filters` | A Dreamhub filter, e.g. `{"status": 1, "amount": {"gte": 50000}, "region": ["EMEA", "NA"]}`. Custom fields can be filtered by their key. |
| `output_path` | Local path or object-storage URI. Templated. |
| `file_format` | `ndjson` (default) or `csv` |
| `fields` | Keep only these fields, in this order |
| `include_additionals` | Also return the fields from the additional fields tab |
| `neutralize_formulas` | CSV only. Values a spreadsheet would run as a formula (starting with `=`, `+`, `-`, `@`) get a leading `'`. On by default; set `False` when the file goes to a warehouse. |

**Completeness.** A read fails instead of producing a partial file: if the number of records the API reports
changes during the read, or the rows read do not add up to it, the task raises `DreamhubConsistencyError` and
leaves `output_path` as it was. Run the task again; to have Airflow do that for you, set `retries` on the task
or in the DAG's `default_args` (the examples use `{"retries": 2}`).

## Load in bulk

```python
from dreamhub_airflow.operators.bulk import DreamhubBulkOperator

DreamhubBulkOperator(
    task_id="update_usage",
    entity="companies",
    operation="update",
    input_path="s3://my-bucket/usage/{{ ds }}.ndjson",
    storage_conn_id="aws_default",
    match_field="account_number",  # a field on the Dreamhub company, custom fields included
)
```

| Parameter | Meaning |
|---|---|
| `operation` | `create`, `update`, `upsert` or `delete` |
| `records` / `input_path` | The records: a list of objects (e.g. `task.output` of an upstream task, or a JSON array rendered from XCom), or an NDJSON/CSV file. In CSV every value is text and an empty cell is null. |
| `input_format` | `ndjson` (default) or `csv` |
| `match_field` | For `update`, `upsert` and `delete`: find each record's Dreamhub id by this field instead of requiring an `id`. Values are compared as text, so `"42"` from a CSV file matches a stored `42`. A value no record holds fails that record (`update`, `delete`) or creates a new record (`upsert`); a value held by several records, or an empty value, fails that record. |
| `override` | `False` (default): a null in your data never clears a stored value. `True`: nulls are written. |
| `match_existing` | `upsert` only. Records without an id are matched on Dreamhub's natural keys (email for people; domain and name for companies) before a new one is created. Default `True`. |
| `batch_size` | Records per API request, 1 to 500 (default 200) |
| `fail_on_partial` | Fail the task when any record failed (default `True`) |
| `results_path` | Also write the full result, with every failure and its reason, to this path |

The task returns `{"operation", "succeeded", "failed", "succeeded_ids", "failures"}`. Each failure has a `key`,
the record's Dreamhub id or its position (0-based) in your data, and the `error`. One exception: an `upsert`
with `match_existing=True` reports a new record Dreamhub could not create as `new record <n> of the batch
starting at <position>`, because Dreamhub matched some records of that batch to existing ones first.

The task log lists only the keys of failed records, never record values or per-record reasons, so data such
as emails stays out of logs. Set `results_path` to keep the reasons when the task fails. The reasons are
Dreamhub's own messages; a validation message can quote the value it rejected, so treat the results file and
the task's return value like the data you load.

Notes:

- `upsert` with `override=False` fills only fields that are empty in Dreamhub; `update` with `override=False`
  writes every non-null value you send.
- A request that may already have been applied is never resent automatically. If the run stops part-way, for
  whatever reason, the task raises `DreamhubBulkError`: what was written and what failed before it stopped is
  logged and written to `results_path`, together with `unknown_positions` (the batch in flight, which may or
  may not have been written) and `not_attempted_positions`, all as positions in your data.
- Records written before a task fails stay written. Running a `create` again creates them again, so for loads
  that may be retried, use `upsert` with `match_field`: a second run updates the records the first run made.
- The task returns at most 100 failures and 1,000 ids; set `results_path` to keep the complete result.
- `delete` is a soft delete, one request per record; `batch_size` and `override` do not apply to it.
- When every record of a batch fails, Dreamhub reports one reason for the whole batch.

## Other endpoints

```python
from dreamhub_airflow.operators.api import DreamhubApiOperator

DreamhubApiOperator(task_id="stages", endpoint="deals/stages", query_params={"limit": 50})
```

`endpoint`, `data` and `query_params` are templated. The JSON response is the task's return value, so it goes
to XCom: use this for small responses. Reads are retried on gateway errors; writes are not, unless you set
`retry_writes=True` for an endpoint that is safe to repeat.

## Using the hook directly

```python
from dreamhub_airflow.hooks.dreamhub import DreamhubHook

hook = DreamhubHook()
try:
    for company in hook.iter_records("companies", {"status": 1}):
        ...
    result = hook.bulk("companies", "update", [{"id": "...", "seats": 10}])
finally:
    hook.close()
```

## Example DAGs

See [`example_dags/`](example_dags):

- `dreamhub_to_warehouse.py`: extract contracts and revenue items to object storage, then load them into a
  warehouse table.
- `usage_to_dreamhub.py`: read a usage file and update companies matched on a key field.

## Limitations

- Each extract reads the full result of its filter (no incremental "changed since" read yet).
- Activities cannot be read yet; this comes with a later release.
- A bulk request carries at most 500 records; larger inputs are split automatically.

## Troubleshooting

- **Every request returns HTTP 401.** The API key has expired or was revoked, or the Dreamhub user it belongs
  to was deactivated. Check the key on **Settings → API Keys** and replace it in the Airflow connection.
- **The integration user was deactivated without anyone doing it.** If your Dreamhub account provisions users
  from your identity provider (SCIM), a user that does not exist there is deactivated when the two are
  reconciled. Ask your Dreamhub admin to exclude the integration user's email from SCIM deprovisioning, or
  create the user in your identity provider.
- **`DreamhubConsistencyError`.** Records were added or removed while the extract was reading. Run the task
  again (see *Completeness* above).
- **`DreamhubApiError` with HTTP 429 after several retries.** The rate limit lasted longer than the retry budget.
  Raise `max_retries` or `max_retry_wait` in the connection extras, or run fewer tasks in parallel.

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md). Security issues: see [SECURITY.md](SECURITY.md).

## License

[Apache License 2.0](LICENSE)
