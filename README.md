# airflow-provider-dreamhub

Apache Airflow provider for [Dreamhub](https://dreamhub.ai). Move Dreamhub data in and out of your pipelines:

- **Extract** records of any entity, with a filter, to NDJSON or CSV, locally or in object storage
  (S3, GCS, Azure, through Airflow's `ObjectStoragePath`), then load the file into your warehouse.
- **Load** records in bulk: create, update, upsert or delete, with a per-record report of what failed and why.
- **Call** any other Dreamhub API endpoint.

Rate limits are handled for you: when Dreamhub answers `429`, the task waits as long as the API asks and
continues instead of failing.

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

   Optional extras: `{"timeout": 60, "max_retries": 5}`.

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
| `entity` | `companies`, `deals`, `leads`, `people`, `contracts`, `revenue_items`, `tasks`, `partners`, `users`, or `activities` (see below) |
| `filters` | A Dreamhub filter, e.g. `{"status": 1, "amount": {"gte": 50000}, "region": ["EMEA", "NA"]}`. Custom fields can be filtered by their key. |
| `output_path` | Local path or object-storage URI. Templated. |
| `file_format` | `ndjson` (default) or `csv` |
| `fields` | Keep only these fields, in this order |
| `include_additionals` | Also return the fields from the additional fields tab |
| `neutralize_formulas` | CSV only. Values a spreadsheet would run as a formula (starting with `=`, `+`, `-`, `@`) get a leading `'`. On by default; set `False` when the file goes to a warehouse. |

**Activities** are read per parent record: pass `entity="activities", parent_entity="companies",
parent_id="<company id>"`.

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
    match_field="customer_account_number",  # a field on the Dreamhub company, custom fields included
)
```

| Parameter | Meaning |
|---|---|
| `operation` | `create`, `update`, `upsert` or `delete` |
| `records` / `input_path` | The records, as a list (e.g. from XCom) or an NDJSON/CSV file |
| `match_field` | For `update`, `upsert` and `delete`: find each record's Dreamhub id by this field instead of requiring an `id`. A value no record holds fails that record (`update`, `delete`) or creates a new record (`upsert`); a value held by several records fails that record. |
| `override` | `False` (default): a null in your data never clears a stored value. `True`: nulls are written. |
| `match_existing` | `upsert` only. Records without an id are matched on Dreamhub's natural keys (email for people; domain and name for companies) before a new one is created. Default `True`. |
| `batch_size` | Records per API request, 1 to 500 (default 200) |
| `fail_on_partial` | Fail the task when any record failed (default `True`) |
| `results_path` | Also write the full result, with every failure, to this path |

The task returns `{"operation", "succeeded", "failed", "succeeded_ids", "failures"}`. Each failure has a `key`
(the record's Dreamhub id; its position in your data; or `field=value` when `match_field` found no single
match) and the `error`.

Notes:

- `upsert` with `override=False` fills only fields that are empty in Dreamhub; `update` with `override=False`
  writes every non-null value you send.
- A request that may already have been applied is never resent automatically. If a write times out, check
  the records before running the task again.
- Records written before a task fails stay written. Running a `create` again creates them again, so for loads
  that may be retried, use `upsert` with `match_field`: a second run updates the records the first run made.
- The task returns at most 100 failures and 1,000 ids; set `results_path` to keep the complete result.
- `delete` is a soft delete of each record.

## Other endpoints

```python
from dreamhub_airflow.operators.api import DreamhubApiOperator

DreamhubApiOperator(task_id="stages", endpoint="deals/stages")
```

Reads are retried on gateway errors; writes are not, unless you set `retry_writes=True` for an endpoint that is
safe to repeat.

## Using the hook directly

```python
from dreamhub_airflow.hooks.dreamhub import DreamhubHook

hook = DreamhubHook()
for company in hook.iter_records("companies", {"status": 1}):
    ...
result = hook.bulk("companies", "update", [{"id": "...", "seats": 10}])
```

## Example DAGs

See [`example_dags/`](example_dags):

- `dreamhub_to_warehouse.py`: extract contracts and revenue items to object storage, then load them into a
  warehouse table.
- `usage_to_dreamhub.py`: read a usage file and update companies matched on a key field.

## Limitations

- Each extract reads the full result of its filter (no incremental "changed since" read yet).
- Activities are read per parent record.
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

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md). Security issues: see [SECURITY.md](SECURITY.md).

## License

[Apache License 2.0](LICENSE)
