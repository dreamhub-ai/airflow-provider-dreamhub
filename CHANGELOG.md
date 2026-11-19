# Changelog

## 0.1.0

First release.

- `DreamhubHook`: API-token connection (type `dreamhub`), paged reads with duplicate removal and a completeness
  check, rate-limit handling (`Retry-After`), bulk create/update/upsert/delete with per-record results, id
  lookup by any field.
- `DreamhubExtractOperator`: NDJSON or CSV to a local path or object storage.
- `DreamhubBulkOperator`: bulk writes from a list or a file, with optional matching on a field you name.
- `DreamhubApiOperator`: any other endpoint.
- Example DAGs for a warehouse extract and a usage update.
