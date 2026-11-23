# Changelog

## 0.1.1

- `DreamhubBulkOperator`: an `upsert` with `match_field` now updates the records it matched with the values
  sent (a null still never clears a stored value unless `override=True`) and upserts only the records it did
  not match. In 0.1.0 matched records went through Dreamhub's upsert, which fills only empty fields, so a
  rerun with changed data left the stored values unchanged.
- README: `upsert` without `match_field` follows Dreamhub's upsert rule (fills empty fields only with
  `override=False`); the retry advice now says what a second run changes.

## 0.1.0

First release.

- `DreamhubHook`: API-token connection (type `dreamhub`), paged reads with duplicate removal and a completeness
  check, rate-limit handling (`Retry-After`), bulk create/update/upsert/delete with per-record results, id
  lookup by any field.
- `DreamhubExtractOperator`: NDJSON or CSV to a local path or object storage.
- `DreamhubBulkOperator`: bulk writes from a list or a file, with optional matching on a field you name.
- `DreamhubApiOperator`: any other endpoint (`endpoint`, `data`, `query_params`).
- Example DAGs for a warehouse extract and a usage update.
