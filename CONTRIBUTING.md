# Contributing to airflow-provider-dreamhub

Thanks for your interest in contributing! This provider is built by [Dreamhub](https://dreamhub.ai) and we
welcome community contributions.

## Before You Start

- **Open an issue first.** Before writing code, open a GitHub issue describing what you want to change. Security issues are the exception: report them privately as described in [SECURITY.md](SECURITY.md), never in a public issue.
- **Only pick up issues labeled `help wanted` or `good first issue`.** These are scoped for community contributors.

## Development Workflow

1. Fork this repository and clone your fork.
2. Create a feature branch from `main`.
3. Install Airflow with its constraints file, then the provider and its test tools:
   ```bash
   python -m venv .venv
   .venv/bin/pip install "apache-airflow==3.3.2" \
     --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt
   .venv/bin/pip install -e ".[test,lint]"
   ```
4. Run the checks:
   ```bash
   .venv/bin/ruff check . && .venv/bin/ruff format --check .
   .venv/bin/pytest --cov
   ```
5. Push your branch and open a pull request against `main`.

## Live test in a real Airflow (Dreamhub maintainers)

`tests/live` runs the provider inside real Airflow 2 and Airflow 3 containers (the official
`apache/airflow` images in standalone mode) against Dreamhub's development environment. It is
not part of the default test run and needs credentials only Dreamhub maintainers have.

It builds the provider wheel from your checkout and installs it in each Airflow. Then it runs
these DAGs through the Airflow scheduler:

- **Extract**: companies and revenue items to NDJSON files. The line counts must equal the API's
  totals.
- **Bulk**: create test companies, then update and upsert them by a match field (a null under the
  default `override=False` keeps the stored value), and clear a field with `override=True`. The ids
  the tasks report as written must be exactly the companies that read back.
- **Delete**: delete the test companies by the match field.
- **Rate limit**: an extract through a local proxy that answers `429` with `Retry-After` first.
  The task must wait and finish.
- **Failures**: an invalid record and a refused token. Both tasks must fail with a reason, and no
  record may be written to Dreamhub.

Every record a run writes carries the run's marker. When the run ends, Airflow is stopped first, so
nothing still running can write afterwards. Then every record the tasks reported as written, or that
still reads back by the run's websites, is deleted and counted, and leftover records fail the run.
No task log or container output may contain the token.

If a run is killed before it can clean up, its records stay. Each record's website starts with
`https://airflow-live-<id>-`, where `<id>` is the run marker printed in the summary, and its name
starts with the marker. Find those companies in the development environment and delete them.

The live harness uses its own `DREAMHUB_LIVE_*` settings rather than the `DREAMHUB_E2E_*` ones of the
read-only `e2e` tests, because it writes and refuses every environment but the development one.

1. Install the extra tools and start Docker:
   ```bash
   .venv/bin/pip install -e ".[test,lint,live]"
   ```
2. Put an API key of a dedicated test user in a file only you can read, and set the development
   API URL (ask the team; the harness refuses any other environment):
   ```bash
   install -m 600 /dev/null ~/.dreamhub-live-token   # then paste the key into it
   export DREAMHUB_LIVE_TOKEN_FILE=~/.dreamhub-live-token
   export DREAMHUB_LIVE_URL=<development API URL ending in /api/v1>
   ```
   `DREAMHUB_LIVE_TOKEN` works too. The token reaches Airflow through an owner-only env file,
   never through a command line. It is then part of the container's environment, visible to
   `docker inspect`, until the container is removed, so use a dedicated key you can revoke.
   Without these settings, `pytest -m airflow_live` fails rather than skipping.
3. Run it:
   ```bash
   .venv/bin/pytest -m airflow_live -v
   ```
   `DREAMHUB_LIVE_AIRFLOW=3` (or `2`) runs one Airflow version only. `DREAMHUB_LIVE_LABELS=key=value,...`
   adds labels to the containers. The summary at the end lists each run's marker and cleanup
   counts.

**See the runs in the Airflow UI.** Run with `DREAMHUB_LIVE_KEEP=1` and the containers stay up
after the test (Airflow is stopped for the cleanup, then started again). The UI is published on `127.0.0.1` only (port 8082 for Airflow 2, 8083 for
Airflow 3). The summary prints each UI's address, the command that shows its generated login, and
the command that removes the container. The Dreamhub connections are environment variables, so the
UI's Connections page does not show them; remove the container when you are done, and revoke the
test key when you no longer need it.

## Pull Request Guidelines

- Reference the GitHub issue your PR addresses (e.g., `Fixes #123`).
- Keep PRs focused: one logical change per PR.
- Tests must pass on both Airflow 2 and Airflow 3 (CI runs both).
- Do not commit secrets, credentials, or API keys.

## License

By submitting a pull request, you agree that your contributions are licensed under the [Apache License 2.0](LICENSE).
