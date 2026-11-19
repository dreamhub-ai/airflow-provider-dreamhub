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
   .venv/bin/pip install -e ".[test]"
   ```
4. Run the checks:
   ```bash
   .venv/bin/ruff check . && .venv/bin/ruff format --check .
   .venv/bin/pytest --cov
   ```
5. Push your branch and open a pull request against `main`.

## Pull Request Guidelines

- Reference the GitHub issue your PR addresses (e.g., `Fixes #123`).
- Keep PRs focused: one logical change per PR.
- Tests must pass on both Airflow 2 and Airflow 3 (CI runs both).
- Do not commit secrets, credentials, or API keys.

## License

By submitting a pull request, you agree that your contributions are licensed under the [Apache License 2.0](LICENSE).
