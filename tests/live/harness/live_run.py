"""One live run: an Airflow container, the Dreamhub account it writes to, and what must be cleaned up after."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from python_on_whales.exceptions import DockerException

from tests.live.dags.live_records import OUT_DIR, CompanyFixture
from tests.live.harness.airflow_container import HARNESS_DIR, AirflowContainer, AirflowImage, AirflowRunError
from tests.live.harness.config import ApiToken, LiveTarget
from tests.live.harness.ledger import RecordLedger
from tests.live.harness.live_api import LiveApi
from tests.live.harness.secrets import SecretScanner

LIVE_DIR = Path(__file__).resolve().parent.parent
DAGS_DIR = LIVE_DIR / "dags"
PROXY_SCRIPT = Path(__file__).resolve().parent / "rate_limit_proxy.py"
PROXY_PORT = 8765
PROXY_REFUSALS = 2
PROXY_RETRY_AFTER_SECONDS = 2
PROXY_STATS = "/tmp/dreamhub-live-proxy-stats.json"
PROXY_LOG = "/tmp/dreamhub-live-proxy.log"

DAG_IDS = [
    "dreamhub_live_extract",
    "dreamhub_live_bulk",
    "dreamhub_live_delete",
    "dreamhub_live_rate_limit",
    "dreamhub_live_failure",
]
# A token the API refuses: same shape as a real one, so the refusal is the API's, not a format check.
REFUSED_TOKEN = "pat_" + "0" * 40
# Every results file a write task may leave in OUT_DIR, read before Airflow stops.
BULK_RESULTS = ["bulk_create.ndjson", "bulk_update.ndjson", "bulk_upsert.ndjson", "bulk_overwrite.ndjson"]
WRITE_RESULTS = [*BULK_RESULTS, "invalid_results.ndjson"]


class ConnectionEnvFile:
    """An owner-only env file holding the Airflow connections, removed when the run ends."""

    def __init__(self, target: LiveTarget, token: ApiToken) -> None:
        self._dir = Path(tempfile.mkdtemp(prefix="dreamhub-live-"))
        self._dir.chmod(0o700)
        self.path = self._dir / "airflow.env"
        lines = [
            self._line("DREAMHUB_DEFAULT", {"host": target.base_url, "password": token.reveal()}),
            self._line(
                "DREAMHUB_RATE_LIMITED",
                {
                    "host": f"http://127.0.0.1:{PROXY_PORT}/api/v1",
                    "password": token.reveal(),
                    "extra": {"max_retry_wait": PROXY_RETRY_AFTER_SECONDS * 3},
                },
            ),
            self._line("DREAMHUB_REFUSED_TOKEN", {"host": target.base_url, "password": REFUSED_TOKEN}),
        ]
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as handle:
            handle.write("\n".join(lines) + "\n")

    @staticmethod
    def _line(name: str, connection: dict[str, object]) -> str:
        return f"AIRFLOW_CONN_{name}={json.dumps({'conn_type': 'dreamhub', **connection})}"

    def remove(self) -> None:
        self.path.unlink(missing_ok=True)
        self._dir.rmdir()


class LiveRun:
    """Runs the live DAGs in one Airflow and accounts for every record they write."""

    def __init__(
        self,
        image: AirflowImage,
        *,
        target: LiveTarget,
        token: ApiToken,
        api: LiveApi,
        wheel: Path,
        labels: dict[str, str],
        keep: bool,
        artifacts: Path,
    ) -> None:
        self.ledger = RecordLedger.for_new_run()
        self.companies = CompanyFixture(self.ledger.run_marker)
        self.api = api
        self.target = target
        self.artifacts = artifacts
        self._scanner = SecretScanner(token)
        self._env = ConnectionEnvFile(target, token)
        self.airflow = AirflowContainer(
            image,
            wheel=wheel,
            dags_dir=DAGS_DIR,
            harness_files=[PROXY_SCRIPT],
            env_file=self._env.path,
            labels=labels,
            keep=keep,
            redact=self._scanner.redact,
        )
        self._runs = 0
        self._unfinished: set[str] = set()

    def start(self) -> None:
        self.airflow.start(DAG_IDS)

    def run_dag(self, dag_id: str) -> tuple[str, str]:
        """Trigger ``dag_id`` with this run's marker and wait for it; returns (run_id, state)."""
        self._runs += 1
        run_id = f"{self.ledger.run_marker}-{self._runs}-{dag_id}"
        self._unfinished.add(run_id)
        self.airflow.trigger(dag_id, run_id, {"run_marker": self.ledger.run_marker, "out_dir": OUT_DIR})
        state = self.airflow.wait_for_run(dag_id, run_id)
        self._unfinished.discard(run_id)
        return run_id, state

    @property
    def has_unfinished_runs(self) -> bool:
        """A triggered run never reached success or failed (a timeout or an interrupt)."""
        return bool(self._unfinished)

    def start_rate_limit_proxy(self) -> None:
        self.airflow.run_in_background(
            PROXY_LOG,
            "python",
            f"{HARNESS_DIR}/{PROXY_SCRIPT.name}",
            "--upstream",
            self.target.origin,
            "--port",
            str(PROXY_PORT),
            "--refusals",
            str(PROXY_REFUSALS),
            "--retry-after",
            str(PROXY_RETRY_AFTER_SECONDS),
            "--stats",
            PROXY_STATS,
        )
        # The proxy writes its first counts once it is listening.
        self.airflow.wait_for_file(PROXY_STATS, PROXY_LOG)

    def proxy_stats(self) -> dict[str, int]:
        return json.loads(self.airflow.read_text(PROXY_STATS))

    def results(self, name: str) -> dict[str, object] | None:
        """The summary a bulk task wrote to ``{OUT_DIR}/{name}``, or None when the task wrote none.

        The operator writes one summary line. Its text is redacted before use, since assertions quote it.
        """
        path = f"{OUT_DIR}/{name}"
        if not self.airflow.file_exists(path):
            return None
        lines = self._scanner.redact(self.airflow.read_text(path)).splitlines()
        if not lines:
            return None
        summary = json.loads(lines[0])
        if not isinstance(summary, dict):
            raise ValueError(f"{name}: the summary is not a JSON object")
        return summary

    def succeeded_ids(self, name: str) -> set[str]:
        """The ids the bulk task that wrote ``{OUT_DIR}/{name}`` reports as succeeded (none if it wrote no file)."""
        summary = self.results(name) or {}
        ids = summary.get("succeeded_ids", [])
        if not isinstance(ids, list):
            raise ValueError(f"{name}: succeeded_ids is not a list")
        return {str(record_id) for record_id in ids}

    def record_written(self, names: list[str]) -> set[str]:
        """Count as created every id the named bulk results report as written; returns those ids.

        Each file is counted as soon as it is read, so a failure on a later one keeps the earlier ones.
        """
        ids: set[str] = set()
        for name in names:
            written = self.succeeded_ids(name)
            self.ledger.created("companies", sorted(written))
            ids |= written
        return ids

    def record_deleted(self, name: str) -> set[str]:
        """Count as deleted every id the named delete results report; returns those ids."""
        ids = self.succeeded_ids(name)
        self.ledger.deleted("companies", sorted(ids))
        return ids

    def read_back_companies(self) -> dict[str, list[dict[str, object]]]:
        """Website → the run's companies as the API returns them, each counted as created."""
        found = self.api.companies_by_website(self.companies.all_websites())
        self.ledger.created("companies", [str(row["id"]) for rows in found.values() for row in rows])
        return found

    def collect_logs(self) -> Path:
        return self.airflow.copy_logs(self.artifacts)

    def token_findings(self) -> list[str]:
        return self._scanner.scan_tree(self.artifacts)

    def tear_down(self) -> dict[str, object]:
        """End the run: count what the tasks reported, stop Airflow, then clean up whatever happened before.

        Results are read while Airflow still runs (a stopped container cannot be read), so even a run
        that timed out has its written ids counted. Airflow is stopped next, so nothing in flight writes
        after the cleanup. Cleanup itself always runs: it needs only the API, not the container.

        ``problems`` lists what could not be confirmed (ids not read, Airflow not stopped). Either one
        means a record could exist that cleanup did not see, so the caller fails the run on it.
        """
        problems: list[str] = []
        try:
            if self.airflow.started:
                self.record_written(WRITE_RESULTS)
        except (AirflowRunError, DockerException, ValueError) as error:
            problems.append(f"results not read: {type(error).__name__}")
        try:
            self.airflow.halt()
        except DockerException as error:
            problems.append(f"Airflow not stopped, a run may still write: {type(error).__name__}")
        report = self.clean_up()
        if problems:
            report["problems"] = problems
        return report

    def clean_up(self) -> dict[str, object]:
        """Soft-delete every record of this run still readable, then report the counts and any leftovers.

        Records come from two independent sources: the ids the bulk tasks reported as written (the
        ledger) and a read by the run's websites, so a record either source misses is still found.
        """
        websites = [*self.companies.all_websites(), self.companies.invalid_website()]
        by_website = self.api.ids_by_website(websites)
        self.ledger.created("companies", by_website)
        pending = sorted(set(self.ledger.leftovers().get("companies", [])) | set(by_website))
        self.api.delete_companies(pending, self.ledger)
        created = self.ledger.created_ids("companies")
        remaining = sorted(set(self.api.ids_by_website(websites)) | set(self.api.existing_ids("companies", created)))
        return {
            "run_marker": self.ledger.run_marker,
            "counts": self.ledger.summary(),
            "leftovers": self.ledger.leftovers(),
            "still_readable": remaining,
        }

    def close(self) -> None:
        try:
            self.airflow.remove()
        finally:
            self._env.remove()
