"""One live run: an Airflow container, the Dreamhub account it writes to, and what must be cleaned up after."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

from tests.live.dags.live_records import CompanyFixture
from tests.live.harness.airflow_container import HARNESS_DIR, AirflowContainer, AirflowImage
from tests.live.harness.config import ApiToken, LiveTarget
from tests.live.harness.ledger import RecordLedger
from tests.live.harness.live_api import LiveApi
from tests.live.harness.secrets import SecretScanner

LIVE_DIR = Path(__file__).resolve().parent.parent
DAGS_DIR = LIVE_DIR / "dags"
PROXY_SCRIPT = Path(__file__).resolve().parent / "rate_limit_proxy.py"
OUT_DIR = "/tmp/dreamhub-live"
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


class ConnectionEnvFile:
    """An owner-only env file holding the Airflow connections, removed when the run ends."""

    def __init__(self, target: LiveTarget, token: ApiToken) -> None:
        self._dir = Path(tempfile.mkdtemp(prefix="dreamhub-live-"))
        self._dir.chmod(0o700)
        self.path = self._dir / "airflow.env"
        connections = {
            "DREAMHUB_DEFAULT": {"host": target.base_url, "password": token.reveal()},
            "DREAMHUB_RATE_LIMITED": {
                "host": f"http://127.0.0.1:{PROXY_PORT}/api/v1",
                "password": token.reveal(),
                "extra": {"max_retry_wait": PROXY_RETRY_AFTER_SECONDS * 3},
            },
            "DREAMHUB_REFUSED_TOKEN": {"host": target.base_url, "password": REFUSED_TOKEN},
        }
        lines = [
            f"AIRFLOW_CONN_{name}={json.dumps({'conn_type': 'dreamhub', **conn})}" for name, conn in connections.items()
        ]
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as handle:
            handle.write("\n".join(lines) + "\n")

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
        )
        self._runs = 0

    def start(self) -> None:
        self.airflow.start(DAG_IDS)

    def run_dag(self, dag_id: str) -> tuple[str, str]:
        """Trigger ``dag_id`` with this run's marker and wait for it; returns (run_id, state)."""
        self._runs += 1
        run_id = f"{self.ledger.run_marker}-{self._runs}-{dag_id}"
        self.airflow.trigger(dag_id, run_id, {"run_marker": self.ledger.run_marker, "out_dir": OUT_DIR})
        return run_id, self.airflow.wait_for_run(dag_id, run_id)

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

    def record_created_companies(self) -> dict[str, list[dict[str, object]]]:
        """Read back the run's companies and count each one as created."""
        found = self.api.companies_by_website(self.companies.all_websites())
        self.ledger.created("companies", [str(row["id"]) for rows in found.values() for row in rows])
        return found

    def collect_logs(self) -> Path:
        return self.airflow.copy_logs(self.artifacts)

    def token_findings(self) -> list[str]:
        return self._scanner.scan_tree(self.artifacts)

    def clean_up(self) -> dict[str, object]:
        """Soft-delete anything of this run still readable, then report the count and any leftovers."""
        websites = [*self.companies.all_websites(), self.companies.invalid_website()]
        still_there = self.api.ids_by_website(websites)
        # Anything still readable was created by this run, whether or not a step recorded it.
        self.ledger.created("companies", still_there)
        self.api.delete_companies(still_there, self.ledger)
        remaining = self.api.ids_by_website(websites)
        return {
            "run_marker": self.ledger.run_marker,
            "counts": self.ledger.summary(),
            "leftovers": self.ledger.leftovers(),
            "still_readable": remaining,
        }

    def close(self) -> None:
        try:
            self.airflow.stop()
        finally:
            self._env.remove()
