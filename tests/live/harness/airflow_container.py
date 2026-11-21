"""A real Airflow, from the official image in standalone mode, with the provider wheel installed."""

from __future__ import annotations

import json
import shlex
import shutil
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from python_on_whales import Container, docker
from python_on_whales.exceptions import DockerException

# Value of the container's `purpose` label; leftovers of earlier runs are found by it.
CONTAINER_PURPOSE = "dreamhub-airflow-live"
DAGS_DIR = "/opt/airflow/dags"
# `docker cp` cannot create directories, so files go into ones the image already has.
HARNESS_DIR = "/tmp"
WHEEL_DIR = "/tmp"
LOGS_DIR = "/opt/airflow/logs"
FINISHED_STATES = frozenset({"success", "failed"})
STOP_TIMEOUT_SECONDS = 30


class AirflowRunError(Exception):
    """Airflow did not start, parse the DAGs, or finish a run in time."""


@dataclass(frozen=True)
class AirflowImage:
    """One official Airflow image the harness runs, and where its CLI and UI differ by major version."""

    tag: str
    major: int
    ui_port: int

    @property
    def label(self) -> str:
        return self.tag.rsplit(":", 1)[-1]

    @property
    def ui_password_file(self) -> str:
        if self.major >= 3:
            return "/opt/airflow/simple_auth_manager_passwords.json.generated"
        return "/opt/airflow/standalone_admin_password.txt"

    def dag_args(self, dag_id: str) -> list[str]:
        """``airflow dags list-runs`` takes the DAG id as ``-d`` in Airflow 2 and positionally in 3."""
        return ["-d", dag_id] if self.major < 3 else [dag_id]


AIRFLOW_2 = AirflowImage("apache/airflow:2.10.5-python3.11", major=2, ui_port=8082)
AIRFLOW_3 = AirflowImage("apache/airflow:3.4.0-python3.12", major=3, ui_port=8083)
IMAGES = (AIRFLOW_2, AIRFLOW_3)


class AirflowContainer:
    """Starts one Airflow, runs DAGs through its scheduler, and removes it again (unless kept for its UI)."""

    def __init__(
        self,
        image: AirflowImage,
        *,
        wheel: Path,
        dags_dir: Path,
        harness_files: list[Path],
        env_file: Path,
        labels: dict[str, str],
        keep: bool,
        redact: Callable[[str], str],
    ) -> None:
        self.image = image
        self._wheel = wheel
        self._dags_dir = dags_dir
        self._harness_files = harness_files
        self._env_file = env_file
        # `purpose` last: a caller's labels must not hide the container from the leftover sweep.
        self._labels = {**labels, "purpose": CONTAINER_PURPOSE}
        self.keep = keep
        self._redact = redact
        self._container: Container | None = None
        self._dag_ids: list[str] = []

    @property
    def started(self) -> bool:
        return self._container is not None

    @property
    def name(self) -> str:
        return self.container.name

    @property
    def container(self) -> Container:
        if self._container is None:
            raise AirflowRunError("the Airflow container is not started")
        return self._container

    # ------------------------------------------------------------------ lifecycle

    def start(self, dag_ids: list[str], timeout: float = 300) -> None:
        self._remove_stopped_leftovers()
        self._dag_ids = list(dag_ids)
        command = f"pip install --quiet --no-deps {WHEEL_DIR}/{self._wheel.name} && exec airflow standalone"
        self._container = docker.container.create(
            self.image.tag,
            ["bash", "-c", command],
            labels=self._labels,
            # The token reaches Airflow through an owner-only env file, never through the command line.
            # It is then part of the container's environment (`docker inspect`) until the container is removed.
            env_files=[self._env_file],
            envs={"AIRFLOW__CORE__LOAD_EXAMPLES": "false", "AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION": "false"},
            # The UI is published only when kept, and only on this machine.
            publish=[("127.0.0.1:" + str(self.image.ui_port), 8080)] if self.keep else [],
        )
        docker.copy(str(self._wheel), (self.container, f"{WHEEL_DIR}/{self._wheel.name}"))
        for path in sorted(self._dags_dir.glob("*.py")):
            docker.copy(str(path), (self.container, f"{DAGS_DIR}/{path.name}"))
        for path in self._harness_files:
            docker.copy(str(path), (self.container, f"{HARNESS_DIR}/{path.name}"))
        docker.container.start(self.container)
        self._wait_for_dags(timeout)

    def halt(self) -> None:
        """Stop Airflow, so no DAG run still in flight can write after the run is cleaned up."""
        if self._container is not None:
            docker.container.stop(self._container, time=STOP_TIMEOUT_SECONDS)

    def resume(self, timeout: float = 300) -> None:
        """Start a halted, kept Airflow again so its UI can be opened. Unfinished runs end as failed."""
        docker.container.start(self.container)
        self._wait_for_dags(timeout)

    def stop(self) -> None:
        """Remove the container, unless it is kept so its UI can be opened."""
        if self._container is not None and not self.keep:
            docker.container.remove(self._container, force=True)
            self._container = None

    def ui_instructions(self) -> str:
        return (
            f"Airflow {self.image.label} UI: http://127.0.0.1:{self.image.ui_port} "
            f"(login: docker exec {self.name} cat {self.image.ui_password_file}); "
            f"remove it with: docker rm -f {self.name}"
        )

    def _remove_stopped_leftovers(self) -> None:
        """Containers an interrupted earlier run left behind, stopped; a kept, running one is left alone."""
        for leftover in docker.container.list(all=True, filters={"label": f"purpose={CONTAINER_PURPOSE}"}):
            if leftover.state.status != "running":
                docker.container.remove(leftover, force=True)

    def _wait_for_dags(self, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if not docker.container.inspect(self.container.id).state.running:
                output = self._redact(docker.container.logs(self.container, tail=40))
                raise AirflowRunError(f"Airflow exited during start:\n{output}")
            try:
                listed = {row.get("dag_id") for row in self._airflow_json("dags", "list")}
                # A DAG that fails to import is never listed: report it now rather than at the timeout.
                errors = self._airflow_json("dags", "list-import-errors")
            except DockerException:
                listed, errors = set(), []
            if errors:
                raise AirflowRunError(f"DAG import errors: {self._redact(json.dumps(errors))[:2000]}")
            if set(self._dag_ids) <= listed:
                return
            time.sleep(2)
        raise AirflowRunError(f"Airflow did not list {self._dag_ids} within {timeout:.0f}s")

    # ------------------------------------------------------------------ checks

    def provider_registered(self) -> bool:
        providers = self._airflow("providers", "list", "-o", "plain")
        hooks = self._airflow("providers", "hooks", "-o", "plain")
        return "airflow-provider-dreamhub" in providers and "dreamhub" in hooks

    # ------------------------------------------------------------------ runs

    def trigger(self, dag_id: str, run_id: str, conf: dict[str, object]) -> None:
        self._airflow("dags", "trigger", dag_id, "-r", run_id, "-c", json.dumps(conf))

    def wait_for_run(self, dag_id: str, run_id: str, timeout: float = 600) -> str:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            runs = self._airflow_json("dags", "list-runs", *self.image.dag_args(dag_id))
            state = next((str(run["state"]) for run in runs if run.get("run_id") == run_id), None)
            if state in FINISHED_STATES:
                return state
            time.sleep(3)
        raise AirflowRunError(f"{dag_id} run {run_id} did not finish within {timeout:.0f}s")

    def task_states(self, dag_id: str, run_id: str) -> dict[str, str]:
        rows = self._airflow_json("tasks", "states-for-dag-run", dag_id, run_id)
        return {str(row["task_id"]): str(row["state"]) for row in rows}

    # ------------------------------------------------------------------ files and processes

    def run_in_background(self, log_path: str, *command: str) -> None:
        """Start ``command`` in the container and return; its output goes to ``log_path`` there."""
        line = " ".join(shlex.quote(part) for part in command)
        docker.execute(self.container, ["bash", "-c", f"exec {line} > {shlex.quote(log_path)} 2>&1"], detach=True)

    def read_text(self, path: str) -> str:
        return self._execute(["cat", path])

    def count_lines(self, path: str) -> int:
        return int(self._execute(["bash", "-c", f"wc -l < {shlex.quote(path)}"]).strip())

    def file_exists(self, path: str) -> bool:
        return self._execute(["bash", "-c", f"test -e {shlex.quote(path)} && echo yes || echo no"]) == "yes"

    def wait_for_file(self, path: str, log_path: str, timeout: float = 30) -> None:
        """Wait until a background process has written ``path`` (its sign that it is ready)."""
        deadline = time.monotonic() + timeout
        while not self.file_exists(path):
            if time.monotonic() > deadline:
                output = self.read_text(log_path) if self.file_exists(log_path) else "(no output)"
                raise AirflowRunError(
                    f"{path} did not appear within {timeout:.0f}s; output:\n{self._redact(output[-2000:])}"
                )
            time.sleep(0.5)

    def copy_logs(self, destination: Path) -> Path:
        """Every task log Airflow wrote, plus the container's own output, under ``destination``.

        Each call replaces the previous copy: `docker cp` into an existing directory nests the new
        copy inside it, which would make every later read see each log twice.
        """
        destination.mkdir(parents=True, exist_ok=True)
        logs = destination / "airflow-logs"
        shutil.rmtree(logs, ignore_errors=True)
        docker.copy((self.container, LOGS_DIR), str(logs))
        (destination / "container-output.log").write_text(docker.container.logs(self.container))
        return destination

    def task_log(self, logs_root: Path, dag_id: str, task_id: str) -> str:
        """The text of every attempt log of one task, found under a copy made by :meth:`copy_logs`."""
        wanted = {f"dag_id={dag_id}", f"task_id={task_id}"}
        logs = sorted(p for p in (logs_root / "airflow-logs").rglob("*.log") if wanted <= set(p.parts))
        return "\n".join(path.read_text(errors="replace") for path in logs)

    def _execute(self, command: list[str]) -> str:
        """Run ``command`` in the container and return its standard output."""
        output = docker.execute(self.container, command)
        if not isinstance(output, str):
            raise AirflowRunError(f"no text output from {command[0]}")
        return output

    def _airflow(self, *args: str) -> str:
        return self._execute(["airflow", *args])

    def _airflow_json(self, *args: str) -> list[dict[str, object]]:
        """The JSON array an Airflow CLI command prints.

        Lines before it (warnings, ``[timestamp]`` log lines) are skipped.
        """
        output = self._airflow(*args, "-o", "json")
        decoder = json.JSONDecoder()
        index = output.find("[")
        while index >= 0:
            try:
                value, _ = decoder.raw_decode(output, index)
            except ValueError:
                index = output.find("[", index + 1)
                continue
            if isinstance(value, list) and all(isinstance(row, dict) for row in value):
                return value
            index = output.find("[", index + 1)
        # Some commands print a "No data found" line instead of an empty array.
        return []
