"""LiveRun's run tracking and teardown order, with a fake Airflow and a fake API: no Docker, no network.

Needs the 'live' extra (LiveRun imports python-on-whales), so it is skipped where the extra is not installed.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path

import pytest

pytest.importorskip("python_on_whales", reason="LiveRun needs the 'live' extra")

from python_on_whales.exceptions import DockerException  # noqa: E402

from tests.live.harness.airflow_container import AIRFLOW_3, AirflowRunError  # noqa: E402
from tests.live.harness.config import ApiToken, LiveTarget  # noqa: E402
from tests.live.harness.ledger import RecordLedger  # noqa: E402
from tests.live.harness.live_run import OUT_DIR, LiveRun  # noqa: E402

TOKEN = "pat_test_0123456789abcdefghijklmnopqrstuv"


class FakeAirflow:
    """Records what teardown asks of Airflow, in order."""

    def __init__(self, files: dict[str, str] | None = None, log_text: str = "task ran\n") -> None:
        self.started = True
        self.calls: list[str] = []
        self.files = files or {}
        self.log_text = log_text
        self.halt_error: Exception | None = None
        self.copy_error: Exception | None = None
        self.wait_error: Exception | None = None

    def trigger(self, dag_id: str, run_id: str, conf: dict[str, object]) -> None:
        self.calls.append(f"trigger {dag_id}")

    def wait_for_run(self, dag_id: str, run_id: str) -> str:
        if self.wait_error is not None:
            raise self.wait_error
        return "success"

    def file_exists(self, path: str) -> bool:
        self.calls.append(f"exists {Path(path).name}")
        return path in self.files

    def read_text(self, path: str) -> str:
        return self.files[path]

    def copy_logs(self, destination: Path) -> Path:
        self.calls.append("copy_logs")
        if self.copy_error is not None:
            raise self.copy_error
        destination.mkdir(parents=True, exist_ok=True)
        (destination / "task.log").write_text(self.log_text)
        return destination

    def halt(self) -> None:
        self.calls.append("halt")
        if self.halt_error is not None:
            raise self.halt_error

    def remove(self) -> None:
        self.calls.append("remove")


class FakeApi:
    """No records exist; each delete is counted."""

    def __init__(self, airflow: FakeAirflow) -> None:
        self.airflow = airflow

    def ids_by_website(self, websites: list[str]) -> list[str]:
        # Cleanup reads by website before and after deleting; record the step once.
        if "clean_up" not in self.airflow.calls:
            self.airflow.calls.append("clean_up")
        return []

    def existing_ids(self, entity: str, ids: list[str]) -> list[str]:
        return []

    def delete_companies(self, ids: list[str], ledger: RecordLedger) -> None:
        ledger.deleted("companies", ids)


@pytest.fixture
def make_run(tmp_path: Path) -> Iterator[object]:
    runs: list[LiveRun] = []

    def make(airflow: FakeAirflow) -> LiveRun:
        run = LiveRun(
            AIRFLOW_3,
            target=LiveTarget(base_url="https://dev.example.test/api/v1"),
            token=ApiToken(TOKEN),
            api=FakeApi(airflow),  # type: ignore[arg-type]
            wheel=tmp_path / "provider.whl",
            labels={},
            keep=False,
            artifacts=tmp_path / "artifacts",
        )
        run.airflow = airflow  # type: ignore[assignment]
        runs.append(run)
        return run

    yield make
    for run in runs:
        run.close()


def summary_line(ids: list[str]) -> str:
    return json.dumps({"operation": "create", "succeeded": len(ids), "failed": 0, "succeeded_ids": ids}) + "\n"


class TestUnfinishedRuns:
    def test_a_finished_run_is_not_unfinished(self, make_run) -> None:
        run = make_run(FakeAirflow())

        run.run_dag("dreamhub_live_bulk")

        assert not run.has_unfinished_runs

    def test_a_run_that_timed_out_stays_unfinished(self, make_run) -> None:
        airflow = FakeAirflow()
        airflow.wait_error = AirflowRunError("did not finish")
        run = make_run(airflow)

        with pytest.raises(AirflowRunError):
            run.run_dag("dreamhub_live_bulk")

        assert run.has_unfinished_runs


class TestTearDown:
    def test_reads_results_before_stopping_airflow_and_scans_logs_after(self, make_run) -> None:
        airflow = FakeAirflow(files={f"{OUT_DIR}/bulk_create.ndjson": summary_line(["c1", "c2"])})
        run = make_run(airflow)

        report = run.tear_down()

        order = [call for call in airflow.calls if call in ("copy_logs", "halt", "clean_up")]
        assert order == ["halt", "copy_logs", "clean_up"]
        assert airflow.calls.index("exists bulk_create.ndjson") < airflow.calls.index("halt")
        assert report["counts"] == {"companies": {"created": 2, "deleted": 2}}
        assert "problems" not in report

    def test_a_token_in_the_logs_is_a_problem(self, make_run) -> None:
        run = make_run(FakeAirflow(log_text=f"Authorization: Bearer {TOKEN}\n"))

        report = run.tear_down()

        assert report["problems"] == ["token found in 1 log lines"]

    def test_logs_that_cannot_be_copied_are_a_problem_and_cleanup_still_runs(self, make_run) -> None:
        airflow = FakeAirflow()
        airflow.copy_error = DockerException(["docker", "cp"], 1)
        run = make_run(airflow)

        report = run.tear_down()

        assert report["problems"] == ["logs not scanned: DockerException"]
        assert airflow.calls[-3:] == ["halt", "copy_logs", "clean_up"]

    def test_airflow_that_cannot_be_stopped_is_a_problem_and_logs_and_cleanup_still_run(self, make_run) -> None:
        airflow = FakeAirflow()
        airflow.halt_error = DockerException(["docker", "stop"], 1)
        run = make_run(airflow)

        report = run.tear_down()

        assert report["problems"] == ["Airflow not stopped, a run may still write: DockerException"]
        assert airflow.calls[-3:] == ["halt", "copy_logs", "clean_up"]

    def test_a_results_line_that_is_not_an_object_is_a_problem(self, make_run) -> None:
        run = make_run(FakeAirflow(files={f"{OUT_DIR}/bulk_create.ndjson": "[1]\n"}))

        report = run.tear_down()

        assert report["problems"] == ["results not read: ValueError"]
