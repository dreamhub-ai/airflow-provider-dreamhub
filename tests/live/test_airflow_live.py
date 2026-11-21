"""The provider in a real Airflow against a live Dreamhub environment.

Opt-in: ``pytest -m airflow_live`` with Docker running and ``DREAMHUB_LIVE_URL`` plus a token
configured (see CONTRIBUTING.md). Writes test companies, all marked with the run's marker, and
deletes and counts every one of them afterwards.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

pytest.importorskip("python_on_whales", reason="the live harness needs the 'live' extra")

from python_on_whales.exceptions import DockerException  # noqa: E402

from tests.live.conftest import SUMMARY_KEY  # noqa: E402
from tests.live.dags.live_records import CLEARED_FIELD, OUT_DIR  # noqa: E402
from tests.live.harness.airflow_container import IMAGES, AirflowImage, AirflowRunError  # noqa: E402
from tests.live.harness.config import ApiToken, HarnessConfigError, LiveTarget  # noqa: E402
from tests.live.harness.live_api import LiveApi  # noqa: E402
from tests.live.harness.live_run import (  # noqa: E402
    BULK_RESULTS,
    PROXY_REFUSALS,
    PROXY_RETRY_AFTER_SECONDS,
    LiveRun,
)

pytestmark = pytest.mark.airflow_live

ROOT = Path(__file__).resolve().parents[2]
WANTED_MAJORS = {part.strip() for part in os.environ.get("DREAMHUB_LIVE_AIRFLOW", "2,3").split(",") if part.strip()}
KNOWN_MAJORS = {str(image.major) for image in IMAGES}
if not WANTED_MAJORS or not WANTED_MAJORS <= KNOWN_MAJORS:
    # An empty selection would skip every live test and still report success.
    raise pytest.UsageError(f"DREAMHUB_LIVE_AIRFLOW must name Airflow majors among {sorted(KNOWN_MAJORS)}")
SELECTED_IMAGES = [image for image in IMAGES if str(image.major) in WANTED_MAJORS]


@pytest.fixture(scope="session")
def live_config() -> tuple[LiveTarget, ApiToken]:
    # These tests run only when selected with `-m airflow_live`; a missing setting then is an error,
    # not a skip that would read as a pass.
    try:
        return LiveTarget.from_environment(), ApiToken.from_environment()
    except HarnessConfigError as error:
        pytest.fail(f"live harness not configured: {error}")


@pytest.fixture(scope="session")
def provider_wheel(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """The provider as it would be published, built from this checkout."""
    out = tmp_path_factory.mktemp("wheel")
    command = [sys.executable, "-m", "pip", "wheel", "--quiet", "--no-deps", "-w", str(out), str(ROOT)]
    # The build needs no Dreamhub setting, so it inherits none of those that can hold the token.
    env = {
        name: value
        for name, value in os.environ.items()
        if not name.startswith(("AIRFLOW_CONN_", "DREAMHUB_LIVE_", "DREAMHUB_E2E_"))
    }
    subprocess.run(command, check=True, env=env)
    return next(out.glob("airflow_provider_dreamhub-*.whl"))


@pytest.fixture(scope="session")
def live_api(live_config: tuple[LiveTarget, ApiToken]) -> Iterator[LiveApi]:
    target, token = live_config
    with LiveApi.connect(target, token) as api:
        yield api


@pytest.fixture(scope="module", params=SELECTED_IMAGES, ids=lambda image: f"airflow-{image.label}")
def live_run(
    request: pytest.FixtureRequest,
    live_config: tuple[LiveTarget, ApiToken],
    live_api: LiveApi,
    provider_wheel: Path,
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[LiveRun]:
    """One Airflow with the live DAGs. On teardown every record the run wrote is deleted and counted."""
    image: AirflowImage = request.param
    target, token = live_config
    labels_setting = os.environ.get("DREAMHUB_LIVE_LABELS", "")
    labels = dict(part.split("=", 1) for part in labels_setting.split(",") if "=" in part)
    keep = os.environ.get("DREAMHUB_LIVE_KEEP") == "1"
    run = LiveRun(
        image,
        target=target,
        token=token,
        api=live_api,
        wheel=provider_wheel,
        labels=labels,
        keep=keep,
        artifacts=tmp_path_factory.mktemp(f"live-{image.label}"),
    )
    summary = request.config.stash[SUMMARY_KEY]
    started = f"{image.label}: run marker {run.ledger.run_marker}; logs under {run.artifacts}"
    summary.append(started)
    # Shown now, before any record is written, so a run killed before the summary can still be found.
    reporter = request.config.pluginmanager.get_plugin("terminalreporter")
    if reporter is not None:
        reporter.write_line(f"Dreamhub live run started: {started}")
    report: dict[str, object] = {}
    try:
        run.start()
        yield run
    finally:
        try:
            # Reads what the tasks reported, stops Airflow so nothing in flight writes, then cleans up.
            report = run.tear_down()
            summary.append(f"{image.label}: cleanup {json.dumps(report)}")
            # Only an Airflow that came up is worth restarting; after a failed start the error above stands.
            if keep and run.airflow.ready and run.has_unfinished_runs:
                # Restarting would let the scheduler finish that run's writes after the cleanup.
                summary.append(f"{image.label}: not restarted, a run never finished; {run.airflow.removal_command()}")
            elif keep and run.airflow.ready:
                try:
                    run.airflow.resume()
                    summary.append(run.airflow.ui_instructions())
                except (AirflowRunError, DockerException) as error:
                    reason = type(error).__name__
                    summary.append(
                        f"{image.label}: the UI could not be restarted ({reason}); {run.airflow.removal_command()}"
                    )
            elif keep and run.airflow.started:
                summary.append(f"{image.label}: Airflow never came up; {run.airflow.removal_command()}")
        finally:
            run.close()
    if report.get("leftovers") or report.get("still_readable"):
        pytest.fail(f"the live run left records behind: {json.dumps(report)}")
    if report.get("problems"):
        # Ids not read or Airflow not stopped: a record may exist that cleanup did not see.
        pytest.fail(f"the live run could not confirm its cleanup: {json.dumps(report)}")


def test_provider_is_installed_and_registered(live_run: LiveRun) -> None:
    assert live_run.airflow.provider_registered()


def test_extract_writes_every_record(live_run: LiveRun) -> None:
    run_id, state = live_run.run_dag("dreamhub_live_extract")

    assert state == "success", live_run.airflow.task_states("dreamhub_live_extract", run_id)
    for entity in ("companies", "revenue_items"):
        lines = live_run.airflow.count_lines(f"{OUT_DIR}/{entity}.ndjson")
        # Read right after the extract; the extract itself fails if the total moved while it read.
        assert lines == live_run.api.total(entity), entity
        assert lines > 0, f"{entity}: an empty extract proves nothing; run against an account with data"


def test_rate_limit_is_waited_out(live_run: LiveRun) -> None:
    live_run.start_rate_limit_proxy()

    run_id, state = live_run.run_dag("dreamhub_live_rate_limit")

    assert state == "success", live_run.airflow.task_states("dreamhub_live_rate_limit", run_id)
    stats = live_run.proxy_stats()
    assert stats["refused"] == PROXY_REFUSALS
    assert stats["forwarded"] >= 1
    lines = live_run.airflow.count_lines(f"{OUT_DIR}/rate_limited_revenue_items.ndjson")
    assert lines == live_run.api.total("revenue_items")
    assert lines > 0, "an empty extract proves nothing; run against an account with revenue items"
    logs = live_run.collect_logs()
    task_log = live_run.airflow.task_log(logs, "dreamhub_live_rate_limit", "extract_through_rate_limit")
    assert task_log.count(f"rate limited; retrying in {PROXY_RETRY_AFTER_SECONDS:.1f}s") == PROXY_REFUSALS


def test_failures_are_reported_and_no_record_is_written(live_run: LiveRun) -> None:
    run_id, state = live_run.run_dag("dreamhub_live_failure")

    assert state == "failed"
    states = live_run.airflow.task_states("dreamhub_live_failure", run_id)
    assert states["create_invalid_record"] == "failed"
    assert states["extract_with_refused_token"] == "failed"
    results = live_run.results("invalid_results.ndjson")
    assert results is not None
    assert live_run.record_written(["invalid_results.ndjson"]) == set()
    assert results["failed"] == 1
    failures = results["failures"]
    assert isinstance(failures, list) and "status" in failures[0]["error"]
    assert live_run.api.ids_by_website([live_run.companies.invalid_website()]) == []
    assert not live_run.airflow.file_exists(f"{OUT_DIR}/never_written.ndjson")
    logs = live_run.collect_logs()
    refused = live_run.airflow.task_log(logs, "dreamhub_live_failure", "extract_with_refused_token")
    assert "HTTP 401" in refused


def test_bulk_create_update_upsert_and_overwrite(live_run: LiveRun) -> None:
    run_id, state = live_run.run_dag("dreamhub_live_bulk")

    # Count what the tasks report as written before anything can fail, so cleanup finds it all.
    written = live_run.record_written(BULK_RESULTS)
    found = live_run.read_back_companies()
    assert state == "success", live_run.airflow.task_states("dreamhub_live_bulk", run_id)
    expected = live_run.companies.expected()
    assert {website: len(rows) for website, rows in found.items()} == dict.fromkeys(expected, 1)
    # What the operators reported writing is exactly what reads back: no write went unseen.
    assert written == {str(rows[0]["id"]) for rows in found.values()}
    # The read by id that cleanup and the delete test rely on finds live records.
    assert set(live_run.api.existing_ids("companies", sorted(written))) == written
    actual = {website: (rows[0]["name"], rows[0].get(CLEARED_FIELD)) for website, rows in found.items()}
    assert actual == expected


def test_delete_by_match_field(live_run: LiveRun) -> None:
    created = set(live_run.api.ids_by_website(live_run.companies.all_websites()))
    assert created, "the bulk test must run first and leave the run's companies to delete"

    run_id, state = live_run.run_dag("dreamhub_live_delete")

    deleted = live_run.record_deleted("delete.ndjson")
    assert state == "success", live_run.airflow.task_states("dreamhub_live_delete", run_id)
    assert deleted == created
    assert live_run.api.ids_by_website(live_run.companies.all_websites()) == []
    assert live_run.api.existing_ids("companies", sorted(created)) == []


def test_no_token_in_any_log(live_run: LiveRun) -> None:
    live_run.collect_logs()

    assert live_run.token_findings() == []
