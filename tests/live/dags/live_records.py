"""The test companies a live run writes, derived from the run's marker.

Imported by the live DAGs inside Airflow (this folder is the DAG folder there) and by the
harness on the host, so both sides agree on every record without passing ids around.

What each bulk step leaves behind, so the final read shows every step's effect:

- company 0: created, updated, then upserted (matched by website);
- company 1: created, then updated with a null phone and the default ``override=False``, so the
  stored phone must survive;
- company 2: created, updated, then written with ``override=True`` and a null phone, which clears it;
- company 3: created by the upsert, because no company holds its website.
"""

from __future__ import annotations

from collections.abc import Callable

# Where the DAGs write their files inside Airflow; the harness reads the results from there.
OUT_DIR = "/tmp/dreamhub-live"
# Companies the bulk create writes; the upsert adds one more.
COMPANY_COUNT = 3
STATUS_ACTIVE = 1
MATCH_FIELD = "website"
# A nullable text field: kept by a null under override=False, cleared by one under override=True.
CLEARED_FIELD = "phone"
PHONE = "+1 555 0100"

Records = list[dict[str, object]]


class CompanyFixture:
    """The companies of one live run. Every value carries the run marker, so a run's records are findable."""

    def __init__(self, run_marker: str) -> None:
        if not run_marker:
            raise ValueError("run_marker is empty: trigger the DAG with conf {'run_marker': ...}")
        self.run_marker = run_marker

    def website(self, index: int) -> str:
        return f"https://{self.run_marker}-{index}.example.com"

    def all_websites(self) -> list[str]:
        """Every website a run may have written, the upserted one included."""
        return [self.website(i) for i in range(COMPANY_COUNT + 1)]

    def _company(self, index: int, label: str) -> dict[str, object]:
        # Bulk update and upsert validate each item as a full record today, so every write
        # sends the fields creating a company requires (see README, Limitations).
        return {
            "name": f"{self.run_marker} company {index} {label}",
            "status": STATUS_ACTIVE,
            MATCH_FIELD: self.website(index),
            CLEARED_FIELD: PHONE,
            "allow_enrich": False,
        }

    def to_create(self) -> Records:
        return [self._company(i, "created") for i in range(COMPANY_COUNT)]

    def to_update(self) -> Records:
        """Companies 0 to 2 renamed; company 1 also sends a null phone, which override=False must ignore."""
        records = [self._company(i, "updated") for i in range(COMPANY_COUNT)]
        records[1][CLEARED_FIELD] = None
        return records

    def to_upsert(self) -> Records:
        """Company 0, matched by website, and company 3, which no company matches, so it is created."""
        return [self._company(0, "upserted"), self._company(COMPANY_COUNT, "upserted")]

    def to_overwrite(self) -> Records:
        """Company 2 with a null phone, written with override so the stored phone is cleared."""
        record = self._company(2, "overwritten")
        record[CLEARED_FIELD] = None
        return [record]

    def to_delete(self) -> Records:
        return [{MATCH_FIELD: website} for website in self.all_websites()]

    def steps(self) -> dict[str, Callable[[], Records]]:
        """The bulk DAG's steps, in order, and the records each one writes."""
        return {
            "create": self.to_create,
            "update": self.to_update,
            "upsert": self.to_upsert,
            "overwrite": self.to_overwrite,
        }

    def expected(self) -> dict[str, tuple[str, str | None]]:
        """Website → the (name, phone) each company holds after the bulk DAG."""
        return {
            self.website(0): (f"{self.run_marker} company 0 upserted", PHONE),
            self.website(1): (f"{self.run_marker} company 1 updated", PHONE),
            self.website(2): (f"{self.run_marker} company 2 overwritten", None),
            self.website(COMPANY_COUNT): (f"{self.run_marker} company {COMPANY_COUNT} upserted", PHONE),
        }

    def invalid(self) -> Records:
        """A company without the required status: the API refuses it and writes nothing."""
        return [{"name": f"{self.run_marker} invalid", MATCH_FIELD: self.invalid_website()}]

    def invalid_website(self) -> str:
        return f"https://{self.run_marker}-invalid.example.com"
