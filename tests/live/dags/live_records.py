"""The test companies a live run writes, derived from the run's marker.

Imported by the live DAGs inside Airflow (this folder is the DAG folder there) and by the
harness on the host, so both sides agree on every record without passing ids around.
"""

from __future__ import annotations

# Companies created by a run: COUNT by the bulk create, one more by the upsert.
COUNT = 3
STATUS_ACTIVE = 1
MATCH_FIELD = "website"
# A nullable text field the full-overwrite step clears.
CLEARED_FIELD = "phone"
PHONE = "+1 555 0100"


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
        return [self.website(i) for i in range(COUNT + 1)]

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

    def to_create(self) -> list[dict[str, object]]:
        return [self._company(i, "created") for i in range(COUNT)]

    def to_update(self) -> list[dict[str, object]]:
        return [self._company(i, "updated") for i in range(COUNT)]

    def to_upsert(self) -> list[dict[str, object]]:
        """Two existing companies matched by website, and one new one."""
        return [self._company(0, "upserted"), self._company(1, "upserted"), self._company(COUNT, "upserted")]

    def to_overwrite(self) -> list[dict[str, object]]:
        """Company 2 with a null phone, written with override so the stored phone is cleared."""
        record = self._company(2, "overwritten")
        record[CLEARED_FIELD] = None
        return [record]

    def to_delete(self) -> list[dict[str, object]]:
        return [{MATCH_FIELD: website} for website in self.all_websites()]

    def expected_names(self) -> dict[str, str]:
        """Website → the name each company holds after the bulk DAG."""
        return {
            self.website(0): f"{self.run_marker} company 0 upserted",
            self.website(1): f"{self.run_marker} company 1 upserted",
            self.website(2): f"{self.run_marker} company 2 overwritten",
            self.website(COUNT): f"{self.run_marker} company {COUNT} upserted",
        }

    def invalid(self) -> list[dict[str, object]]:
        """A company without the required status: the API refuses it and writes nothing."""
        return [{"name": f"{self.run_marker} invalid", MATCH_FIELD: self.invalid_website()}]

    def invalid_website(self) -> str:
        return f"https://{self.run_marker}-invalid.example.com"
