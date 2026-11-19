"""Where each Dreamhub entity lives in the REST API, and the key its rows come back under."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EntityRoute:
    """The API path of one entity type and the response key that holds its rows."""

    name: str
    path: str
    items_key: str

    @classmethod
    def for_entity(cls, entity: str) -> EntityRoute:
        """The route for a known entity, or one derived from a path such as ``partners``."""
        known = KNOWN_ENTITIES.get(entity)
        if known is not None:
            return known
        path = entity.strip("/")
        if not path:
            raise ValueError("entity must not be empty")
        return cls(name=entity, path=path, items_key=path.rsplit("/", 1)[-1])

    @classmethod
    def activities_of(cls, parent_entity: str, parent_id: str) -> EntityRoute:
        """Activities of one parent record (a company, deal, lead or person).

        An account-wide activity read is planned at ``users/activities/filter``; until the API
        offers it, activities are read per parent record.
        """
        parent = cls.for_entity(parent_entity)
        return cls(name="activities", path=f"{parent.path}/{parent_id}/activities", items_key="activities")


KNOWN_ENTITIES: dict[str, EntityRoute] = {
    route.name: route
    for route in (
        EntityRoute("companies", "companies", "companies"),
        EntityRoute("deals", "deals", "deals"),
        EntityRoute("leads", "leads", "leads"),
        EntityRoute("people", "people", "people"),
        EntityRoute("contracts", "contracts", "contracts"),
        EntityRoute("revenue_items", "contracts/revenue_items", "revenue_items"),
        EntityRoute("tasks", "tasks", "tasks"),
        EntityRoute("partners", "partners", "partners"),
        EntityRoute("users", "users", "users"),
    )
}
