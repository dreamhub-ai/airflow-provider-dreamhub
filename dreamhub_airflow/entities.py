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
        path = entity.strip("/")
        if not path:
            raise ValueError("entity must not be empty")
        known = KNOWN_ENTITIES.get(path)
        if known is not None:
            return known
        return cls(name=entity, path=path, items_key=path.rsplit("/", 1)[-1])


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
