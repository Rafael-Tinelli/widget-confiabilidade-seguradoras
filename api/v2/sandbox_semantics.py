from __future__ import annotations

from copy import deepcopy
from typing import Any

SANDBOX_STATUSES = {"temporary_authorized", "sandbox_authorization_cancelled"}


def normalize_sandbox_entity_semantics(
    entities: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Ensure experimental Sandbox participants are not typed as insurers.

    ``insurer`` is reserved for entities licensed under the ordinary SUSEP
    regime. Sandbox authorization is experimental, scoped and temporary; the
    public product must never let these records enter the insurer ranking by
    sharing the same entity type.
    """
    output: list[dict[str, Any]] = []
    for raw in entities:
        entity = deepcopy(raw)
        if (
            entity.get("regulatory_regime") == "sandbox"
            or entity.get("regulatory_status") in SANDBOX_STATUSES
        ):
            entity["entity_type"] = "sandbox_participant"
        output.append(entity)
    return sorted(output, key=lambda item: item["entity_id"])
