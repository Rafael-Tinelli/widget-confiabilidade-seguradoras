from __future__ import annotations

from collections import Counter
from copy import deepcopy
from typing import Any

from api.utils.identifiers import normalize_cnpj_v2


class SandboxIdentityConflictError(ValueError):
    """Raised when a Sandbox-only identity cannot be materialized safely."""


def _empty_activities() -> dict[str, bool]:
    return {
        "insurance": False,
        "pension": False,
        "capitalization": False,
        "reinsurance": False,
    }


def _sandbox_evidence(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "source": record.get("source"),
        "legal_name": record.get("legal_name"),
        "cnpj": normalize_cnpj_v2(record.get("cnpj")),
        "edition": record.get("edition"),
        "regulatory_status": record.get("regulatory_status"),
        "raw_status": record.get("raw_status"),
        "authorization_start": record.get("authorization_start"),
        "authorization_end": record.get("authorization_end"),
        "authorization_end_raw": record.get("authorization_end_raw"),
        "modalities": record.get("modalities"),
    }


def _sandbox_only_entity(record: dict[str, Any]) -> dict[str, Any]:
    cnpj = normalize_cnpj_v2(record.get("cnpj"))
    if not cnpj:
        raise SandboxIdentityConflictError("Sandbox-only identity requires a valid CNPJ")

    status = record.get("regulatory_status")
    if status not in {"temporary_authorized", "sandbox_authorization_cancelled"}:
        raise SandboxIdentityConflictError(
            f"Unsupported Sandbox-only regulatory status for CNPJ {cnpj}: {status}"
        )

    return {
        "entity_id": f"cnpj:{cnpj}",
        "fip_code": None,
        "cnpj": cnpj,
        "legal_entity_id": f"cnpj:{cnpj}",
        "legal_name": str(record.get("legal_name") or "").strip(),
        "entity_type": "insurer",
        "regulatory_regime": "sandbox",
        "regulatory_status": status,
        "activities": _empty_activities(),
        "evidence": {
            "ses_present": False,
            "identity_origin": "susep_sandbox",
            "identity_key_reason": "official_sandbox_cnpj_without_published_fip",
            "sandbox": _sandbox_evidence(record),
        },
    }


def materialize_unmatched_sandbox_identities(
    entities: list[dict[str, Any]],
    unresolved_sandbox: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Create CNPJ-keyed identities only for unambiguous Sandbox-only records.

    Current SUSEP Sandbox participant data publishes official CNPJ but no FIP.
    When the CNPJ does not map to an existing FIP identity, the official CNPJ is
    sufficient to retain that regulated participant as a separate Sandbox
    identity. If the same CNPJ later appears in an ordinary licensed FIP record,
    the normal classification pipeline can attach Sandbox history to that FIP.

    Ambiguous CNPJ matches remain unresolved. No name matching is performed.
    """
    output = [deepcopy(item) for item in entities]
    remaining: list[dict[str, Any]] = []

    existing_ids = {item.get("entity_id") for item in output}
    existing_cnpjs = Counter(
        normalize_cnpj_v2(item.get("cnpj"))
        for item in output
        if normalize_cnpj_v2(item.get("cnpj"))
    )

    for raw in unresolved_sandbox:
        record = deepcopy(raw)
        resolution = record.get("resolution")
        cnpj = normalize_cnpj_v2(record.get("cnpj"))

        if resolution != "unmatched_cnpj":
            remaining.append(record)
            continue
        if not cnpj:
            remaining.append({**record, "resolution": "invalid_cnpj"})
            continue
        if existing_cnpjs.get(cnpj, 0):
            remaining.append({**record, "resolution": "cnpj_became_non_unique"})
            continue

        entity = _sandbox_only_entity(record)
        if entity["entity_id"] in existing_ids:
            raise SandboxIdentityConflictError(
                f"Duplicate Sandbox-only entity_id {entity['entity_id']}"
            )
        output.append(entity)
        existing_ids.add(entity["entity_id"])
        existing_cnpjs[cnpj] += 1

    return sorted(output, key=lambda item: item["entity_id"]), remaining
