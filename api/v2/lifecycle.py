from __future__ import annotations

from copy import deepcopy
from typing import Any

from api.utils.identifiers import normalize_cnpj_v2


class LifecycleConflictError(ValueError):
    """Raised when legal lifecycle evidence conflicts with current regulation."""


def apply_legal_lifecycle(
    entities: list[dict[str, Any]],
    lifecycle_records: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Attach Receita cadastral lifecycle by exact current/known CNPJ.

    Receita legal status and SUSEP regulatory status are independent dimensions.
    A closed CNPJ therefore does not overwrite ``regulatory_status``. Instead it
    is attached under ``legal_lifecycle`` and can later explain historical or
    incorporated records to the user.
    """
    output = [deepcopy(item) for item in entities]
    by_cnpj: dict[str, list[int]] = {}
    for index, entity in enumerate(output):
        cnpj = normalize_cnpj_v2(entity.get("cnpj"))
        if cnpj:
            by_cnpj.setdefault(cnpj, []).append(index)

    unresolved: list[dict[str, Any]] = []
    for raw in lifecycle_records:
        record = deepcopy(raw)
        cnpj = normalize_cnpj_v2(record.get("cnpj"))
        matches = by_cnpj.get(cnpj or "", [])
        if len(matches) != 1:
            unresolved.append(
                {
                    **record,
                    "resolution": "ambiguous_cnpj" if len(matches) > 1 else "unmatched_cnpj",
                    "matched_entity_ids": [output[index]["entity_id"] for index in matches],
                }
            )
            continue

        entity = output[matches[0]]
        legal_status = record.get("cadastral_status")
        regulatory_status = entity.get("regulatory_status") or "unknown"
        if legal_status == "closed" and regulatory_status == "active_licensed":
            raise LifecycleConflictError(
                f"CNPJ {cnpj} is closed in Receita evidence but active_licensed in SUSEP"
            )

        evidence = deepcopy(entity.get("evidence") or {})
        evidence["receita_cnpj"] = {
            "source_authority": record.get("source_authority"),
            "source_document": record.get("source_document"),
            "source_mode": record.get("source_mode"),
            "observed_at": record.get("observed_at"),
        }
        entity["evidence"] = evidence
        entity["legal_lifecycle"] = {
            "cadastral_status": legal_status,
            "status_date": record.get("status_date"),
            "status_reason": record.get("status_reason"),
            "raw_status": record.get("raw_status"),
            "raw_reason": record.get("raw_reason"),
        }
        output[matches[0]] = entity

    return sorted(output, key=lambda item: item["entity_id"]), unresolved


def lifecycle_summary(
    entities: list[dict[str, Any]],
    source_records: list[dict[str, Any]],
    unresolved: list[dict[str, Any]],
) -> dict[str, Any]:
    attached = [item for item in entities if item.get("legal_lifecycle")]
    closed = [
        item
        for item in attached
        if (item.get("legal_lifecycle") or {}).get("cadastral_status") == "closed"
    ]
    incorporation = [
        item
        for item in closed
        if (item.get("legal_lifecycle") or {}).get("status_reason") == "incorporation"
    ]
    return {
        "receita_lifecycle_source_count": len(source_records),
        "receita_lifecycle_attached_count": len(attached),
        "receita_lifecycle_unresolved_count": len(unresolved),
        "closed_legal_entities_count": len(closed),
        "closed_by_incorporation_count": len(incorporation),
    }
