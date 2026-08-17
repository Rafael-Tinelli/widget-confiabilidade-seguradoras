from __future__ import annotations

from collections import Counter
from copy import deepcopy
from typing import Any

from api.utils.identifiers import normalize_cnpj
from api.v2.identity import canonical_fip_code


class ClassificationConflictError(ValueError):
    """Raised when official classification conflicts with canonical identity."""


def _licensed_index(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    by_fip: dict[str, dict[str, Any]] = {}
    for raw in records:
        fip = canonical_fip_code(raw.get("fip_code"))
        if not fip:
            raise ClassificationConflictError("Licensed record without valid FIP code")

        item = dict(raw)
        item["fip_code"] = fip
        item["cnpj"] = normalize_cnpj(item.get("cnpj"))

        previous = by_fip.get(fip)
        if previous and (
            previous.get("entity_type") != item.get("entity_type")
            or previous.get("cnpj") != item.get("cnpj")
        ):
            raise ClassificationConflictError(f"Conflicting licensed records for FIP {fip}")
        by_fip[fip] = item
    return by_fip


def _assert_unique_classified_entities(entities: list[dict[str, Any]]) -> None:
    for field in ("entity_id", "fip_code", "cnpj"):
        values = [item.get(field) for item in entities if item.get(field)]
        duplicates = sorted(value for value, count in Counter(values).items() if count > 1)
        if duplicates:
            raise ClassificationConflictError(
                f"Duplicate {field} after official classification: {duplicates[:5]}"
            )


def apply_licensed_classification(
    entities: list[dict[str, Any]],
    licensed_records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Enrich canonical SES identities with official current licensing data.

    Rules:
    - FIP is the join key.
    - A CNPJ already present in SES must agree with the official licensed source.
    - When SES has no CNPJ and SUSEP licensing does, the official CNPJ becomes
      part of the canonical identity and entity_id is promoted to ``cnpj:...``.
    - Presence in the licensed-entities service means the record is in the
      current licensed universe.  The service itself excludes special regimes,
      Sandbox authorization and entities that ended operations/incorporated.
    - Unmatched SES records remain ``unknown`` until another official source
      classifies them.
    """
    by_fip = _licensed_index(licensed_records)
    output: list[dict[str, Any]] = []

    for raw_entity in entities:
        entity = deepcopy(raw_entity)
        fip = canonical_fip_code(entity.get("fip_code"))
        licensed = by_fip.get(fip)
        if not licensed:
            output.append(entity)
            continue

        current_cnpj = normalize_cnpj(entity.get("cnpj"))
        licensed_cnpj = normalize_cnpj(licensed.get("cnpj"))
        if current_cnpj and licensed_cnpj and current_cnpj != licensed_cnpj:
            raise ClassificationConflictError(
                f"CNPJ conflict for FIP {fip}: SES={current_cnpj} licensed={licensed_cnpj}"
            )

        effective_cnpj = current_cnpj or licensed_cnpj
        entity["cnpj"] = effective_cnpj
        entity["entity_id"] = f"cnpj:{effective_cnpj}" if effective_cnpj else f"fip:{fip}"
        entity["entity_type"] = licensed["entity_type"]
        entity["regulatory_regime"] = "ordinary"
        entity["regulatory_status"] = "active_licensed"

        evidence = deepcopy(entity.get("evidence") or {})
        evidence["licensed_entities"] = {
            "matched": True,
            "source": licensed.get("source"),
            "source_type_code": licensed.get("source_type_code"),
            "entity_type": licensed.get("entity_type"),
            "cnpj_present": licensed_cnpj is not None,
        }
        if not current_cnpj and licensed_cnpj:
            evidence["identity_cnpj_source"] = "susep_licensed_entities"
        entity["evidence"] = evidence
        output.append(entity)

    _assert_unique_classified_entities(output)
    return sorted(output, key=lambda item: item["entity_id"])


def classification_summary(
    entities: list[dict[str, Any]],
    licensed_records: list[dict[str, Any]],
) -> dict[str, Any]:
    licensed_fips = {canonical_fip_code(item.get("fip_code")) for item in licensed_records}
    inventory_fips = {canonical_fip_code(item.get("fip_code")) for item in entities}

    classified = [item for item in entities if item.get("regulatory_status") == "active_licensed"]
    by_type = Counter(item.get("entity_type") or "unknown" for item in classified)
    cnpj_from_licensed = sum(
        (item.get("evidence") or {}).get("identity_cnpj_source") == "susep_licensed_entities"
        for item in classified
    )

    return {
        "inventory_count": len(entities),
        "classified_active_licensed": len(classified),
        "unclassified": len(entities) - len(classified),
        "by_entity_type": dict(sorted(by_type.items())),
        "cnpj_filled_from_licensed_source": cnpj_from_licensed,
        "licensed_source_count": len(licensed_records),
        "licensed_records_not_in_ses_inventory": len(licensed_fips - inventory_fips),
    }
