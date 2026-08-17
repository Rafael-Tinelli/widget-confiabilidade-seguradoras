from __future__ import annotations

from collections import Counter
from copy import deepcopy
from typing import Any

from api.utils.identifiers import normalize_cnpj
from api.v2.identity import canonical_fip_code


class ClassificationConflictError(ValueError):
    """Raised when authoritative classification records conflict by FIP."""


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
            or previous.get("legal_name") != item.get("legal_name")
        ):
            raise ClassificationConflictError(f"Conflicting licensed records for FIP {fip}")
        by_fip[fip] = item
    return by_fip


def _assert_unique_classified_entities(entities: list[dict[str, Any]]) -> None:
    for field in ("entity_id", "fip_code"):
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
    """Enrich FIP-stable identities with the current SUSEP licensed universe.

    FIP is the authoritative join key. The licensed service supplies current
    regulatory type and current regulated-entity identity. If its CNPJ differs
    from SES/LISTAEMPRESAS, the licensed value becomes the primary current CNPJ
    while the SES value is retained as evidence. This accommodates legitimate
    structures such as foreign reinsurers and Brazilian representative offices
    without silently discarding the source disagreement.

    Unmatched SES records remain unknown until special-regime, Sandbox or other
    authoritative sources classify them.
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

        ses_cnpj = normalize_cnpj(entity.get("cnpj"))
        licensed_cnpj = normalize_cnpj(licensed.get("cnpj"))
        ses_name = str(entity.get("legal_name") or "").strip()
        licensed_name = str(licensed.get("legal_name") or "").strip()

        effective_cnpj = licensed_cnpj or ses_cnpj
        entity["entity_id"] = f"fip:{fip}"
        entity["cnpj"] = effective_cnpj
        entity["legal_entity_id"] = f"cnpj:{effective_cnpj}" if effective_cnpj else None
        if licensed_name:
            entity["legal_name"] = licensed_name
        entity["entity_type"] = licensed["entity_type"]
        entity["regulatory_regime"] = "ordinary"
        entity["regulatory_status"] = "active_licensed"

        evidence = deepcopy(entity.get("evidence") or {})
        evidence["licensed_entities"] = {
            "matched": True,
            "source": licensed.get("source"),
            "source_type_code": licensed.get("source_type_code"),
            "entity_type": licensed.get("entity_type"),
            "legal_name": licensed_name,
            "cnpj": licensed_cnpj,
        }

        variances: dict[str, Any] = {}
        if ses_cnpj and licensed_cnpj and ses_cnpj != licensed_cnpj:
            variances["cnpj"] = {
                "ses": ses_cnpj,
                "licensed": licensed_cnpj,
            }
        if ses_name and licensed_name and ses_name != licensed_name:
            variances["legal_name"] = {
                "ses": ses_name,
                "licensed": licensed_name,
            }
        if variances:
            evidence["identity_variances"] = variances

        if licensed_cnpj:
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

    classified = [
        item
        for item in entities
        if item.get("regulatory_status") == "active_licensed"
    ]
    by_type = Counter(item.get("entity_type") or "unknown" for item in classified)

    cnpj_filled = 0
    cnpj_variances = 0
    for item in classified:
        evidence = item.get("evidence") or {}
        ses_identity = evidence.get("ses_identity") or {}
        licensed = evidence.get("licensed_entities") or {}
        if not ses_identity.get("cnpj") and licensed.get("cnpj"):
            cnpj_filled += 1
        if (evidence.get("identity_variances") or {}).get("cnpj"):
            cnpj_variances += 1

    cnpj_counts = Counter(
        item.get("cnpj") for item in entities if item.get("cnpj")
    )
    duplicate_current_cnpjs = sum(1 for count in cnpj_counts.values() if count > 1)

    return {
        "inventory_count": len(entities),
        "classified_active_licensed": len(classified),
        "unclassified": len(entities) - len(classified),
        "by_entity_type": dict(sorted(by_type.items())),
        "cnpj_filled_from_licensed_source": cnpj_filled,
        "cnpj_variances_between_ses_and_licensed": cnpj_variances,
        "duplicate_current_cnpj_values": duplicate_current_cnpjs,
        "licensed_source_count": len(licensed_records),
        "licensed_records_not_in_ses_inventory": len(licensed_fips - inventory_fips),
    }
