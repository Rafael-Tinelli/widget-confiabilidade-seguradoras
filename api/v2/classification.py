from __future__ import annotations

from collections import Counter, defaultdict
from copy import deepcopy
from typing import Any

from api.utils.identifiers import normalize_cnpj
from api.v2.identity import canonical_fip_code


class ClassificationConflictError(ValueError):
    """Raised when authoritative classification records conflict."""


def _empty_activities() -> dict[str, bool]:
    return {
        "insurance": False,
        "pension": False,
        "capitalization": False,
        "reinsurance": False,
    }


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


def _special_index(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    by_fip: dict[str, dict[str, Any]] = {}
    for raw in records:
        fip = canonical_fip_code(raw.get("fip_code"))
        if not fip:
            raise ClassificationConflictError("Special-regime record without valid FIP code")
        item = dict(raw)
        item["fip_code"] = fip
        previous = by_fip.get(fip)
        if previous and previous.get("regulatory_status") != item.get("regulatory_status"):
            raise ClassificationConflictError(f"Conflicting special-regime records for FIP {fip}")
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


def _licensed_evidence(licensed: dict[str, Any]) -> dict[str, Any]:
    return {
        "matched": True,
        "source": licensed.get("source"),
        "source_type_code": licensed.get("source_type_code"),
        "entity_type": licensed.get("entity_type"),
        "legal_name": licensed.get("legal_name"),
        "cnpj": normalize_cnpj(licensed.get("cnpj")),
    }


def _entity_from_licensed(licensed: dict[str, Any]) -> dict[str, Any]:
    fip = canonical_fip_code(licensed.get("fip_code"))
    cnpj = normalize_cnpj(licensed.get("cnpj"))
    legal_name = str(licensed.get("legal_name") or "").strip()
    return {
        "entity_id": f"fip:{fip}",
        "fip_code": fip,
        "cnpj": cnpj,
        "legal_entity_id": f"cnpj:{cnpj}" if cnpj else None,
        "legal_name": legal_name,
        "entity_type": licensed["entity_type"],
        "regulatory_regime": "ordinary",
        "regulatory_status": "active_licensed",
        "activities": _empty_activities(),
        "evidence": {
            "ses_present": False,
            "identity_origin": "susep_licensed_entities",
            "licensed_entities": _licensed_evidence(licensed),
            "identity_cnpj_source": "susep_licensed_entities" if cnpj else None,
        },
    }


def apply_licensed_classification(
    entities: list[dict[str, Any]],
    licensed_records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Apply the current ordinary licensed universe and add missing FIPs."""
    by_fip = _licensed_index(licensed_records)
    output: list[dict[str, Any]] = []
    seen_fips: set[str] = set()

    for raw_entity in entities:
        entity = deepcopy(raw_entity)
        fip = canonical_fip_code(entity.get("fip_code"))
        seen_fips.add(fip)
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
        evidence["licensed_entities"] = _licensed_evidence(licensed)
        variances: dict[str, Any] = {}
        if ses_cnpj and licensed_cnpj and ses_cnpj != licensed_cnpj:
            variances["cnpj"] = {"ses": ses_cnpj, "licensed": licensed_cnpj}
        if ses_name and licensed_name and ses_name != licensed_name:
            variances["legal_name"] = {"ses": ses_name, "licensed": licensed_name}
        if variances:
            evidence["identity_variances"] = variances
        if licensed_cnpj:
            evidence["identity_cnpj_source"] = "susep_licensed_entities"
        entity["evidence"] = evidence
        output.append(entity)

    for fip in sorted(set(by_fip) - seen_fips):
        output.append(_entity_from_licensed(by_fip[fip]))

    _assert_unique_classified_entities(output)
    return sorted(output, key=lambda item: item["entity_id"])


def _entity_from_special(record: dict[str, Any]) -> dict[str, Any]:
    fip = canonical_fip_code(record.get("fip_code"))
    return {
        "entity_id": f"fip:{fip}",
        "fip_code": fip,
        "cnpj": None,
        "legal_entity_id": None,
        "legal_name": str(record.get("legal_name") or "").strip(),
        "entity_type": record.get("entity_type") or "unknown",
        "regulatory_regime": "special",
        "regulatory_status": record.get("regulatory_status") or "unknown",
        "activities": _empty_activities(),
        "evidence": {
            "ses_present": False,
            "identity_origin": "susep_special_regimes",
            "special_regime": {
                "source": record.get("source"),
                "regulatory_status": record.get("regulatory_status"),
                "entity_type": record.get("entity_type") or "unknown",
            },
        },
    }


def apply_special_regime_classification(
    entities: list[dict[str, Any]],
    special_records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Apply current SUSEP special-regime lists with explicit precedence."""
    by_fip = _special_index(special_records)
    output: list[dict[str, Any]] = []
    seen_fips: set[str] = set()

    for raw_entity in entities:
        entity = deepcopy(raw_entity)
        fip = canonical_fip_code(entity.get("fip_code"))
        seen_fips.add(fip)
        special = by_fip.get(fip)
        if not special:
            output.append(entity)
            continue

        if entity.get("regulatory_status") == "active_licensed":
            raise ClassificationConflictError(
                f"FIP {fip} appears simultaneously as ordinary licensed and special regime"
            )

        special_type = special.get("entity_type") or "unknown"
        current_type = entity.get("entity_type") or "unknown"
        if special_type != "unknown" and current_type not in {"unknown", special_type}:
            raise ClassificationConflictError(
                f"Entity-type conflict for special-regime FIP {fip}: "
                f"current={current_type} special={special_type}"
            )
        if special_type != "unknown":
            entity["entity_type"] = special_type

        special_name = str(special.get("legal_name") or "").strip()
        if special_name:
            entity["legal_name"] = special_name
        entity["regulatory_regime"] = "special"
        entity["regulatory_status"] = special["regulatory_status"]

        evidence = deepcopy(entity.get("evidence") or {})
        evidence["special_regime"] = {
            "source": special.get("source"),
            "regulatory_status": special.get("regulatory_status"),
            "entity_type": special_type,
        }
        entity["evidence"] = evidence
        output.append(entity)

    for fip in sorted(set(by_fip) - seen_fips):
        output.append(_entity_from_special(by_fip[fip]))

    _assert_unique_classified_entities(output)
    return sorted(output, key=lambda item: item["entity_id"])


def _sandbox_evidence(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "source": record.get("source"),
        "legal_name": record.get("legal_name"),
        "cnpj": normalize_cnpj(record.get("cnpj")),
        "edition": record.get("edition"),
        "regulatory_status": record.get("regulatory_status"),
        "raw_status": record.get("raw_status"),
        "authorization_start": record.get("authorization_start"),
        "authorization_end": record.get("authorization_end"),
        "authorization_end_raw": record.get("authorization_end_raw"),
        "modalities": record.get("modalities"),
    }


def apply_sandbox_classification(
    entities: list[dict[str, Any]],
    sandbox_records: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Apply Sandbox status by exact CNPJ only.

    The consolidated Sandbox page publishes CNPJ but not FIP. Therefore fuzzy
    matching is prohibited. A record is applied only when its CNPJ maps to one
    and only one FIP-stable regulatory identity. Unmatched or ambiguous records
    are returned for audit instead of being converted into invented identities.
    """
    output = [deepcopy(item) for item in entities]
    indexes: dict[str, list[int]] = defaultdict(list)
    for index, entity in enumerate(output):
        cnpj = normalize_cnpj(entity.get("cnpj"))
        if cnpj:
            indexes[cnpj].append(index)

    unresolved: list[dict[str, Any]] = []
    for raw in sandbox_records:
        record = dict(raw)
        cnpj = normalize_cnpj(record.get("cnpj"))
        matches = indexes.get(cnpj or "", [])
        if len(matches) != 1:
            unresolved.append(
                {
                    **record,
                    "cnpj": cnpj,
                    "resolution": "ambiguous_cnpj" if len(matches) > 1 else "unmatched_cnpj",
                    "matched_entity_ids": [output[index]["entity_id"] for index in matches],
                }
            )
            continue

        index = matches[0]
        entity = output[index]
        status = record.get("regulatory_status")
        evidence = deepcopy(entity.get("evidence") or {})
        evidence["sandbox"] = _sandbox_evidence(record)

        if status == "temporary_authorized":
            current_status = entity.get("regulatory_status") or "unknown"
            current_regime = entity.get("regulatory_regime") or "unknown"
            if current_status not in {"unknown", "temporary_authorized"} or current_regime not in {
                "unknown",
                "sandbox",
            }:
                raise ClassificationConflictError(
                    f"Sandbox CNPJ {cnpj} maps to {entity['entity_id']} with current "
                    f"status={current_status} regime={current_regime}"
                )
            current_type = entity.get("entity_type") or "unknown"
            if current_type not in {"unknown", "insurer"}:
                raise ClassificationConflictError(
                    f"Sandbox CNPJ {cnpj} maps to non-insurer {entity['entity_id']}"
                )
            entity["entity_type"] = "insurer"
            entity["regulatory_regime"] = "sandbox"
            entity["regulatory_status"] = "temporary_authorized"
        elif status == "sandbox_authorization_cancelled":
            # A historical Sandbox cancellation must never downgrade a later
            # ordinary authorization or special-regime state.
            if entity.get("regulatory_status") in {None, "", "unknown"}:
                entity["entity_type"] = (
                    "insurer" if entity.get("entity_type") in {None, "", "unknown"} else entity["entity_type"]
                )
                entity["regulatory_regime"] = "sandbox"
                entity["regulatory_status"] = "sandbox_authorization_cancelled"
        else:
            raise ClassificationConflictError(
                f"Unsupported Sandbox status for CNPJ {cnpj}: {status}"
            )

        entity["evidence"] = evidence
        output[index] = entity

    _assert_unique_classified_entities(output)
    return sorted(output, key=lambda item: item["entity_id"]), unresolved


def classification_summary(
    entities: list[dict[str, Any]],
    licensed_records: list[dict[str, Any]],
    special_records: list[dict[str, Any]] | None = None,
    sandbox_records: list[dict[str, Any]] | None = None,
    unresolved_sandbox: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    classified = [
        item
        for item in entities
        if item.get("regulatory_status") not in {None, "", "unknown"}
    ]
    by_type = Counter(item.get("entity_type") or "unknown" for item in classified)
    by_status = Counter(item.get("regulatory_status") or "unknown" for item in entities)

    cnpj_filled = 0
    cnpj_variances = 0
    variance_sample: list[dict[str, Any]] = []
    for item in entities:
        evidence = item.get("evidence") or {}
        ses_identity = evidence.get("ses_identity") or {}
        licensed = evidence.get("licensed_entities") or {}
        if not ses_identity.get("cnpj") and licensed.get("cnpj") and evidence.get("ses_present"):
            cnpj_filled += 1
        variance = (evidence.get("identity_variances") or {}).get("cnpj")
        if variance:
            cnpj_variances += 1
            if len(variance_sample) < 20:
                variance_sample.append(
                    {
                        "entity_id": item.get("entity_id"),
                        "legal_name": item.get("legal_name"),
                        "cnpj_variance": variance,
                    }
                )

    cnpj_counts = Counter(item.get("cnpj") for item in entities if item.get("cnpj"))
    duplicate_cnpjs = [cnpj for cnpj, count in cnpj_counts.items() if count > 1]
    added_licensed = sum(
        (item.get("evidence") or {}).get("identity_origin") == "susep_licensed_entities"
        for item in entities
    )
    added_special = sum(
        (item.get("evidence") or {}).get("identity_origin") == "susep_special_regimes"
        for item in entities
    )
    ses_present = sum(bool((item.get("evidence") or {}).get("ses_present")) for item in entities)
    sandbox_applied = sum(bool((item.get("evidence") or {}).get("sandbox")) for item in entities)

    return {
        "inventory_count": len(entities),
        "ses_present_count": ses_present,
        "classified_count": len(classified),
        "unclassified": len(entities) - len(classified),
        "by_entity_type": dict(sorted(by_type.items())),
        "by_regulatory_status": dict(sorted(by_status.items())),
        "cnpj_filled_from_licensed_source": cnpj_filled,
        "cnpj_variances_between_ses_and_licensed": cnpj_variances,
        "cnpj_variance_sample": variance_sample,
        "duplicate_current_cnpj_values": len(duplicate_cnpjs),
        "duplicate_current_cnpj_sample": sorted(duplicate_cnpjs)[:20],
        "licensed_source_count": len(licensed_records),
        "entities_added_from_licensed_source": added_licensed,
        "special_regime_source_count": len(special_records or []),
        "entities_added_from_special_regimes": added_special,
        "sandbox_source_count": len(sandbox_records or []),
        "sandbox_applied_by_exact_cnpj": sandbox_applied,
        "sandbox_unresolved_count": len(unresolved_sandbox or []),
    }
