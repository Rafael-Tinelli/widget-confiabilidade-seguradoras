from __future__ import annotations

from collections import Counter
from copy import deepcopy
from typing import Any

ELIGIBILITY_VERSION = "2.0-draft-regulatory-gate-1"
REGULATORY_UNIVERSE_ID = "ordinary_current_insurers"

PENDING_ASSESSMENT_REQUIREMENTS = [
    "financial_evidence_gate",
    "complaints_evidence_gate",
    "methodology_calibration",
    "assessment_confidence_gate",
    "comparison_cohort_definition",
]

REINSURANCE_ENTITY_TYPES = {
    "local_reinsurer",
    "admitted_reinsurer",
    "occasional_reinsurer",
    "reinsurance_broker",
}

NON_ACTIVE_LEGAL_STATUSES = {"closed", "suspended", "unfit", "null"}


class EligibilityInvariantError(ValueError):
    """Raised when an eligibility state contradicts identity/regulatory facts."""


def _has_successor(entity: dict[str, Any]) -> bool:
    return any(
        relation.get("relationship_type") == "incorporated_into"
        and relation.get("target_entity_id")
        for relation in (entity.get("relationships") or [])
    )


def _is_historical(entity: dict[str, Any]) -> bool:
    query_context = entity.get("query_context") or {}
    lifecycle = entity.get("legal_lifecycle") or {}
    return bool(
        query_context.get("filter_bucket") == "historical"
        or lifecycle.get("cadastral_status") == "closed"
        or _has_successor(entity)
    )


def _outside_scope_reason(entity_type: str) -> str:
    if entity_type == "sandbox_participant":
        return "sandbox_experimental_regime"
    if entity_type == "open_pension_entity":
        return "different_market_open_pension"
    if entity_type == "capitalization_company":
        return "different_market_capitalization"
    if entity_type in REINSURANCE_ENTITY_TYPES:
        return "different_market_reinsurance"
    if entity_type == "self_regulator":
        return "different_market_self_regulation"
    return "entity_type_not_insurer"


def derive_eligibility(entity: dict[str, Any]) -> dict[str, Any]:
    """Derive v2 eligibility without calculating any score.

    Regulatory-universe eligibility answers only whether the record is a
    current ordinary insurer that can proceed to the future evidence gates.
    It deliberately does not imply assessment or ranking eligibility.

    SUSEP is authoritative for licensing. Receita lifecycle is a legal
    cross-check: contradictory non-active CNPJ evidence blocks the entity, but
    an unavailable Receita observation must not silently revoke a SUSEP license.
    """
    entity_type = str(entity.get("entity_type") or "unknown")
    regulatory_regime = str(entity.get("regulatory_regime") or "unknown")
    regulatory_status = str(entity.get("regulatory_status") or "unknown")
    lifecycle = entity.get("legal_lifecycle") or {}
    legal_status = lifecycle.get("cadastral_status")

    reasons: list[str] = []
    regulatory_eligible = True

    if _is_historical(entity):
        regulatory_eligible = False
        reasons.append("historical_legal_entity")

    if entity_type != "insurer":
        regulatory_eligible = False
        reasons.append(_outside_scope_reason(entity_type))

    if regulatory_regime == "special":
        regulatory_eligible = False
        reasons.append("special_regulatory_regime")
    elif regulatory_regime != "ordinary" and entity_type == "insurer":
        regulatory_eligible = False
        reasons.append("non_ordinary_regulatory_regime")

    if entity_type == "insurer" and regulatory_status != "active_licensed":
        regulatory_eligible = False
        reasons.append("not_currently_licensed_as_insurer")

    if entity_type == "insurer" and not entity.get("cnpj"):
        regulatory_eligible = False
        reasons.append("missing_legal_entity_identifier")

    if legal_status in NON_ACTIVE_LEGAL_STATUSES:
        regulatory_eligible = False
        reasons.append("legal_entity_not_active_in_receita")

    if regulatory_eligible:
        reasons.extend(
            [
                "susep_current_insurer",
                "ordinary_regulatory_regime",
                "current_active_license",
            ]
        )
        if legal_status == "active":
            reasons.append("receita_legal_entity_active")
        elif legal_status is None:
            reasons.append("receita_lifecycle_crosscheck_unavailable")

    assessment_state = "pending_evidence" if regulatory_eligible else "not_eligible"
    ranking_state = "pending_assessment" if regulatory_eligible else "not_eligible"

    return {
        "eligibility_version": ELIGIBILITY_VERSION,
        "regulatory_universe_id": (
            REGULATORY_UNIVERSE_ID if regulatory_eligible else None
        ),
        "regulatory_universe_eligible": regulatory_eligible,
        "assessment_state": assessment_state,
        "assessment_eligible": False,
        "ranking_state": ranking_state,
        "ranking_eligible": False,
        "comparison_cohort": None,
        "pending_requirements": (
            list(PENDING_ASSESSMENT_REQUIREMENTS) if regulatory_eligible else []
        ),
        "reason_codes": list(dict.fromkeys(reasons)),
    }


def apply_eligibility(entities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    for raw in entities:
        entity = deepcopy(raw)
        entity["eligibility"] = derive_eligibility(entity)
        output.append(entity)
    return sorted(output, key=lambda item: item["entity_id"])


def validate_eligibility(entities: list[dict[str, Any]]) -> None:
    errors: list[str] = []
    for entity in entities:
        state = entity.get("eligibility") or {}
        entity_id = entity.get("entity_id")

        if state.get("assessment_eligible"):
            errors.append(f"{entity_id}: assessment cannot be eligible before evidence gates")
        if state.get("ranking_eligible"):
            errors.append(f"{entity_id}: ranking cannot be eligible before assessment")

        if not state.get("regulatory_universe_eligible"):
            continue

        if entity.get("entity_type") != "insurer":
            errors.append(f"{entity_id}: non-insurer entered regulatory universe")
        if entity.get("regulatory_regime") != "ordinary":
            errors.append(f"{entity_id}: non-ordinary regime entered regulatory universe")
        if entity.get("regulatory_status") != "active_licensed":
            errors.append(f"{entity_id}: inactive license entered regulatory universe")
        if not entity.get("cnpj"):
            errors.append(f"{entity_id}: eligible insurer without CNPJ")
        if _is_historical(entity):
            errors.append(f"{entity_id}: historical entity entered regulatory universe")
        legal_status = (entity.get("legal_lifecycle") or {}).get("cadastral_status")
        if legal_status in NON_ACTIVE_LEGAL_STATUSES:
            errors.append(f"{entity_id}: non-active Receita CNPJ entered regulatory universe")

    if errors:
        raise EligibilityInvariantError("; ".join(errors[:20]))


def eligibility_summary(entities: list[dict[str, Any]]) -> dict[str, Any]:
    eligible = [
        entity
        for entity in entities
        if (entity.get("eligibility") or {}).get("regulatory_universe_eligible")
    ]
    exclusion_reasons: Counter[str] = Counter()
    legal_crosscheck: Counter[str] = Counter()

    for entity in entities:
        state = entity.get("eligibility") or {}
        if not state.get("regulatory_universe_eligible"):
            exclusion_reasons.update(state.get("reason_codes") or [])
        else:
            legal_status = (entity.get("legal_lifecycle") or {}).get(
                "cadastral_status"
            )
            legal_crosscheck[legal_status or "unavailable"] += 1

    return {
        "eligibility_version": ELIGIBILITY_VERSION,
        "regulatory_universe_id": REGULATORY_UNIVERSE_ID,
        "regulatory_universe_eligible_count": len(eligible),
        "assessment_eligible_count": sum(
            1
            for entity in entities
            if (entity.get("eligibility") or {}).get("assessment_eligible")
        ),
        "ranking_eligible_count": sum(
            1
            for entity in entities
            if (entity.get("eligibility") or {}).get("ranking_eligible")
        ),
        "eligible_legal_crosscheck_counts": dict(sorted(legal_crosscheck.items())),
        "exclusion_reason_counts": dict(sorted(exclusion_reasons.items())),
    }
