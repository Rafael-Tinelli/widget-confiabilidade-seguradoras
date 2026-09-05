from __future__ import annotations

import json
import math
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from api.sources.susep_insurance_exposure import (
    DEFAULT_SES_ZIP,
    load_susep_insurance_exposure,
)
from api.utils.identifiers import normalize_cnpj_v2

ELIGIBILITY_PATH = Path("data/derived/v2/entity_eligibility_inventory.json")
CONDUCT_PATH = Path("data/derived/v2/consumer_gov_conduct_evidence.json")
RELATIONSHIPS_PATH = Path("data/reference/v2/conduct_subject_relationships.json")
OUTPUT_PATH = Path("data/derived/v2/conduct_coverage_reconciliation.json")
VERSION = "2.0-draft-conduct-coverage-reconciliation-1"


class ConductCoverageReconciliationError(RuntimeError):
    """Raised when Conduct coverage inputs cannot be reconciled safely."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _period(value: Any) -> int:
    text = str(value or "").strip().replace("-", "")
    if len(text) != 6 or not text.isdigit():
        raise ConductCoverageReconciliationError(f"invalid comparison month: {value!r}")
    year = int(text[:4])
    month = int(text[4:])
    if year < 2000 or not 1 <= month <= 12:
        raise ConductCoverageReconciliationError(f"invalid comparison month: {value!r}")
    return int(text)


def _next_period(period: int) -> int:
    year, month = divmod(period, 100)
    return (year + 1) * 100 + 1 if month == 12 else year * 100 + month + 1


def _validated_periods(months: list[str]) -> list[int]:
    if not months:
        raise ConductCoverageReconciliationError(
            "Conduct evidence has no comparison months"
        )
    periods = [_period(month) for month in months]
    if len(periods) != len(set(periods)):
        raise ConductCoverageReconciliationError(
            "Conduct evidence contains duplicate comparison months"
        )
    if periods != sorted(periods):
        raise ConductCoverageReconciliationError(
            "Conduct comparison months must be chronological"
        )
    if any(
        periods[index] != _next_period(periods[index - 1])
        for index in range(1, len(periods))
    ):
        raise ConductCoverageReconciliationError(
            "Conduct comparison months must be consecutive"
        )
    return periods


def _nonnegative_int(value: Any, *, field: str) -> int:
    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise ConductCoverageReconciliationError(
            f"invalid non-negative integer {field}: {value!r}"
        ) from exc
    if not math.isfinite(numeric) or numeric < 0 or not numeric.is_integer():
        raise ConductCoverageReconciliationError(
            f"invalid non-negative integer {field}: {value!r}"
        )
    return int(numeric)


def _eligible_entities(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        entity
        for entity in payload.get("entities") or []
        if (entity.get("eligibility") or {}).get("regulatory_universe_eligible")
    ]


def _load_relationships(
    payload: dict[str, Any],
    entities: list[dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    by_cnpj = {
        normalize_cnpj_v2(entity.get("cnpj")): entity
        for entity in entities
        if normalize_cnpj_v2(entity.get("cnpj"))
    }
    subjects: dict[str, dict[str, Any]] = {}
    targets: dict[str, list[dict[str, Any]]] = {}

    for raw in payload.get("relationships") or []:
        if not isinstance(raw, dict):
            raise ConductCoverageReconciliationError("relationship must be an object")
        relation = dict(raw)
        relation_id = str(relation.get("relationship_id") or "").strip()
        subject = normalize_cnpj_v2(relation.get("subject_cnpj"))
        target_cnpjs = [
            normalize_cnpj_v2(value) for value in relation.get("target_cnpjs") or []
        ]
        if not relation_id or not subject:
            raise ConductCoverageReconciliationError(
                "relationship_id and subject_cnpj are required"
            )
        if subject not in by_cnpj:
            raise ConductCoverageReconciliationError(
                f"Conduct relationship subject is not in eligible universe: {relation_id}"
            )
        if subject in subjects:
            raise ConductCoverageReconciliationError(
                f"duplicate Conduct subject relationship for CNPJ {subject}"
            )
        for target in target_cnpjs:
            if not target or target not in by_cnpj:
                raise ConductCoverageReconciliationError(
                    f"Conduct relationship target is not in eligible universe: {relation_id} -> {target}"
                )
        relation["subject_cnpj"] = subject
        relation["target_cnpjs"] = target_cnpjs
        subjects[subject] = relation
        for target in target_cnpjs:
            targets.setdefault(target, []).append(relation)

    return subjects, targets


def _complaint_count(evidence: dict[str, Any]) -> tuple[dict[str, Any], int]:
    totals = evidence.get("totals")
    if not isinstance(totals, dict) or "complaints" not in totals:
        raise ConductCoverageReconciliationError(
            "Conduct evidence totals.complaints is required; missing is not zero"
        )
    raw = totals.get("complaints")
    try:
        number = float(raw)
    except (TypeError, ValueError) as exc:
        raise ConductCoverageReconciliationError(
            f"invalid Conduct complaints count: {raw!r}"
        ) from exc
    if not math.isfinite(number) or number < 0 or not number.is_integer():
        raise ConductCoverageReconciliationError(
            f"invalid Conduct complaints count: {raw!r}"
        )
    return dict(totals), int(number)


def _insurance_exposure_12m(
    ses_entity: dict[str, Any],
    periods: list[int],
) -> dict[str, Any]:
    direct = 0.0
    earned = 0.0
    row_count = 0
    months_with_rows = 0
    direct_missing_rows = 0
    earned_missing_rows = 0
    branches: Counter[str] = Counter()
    months = ses_entity.get("months") or {}

    for period in periods:
        month = months.get(period) or {}
        month_rows = 0
        direct_value = float(month.get("insurance_premium_direct") or 0.0)
        earned_value = float(month.get("insurance_premium_earned") or 0.0)
        if not math.isfinite(direct_value) or not math.isfinite(earned_value):
            raise ConductCoverageReconciliationError("non-finite SES insurance exposure")
        direct += direct_value
        earned += earned_value
        direct_missing_rows += _nonnegative_int(
            month.get("insurance_premium_direct_missing_rows") or 0,
            field="insurance_premium_direct_missing_rows",
        )
        earned_missing_rows += _nonnegative_int(
            month.get("insurance_premium_earned_missing_rows") or 0,
            field="insurance_premium_earned_missing_rows",
        )
        for branch, values in (month.get("insurance_branches") or {}).items():
            rows = _nonnegative_int(
                (values or {}).get("rows") or 0,
                field=f"insurance_branches[{branch}].rows",
            )
            amount = float((values or {}).get("premium_direct") or 0.0)
            if not math.isfinite(amount):
                raise ConductCoverageReconciliationError(
                    "non-finite SES branch insurance exposure"
                )
            month_rows += rows
            if amount != 0.0:
                branches[str(branch)] += amount
        row_count += month_rows
        months_with_rows += int(month_rows > 0)

    return {
        "insurance_premium_direct": float(direct),
        "insurance_premium_earned_diagnostic": float(earned),
        "insurance_rows": row_count,
        "insurance_months_with_rows": months_with_rows,
        "insurance_premium_direct_missing_rows": direct_missing_rows,
        "insurance_premium_earned_missing_rows": earned_missing_rows,
        "insurance_premium_direct_complete": direct_missing_rows == 0,
        "insurance_branch_premium_direct": {
            key: float(value) for key, value in sorted(branches.items())
        },
        "private_pension_amount_used": False,
        "capitalization_amount_used": False,
    }


def _target_dependency_state(relations: list[dict[str, Any]]) -> tuple[str, str] | None:
    states: list[tuple[str, str]] = []
    for relation in relations:
        relationship_type = str(relation.get("relationship_type") or "")
        if relationship_type == "consumer_subject_single_risk_carrier":
            states.append(
                (
                    "shared_exposure_with_external_consumer_subject",
                    "brand_complaints_outside_carrier_numerator",
                )
            )
        elif relationship_type == "insurance_portfolio_transfer":
            states.append(
                (
                    "portfolio_transfer_counterparty_requires_temporal_reconciliation",
                    "transferred_portfolio_complaints_and_exposure_not_yet_temporally_aligned",
                )
            )
        elif relationship_type == "insurance_portfolio_split":
            states.append(
                (
                    "shared_consumer_subject_requires_product_split",
                    "generic_subject_complaints_may_belong_to_multiple_carriers",
                )
            )
    if not states:
        return None
    if len({state for state, _ in states}) > 1:
        return (
            "multiple_reconciliation_dependencies",
            "multiple_external_subject_or_portfolio_dependencies",
        )
    return states[0]


def _pressure_state(
    entity: dict[str, Any],
    exposure: dict[str, Any],
    subject_relation: dict[str, Any] | None,
    target_relations: list[dict[str, Any]],
) -> dict[str, Any]:
    relationship_ids = sorted(
        {
            str(relation.get("relationship_id") or "")
            for relation in ([subject_relation] if subject_relation else [])
            + target_relations
            if str(relation.get("relationship_id") or "")
        }
    )

    if subject_relation is not None:
        state = str(subject_relation.get("reconciliation_state") or "").strip()
        policy = str(subject_relation.get("pressure_policy") or "").strip()
        if not state or not policy:
            raise ConductCoverageReconciliationError(
                "Conduct subject relationship lacks policy/state"
            )
        return {
            "state": state,
            "pressure_eligible_candidate": False,
            "reason_code": policy,
            "relationship_ids": relationship_ids,
        }

    dependency = _target_dependency_state(target_relations)
    if dependency is not None:
        state, reason = dependency
        return {
            "state": state,
            "pressure_eligible_candidate": False,
            "reason_code": reason,
            "relationship_ids": relationship_ids,
        }

    activities = entity.get("activities") or {}
    insurance = bool(activities.get("insurance"))
    pension = bool(activities.get("pension"))
    direct = float(exposure["insurance_premium_direct"])

    if insurance and pension:
        return {
            "state": "hybrid_insurance_pension_requires_product_numerator",
            "pressure_eligible_candidate": False,
            "reason_code": "consumer_gov_numerator_not_product_separated_under_p3",
            "relationship_ids": relationship_ids,
        }
    if not insurance and pension:
        return {
            "state": "no_current_insurance_activity_observed_pension_activity_present",
            "pressure_eligible_candidate": False,
            "reason_code": "regulatory_insurer_without_observed_insurance_activity_requires_universe_audit",
            "relationship_ids": relationship_ids,
        }
    if not insurance:
        return {
            "state": "no_current_insurance_activity_observed",
            "pressure_eligible_candidate": False,
            "reason_code": "no_ses_insurance_activity_observed_requires_reconciliation",
            "relationship_ids": relationship_ids,
        }
    if _nonnegative_int(
        exposure.get("insurance_premium_direct_missing_rows") or 0,
        field="insurance_premium_direct_missing_rows",
    ) > 0:
        return {
            "state": "insurance_premium_direct_incomplete",
            "pressure_eligible_candidate": False,
            "reason_code": "missing_direct_premium_rows_in_comparison_window",
            "relationship_ids": relationship_ids,
        }
    if direct < 0:
        return {
            "state": "negative_direct_premium_requires_accounting_review",
            "pressure_eligible_candidate": False,
            "reason_code": "negative_insurance_premium_direct_not_valid_as_pressure_denominator",
            "relationship_ids": relationship_ids,
        }
    if direct == 0:
        return {
            "state": "no_positive_insurance_premium_observed",
            "pressure_eligible_candidate": False,
            "reason_code": "no_positive_insurance_premium_in_comparison_window",
            "relationship_ids": relationship_ids,
        }
    return {
        "state": "direct_one_to_one_candidate",
        "pressure_eligible_candidate": True,
        "reason_code": None,
        "relationship_ids": relationship_ids,
    }


def _widget_coverage_state(
    conduct_state: str,
    pressure_candidate: bool,
) -> str:
    if conduct_state == "source_missing_for_entity":
        return "identity_financial_context_conduct_source_missing"
    if conduct_state == "observed":
        return (
            "conduct_observed_pressure_candidate"
            if pressure_candidate
            else "conduct_observed_pressure_unavailable"
        )
    return (
        "no_observed_complaints_pressure_candidate"
        if pressure_candidate
        else "identity_financial_context_pressure_unavailable"
    )


def build_reconciliation(
    eligibility: dict[str, Any],
    conduct: dict[str, Any],
    ses: dict[str, Any],
    relationship_registry: dict[str, Any],
) -> dict[str, Any]:
    eligible = _eligible_entities(eligibility)
    if not eligible:
        raise ConductCoverageReconciliationError(
            "eligibility artifact has no eligible insurers"
        )

    conduct_months = [
        str(month) for month in (conduct.get("source") or {}).get("months") or []
    ]
    periods = _validated_periods(conduct_months)
    ses_periods = {int(period) for period in ses.get("periods") or []}
    missing = [period for period in periods if period not in ses_periods]
    if missing:
        raise ConductCoverageReconciliationError(
            f"SES insurance exposure misses comparison periods: {missing}"
        )

    conduct_by_id = {
        str(entity.get("entity_id") or ""): entity
        for entity in conduct.get("entities") or []
        if str(entity.get("entity_id") or "")
    }
    subjects, targets = _load_relationships(relationship_registry, eligible)
    ses_entities = ses.get("entities") or {}

    rows: list[dict[str, Any]] = []
    conduct_counts: Counter[str] = Counter()
    pressure_counts: Counter[str] = Counter()
    widget_counts: Counter[str] = Counter()
    universe_flags: Counter[str] = Counter()

    for entity in sorted(eligible, key=lambda item: str(item.get("entity_id") or "")):
        entity_id = str(entity.get("entity_id") or "")
        cnpj = normalize_cnpj_v2(entity.get("cnpj"))
        fip = str(entity.get("fip_code") or "").strip()
        evidence = conduct_by_id.get(entity_id)
        if evidence is None:
            conduct_state = "source_missing_for_entity"
            complaints = None
            totals = None
            film = None
        else:
            totals, complaints = _complaint_count(evidence)
            conduct_state = (
                "observed" if complaints > 0 else "no_observed_complaints"
            )
            film = evidence.get("film")

        exposure = _insurance_exposure_12m(
            ses_entities.get(fip) or {"months": {}}, periods
        )
        subject_relation = subjects.get(cnpj or "")
        target_relations = targets.get(cnpj or "", [])
        pressure = _pressure_state(
            entity, exposure, subject_relation, target_relations
        )
        widget_state = _widget_coverage_state(
            conduct_state, bool(pressure["pressure_eligible_candidate"])
        )

        activities = dict(entity.get("activities") or {})
        audit_flags: list[str] = []
        if not bool(activities.get("insurance")) and bool(activities.get("pension")):
            audit_flags.append(
                "licensed_insurer_without_observed_insurance_activity_pension_present"
            )
        elif not bool(activities.get("insurance")):
            audit_flags.append("licensed_insurer_without_observed_insurance_activity")
        if _nonnegative_int(
            exposure.get("insurance_premium_direct_missing_rows") or 0,
            field="insurance_premium_direct_missing_rows",
        ) > 0:
            audit_flags.append("incomplete_insurance_premium_direct")
        if float(exposure["insurance_premium_direct"]) < 0:
            audit_flags.append("negative_insurance_premium_direct")
        for flag in audit_flags:
            universe_flags[flag] += 1

        relation_context = []
        for relation in ([subject_relation] if subject_relation else []) + target_relations:
            relation_context.append(
                {
                    "relationship_id": relation.get("relationship_id"),
                    "relationship_type": relation.get("relationship_type"),
                    "role": "subject" if relation is subject_relation else "target",
                    "effective_from": relation.get("effective_from"),
                    "pressure_policy": relation.get("pressure_policy"),
                    "evidence": relation.get("evidence") or [],
                }
            )

        conduct_counts[conduct_state] += 1
        pressure_counts[str(pressure["state"])] += 1
        widget_counts[widget_state] += 1
        rows.append(
            {
                "entity_id": entity_id,
                "fip_code": fip or None,
                "cnpj": cnpj,
                "legal_name": entity.get("legal_name"),
                "display_name": evidence.get("display_name") if evidence else None,
                "regulatory_universe_eligible": True,
                "activities": activities,
                "conduct_evidence_state": conduct_state,
                "conduct_totals": totals,
                "conduct_film": film,
                "complaints_12m": complaints,
                "insurance_exposure_12m": exposure,
                "pressure_comparability": pressure,
                "widget_coverage_state": widget_state,
                "reconciliation_relationships": relation_context,
                "universe_audit_flags": audit_flags,
            }
        )

    if len(rows) != len(eligible):
        raise ConductCoverageReconciliationError(
            "eligible universe did not reconcile"
        )

    pressure_unavailable = sorted(
        [
            row
            for row in rows
            if not (row.get("pressure_comparability") or {}).get(
                "pressure_eligible_candidate"
            )
        ],
        key=lambda row: (
            -_nonnegative_int(
                row.get("complaints_12m") or 0,
                field="complaints_12m",
            ),
            str(row.get("legal_name") or ""),
        ),
    )

    return {
        "artifact": "v2_conduct_coverage_reconciliation",
        "generated_at": _utc_now(),
        "status": "experimental_audit",
        "version": VERSION,
        "assessment_role": "conduct_coverage_and_identity_exposure_reconciliation_only",
        "scoring": "forbidden_in_this_artifact",
        "ranking": "forbidden_in_this_artifact",
        "principles": {
            "player_coverage": "pressure_unavailable_does_not_remove_player_from_widget",
            "consumer_subject": "complaints_remain_attached_to_consumer_facing_subject",
            "carrier_context": "verified_carrier_relationships_are_context_not_complaint_transfer",
            "insurance_exposure": "Ses_seguros_premio_direto_only_candidate",
            "insurance_exposure_missingness": "missing_direct_premium_is_not_zero_and_blocks_pressure",
            "private_pension": "excluded_from_insurance_pressure_denominator",
            "capitalization": "excluded_from_insurance_pressure_denominator",
            "zero_complaints": "no_observed_complaints_is_not_a_favorable_conduct_finding",
        },
        "source": {
            "eligibility_artifact": str(ELIGIBILITY_PATH),
            "conduct_artifact": str(CONDUCT_PATH),
            "ses_artifact": str(DEFAULT_SES_ZIP),
            "relationship_registry": str(RELATIONSHIPS_PATH),
            "months": conduct_months,
            "insurance_exposure_component_file": "Ses_seguros.csv",
            "noninsurance_amount_files_not_used": [
                "Ses_Contrib_Benef.csv",
                "Ses_Dados_Cap.csv",
            ],
        },
        "summary": {
            "regulatory_universe_entities": len(rows),
            "conduct_evidence_state_counts": dict(sorted(conduct_counts.items())),
            "pressure_comparability_state_counts": dict(sorted(pressure_counts.items())),
            "widget_coverage_state_counts": dict(sorted(widget_counts.items())),
            "pressure_candidate_entities": sum(
                bool(
                    (row.get("pressure_comparability") or {}).get(
                        "pressure_eligible_candidate"
                    )
                )
                for row in rows
            ),
            "pressure_unavailable_entities": len(pressure_unavailable),
            "universe_audit_flag_counts": dict(sorted(universe_flags.items())),
            "relationship_subjects": len(subjects),
            "relationship_targets": len(targets),
        },
        "pressure_unavailable_by_complaints_desc": [
            {
                "entity_id": row["entity_id"],
                "legal_name": row["legal_name"],
                "complaints_12m": row["complaints_12m"],
                "pressure_state": row["pressure_comparability"]["state"],
                "reason_code": row["pressure_comparability"]["reason_code"],
            }
            for row in pressure_unavailable
        ],
        "entities": rows,
    }


def build_from_files(
    *,
    eligibility_path: Path = ELIGIBILITY_PATH,
    conduct_path: Path = CONDUCT_PATH,
    relationships_path: Path = RELATIONSHIPS_PATH,
    ses_path: Path = DEFAULT_SES_ZIP,
) -> dict[str, Any]:
    eligibility = json.loads(eligibility_path.read_text(encoding="utf-8"))
    conduct = json.loads(conduct_path.read_text(encoding="utf-8"))
    relationships = json.loads(relationships_path.read_text(encoding="utf-8"))
    eligible = _eligible_entities(eligibility)
    fips = [
        str(entity.get("fip_code") or "")
        for entity in eligible
        if str(entity.get("fip_code") or "").strip()
    ]
    ses = load_susep_insurance_exposure(fips, ses_path)
    return build_reconciliation(eligibility, conduct, ses, relationships)


def main() -> None:
    payload = build_from_files()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "artifact": payload["artifact"],
                "version": payload["version"],
                "summary": payload["summary"],
                "top_pressure_unavailable": payload[
                    "pressure_unavailable_by_complaints_desc"
                ][:20],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    print(f"written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
