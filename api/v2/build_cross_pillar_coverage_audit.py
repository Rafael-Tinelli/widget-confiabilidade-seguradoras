from __future__ import annotations

import json
import math
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

STAGE1_PATH = Path("data/derived/v2/cross_pillar_calibration_diagnostic.json")
RECONCILIATION_PATH = Path("data/derived/v2/conduct_coverage_reconciliation.json")
OUTPUT_PATH = Path("data/derived/v2/cross_pillar_coverage_audit.json")

VERSION = "2.0-draft-cross-pillar-coverage-audit-1"


class CrossPillarCoverageAuditError(RuntimeError):
    """Raised when market-footprint coverage cannot be audited safely."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _finite(value: Any) -> float:
    try:
        number = float(value or 0.0)
    except (TypeError, ValueError) as exc:
        raise CrossPillarCoverageAuditError(f"non-numeric exposure: {value!r}") from exc
    if not math.isfinite(number):
        raise CrossPillarCoverageAuditError(f"non-finite exposure: {value!r}")
    return number


def _stage1_entities(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(row.get("entity_id") or ""): row
        for row in payload.get("entities") or []
        if str(row.get("entity_id") or "")
    }


def _reconciliation_entities(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(row.get("entity_id") or ""): row
        for row in payload.get("entities") or []
        if str(row.get("entity_id") or "")
    }


def _premium(row: dict[str, Any]) -> float:
    exposure = row.get("insurance_exposure_12m") or {}
    return _finite(exposure.get("insurance_premium_direct"))


def _complaints(row: dict[str, Any]) -> int:
    value = row.get("complaints_12m")
    if value is None:
        return 0
    return int(value)


def _aggregate(
    ids: set[str],
    reconciliation: dict[str, dict[str, Any]],
    *,
    total_positive_premium: float,
    total_complaints: int,
) -> dict[str, Any]:
    signed_premium = sum(_premium(reconciliation[entity_id]) for entity_id in ids)
    positive_premium = sum(
        max(_premium(reconciliation[entity_id]), 0.0) for entity_id in ids
    )
    complaints = sum(_complaints(reconciliation[entity_id]) for entity_id in ids)
    return {
        "entity_count": len(ids),
        "insurance_premium_direct_signed": float(signed_premium),
        "insurance_premium_direct_positive_footprint": float(positive_premium),
        "positive_premium_share": (
            positive_premium / total_positive_premium if total_positive_premium > 0 else None
        ),
        "complaints_12m": complaints,
        "complaint_share": complaints / total_complaints if total_complaints > 0 else None,
    }


def build_cross_pillar_coverage_audit(
    stage1: dict[str, Any],
    reconciliation: dict[str, Any],
) -> dict[str, Any]:
    if stage1.get("status") != "cross_pillar_calibration_stage_1_diagnostic":
        raise CrossPillarCoverageAuditError("unexpected stage-1 calibration status")
    if stage1.get("scoring") != "forbidden_in_this_artifact":
        raise CrossPillarCoverageAuditError("stage 1 must forbid scoring")
    if reconciliation.get("scoring") != "forbidden_in_this_artifact":
        raise CrossPillarCoverageAuditError("reconciliation must forbid scoring")

    stage1_by_id = _stage1_entities(stage1)
    reconciliation_by_id = _reconciliation_entities(reconciliation)
    if len(stage1_by_id) != 157 or len(reconciliation_by_id) != 157:
        raise CrossPillarCoverageAuditError("coverage audit requires 157 entities")
    if set(stage1_by_id) != set(reconciliation_by_id):
        raise CrossPillarCoverageAuditError("stage-1 and reconciliation populations differ")

    all_ids = set(stage1_by_id)
    total_positive_premium = sum(
        max(_premium(reconciliation_by_id[entity_id]), 0.0) for entity_id in all_ids
    )
    total_signed_premium = sum(
        _premium(reconciliation_by_id[entity_id]) for entity_id in all_ids
    )
    total_complaints = sum(
        _complaints(reconciliation_by_id[entity_id]) for entity_id in all_ids
    )

    readiness_groups: dict[str, set[str]] = defaultdict(set)
    for entity_id, row in stage1_by_id.items():
        readiness_groups[str(row.get("joint_evidence_readiness") or "missing")].add(
            entity_id
        )

    joint_conclusive = readiness_groups.get("joint_core_conclusive", set())
    joint_incomplete = all_ids - joint_conclusive
    pressure_candidates = {
        entity_id
        for entity_id, row in reconciliation_by_id.items()
        if bool((row.get("pressure_comparability") or {}).get("pressure_eligible_candidate"))
    }
    pressure_noncomparable = all_ids - pressure_candidates

    readiness_coverage = {
        state: _aggregate(
            ids,
            reconciliation_by_id,
            total_positive_premium=total_positive_premium,
            total_complaints=total_complaints,
        )
        for state, ids in sorted(readiness_groups.items())
    }

    comparability_groups: dict[str, set[str]] = defaultdict(set)
    for entity_id, row in reconciliation_by_id.items():
        state = str((row.get("pressure_comparability") or {}).get("state") or "missing")
        comparability_groups[state].add(entity_id)

    comparability_coverage = {
        state: _aggregate(
            ids,
            reconciliation_by_id,
            total_positive_premium=total_positive_premium,
            total_complaints=total_complaints,
        )
        for state, ids in sorted(comparability_groups.items())
    }

    excluded_by_premium = sorted(
        (
            {
                "entity_id": entity_id,
                "legal_name": reconciliation_by_id[entity_id].get("legal_name"),
                "positive_insurance_premium_direct": max(
                    _premium(reconciliation_by_id[entity_id]), 0.0
                ),
                "positive_premium_share_of_universe": (
                    max(_premium(reconciliation_by_id[entity_id]), 0.0)
                    / total_positive_premium
                    if total_positive_premium > 0
                    else None
                ),
                "complaints_12m": _complaints(reconciliation_by_id[entity_id]),
                "joint_evidence_readiness": stage1_by_id[entity_id].get(
                    "joint_evidence_readiness"
                ),
                "pressure_comparability_state": (
                    reconciliation_by_id[entity_id].get("pressure_comparability") or {}
                ).get("state"),
            }
            for entity_id in joint_incomplete
        ),
        key=lambda row: (
            -float(row["positive_insurance_premium_direct"]),
            str(row.get("legal_name") or ""),
        ),
    )

    top10_positive_premium = sum(
        float(row["positive_insurance_premium_direct"])
        for row in excluded_by_premium[:10]
    )

    reason_counts = Counter(
        str((reconciliation_by_id[entity_id].get("pressure_comparability") or {}).get("state") or "missing")
        for entity_id in joint_incomplete
    )

    return {
        "artifact": "v2_cross_pillar_coverage_audit",
        "generated_at": _utc_now(),
        "version": VERSION,
        "status": "cross_pillar_market_coverage_audit",
        "scoring": "forbidden_in_this_artifact",
        "ranking": "forbidden_in_this_artifact",
        "human_model": {
            "primary_question": "Se a avaliacao conjunta usar apenas quem tem os dois pilares conclusivos, quanto do mercado fica de fora?",
            "questions": [
                "Qual parcela do premio direto positivo esta coberta?",
                "Qual parcela das reclamacoes observadas esta coberta?",
                "A ausencia de cobertura esta concentrada em empresas pequenas ou em players materialmente relevantes?",
                "Quais rotas de reconciliacao recuperariam mais representatividade?",
            ],
            "principle": "uma metodologia correta sobre uma subamostra pode continuar sendo publicamente enganosa se a cobertura material nao for explicitada",
        },
        "market_footprint_policy": {
            "premium_measure": "insurance_premium_direct_12m",
            "positive_footprint_uses_max_premium_zero": True,
            "reason": "negative direct premium is an accounting/reconciliation exception and must not subtract another entity's market footprint",
            "complaints_measure": "consumer_gov_matched_current_insurer_complaints_12m",
            "complaints_are_not_customer_counts": True,
        },
        "universe": {
            "entities": 157,
            "positive_direct_premium_12m": float(total_positive_premium),
            "signed_direct_premium_12m": float(total_signed_premium),
            "complaints_12m": total_complaints,
        },
        "coverage": {
            "joint_core_conclusive": _aggregate(
                joint_conclusive,
                reconciliation_by_id,
                total_positive_premium=total_positive_premium,
                total_complaints=total_complaints,
            ),
            "joint_core_incomplete": _aggregate(
                joint_incomplete,
                reconciliation_by_id,
                total_positive_premium=total_positive_premium,
                total_complaints=total_complaints,
            ),
            "conduct_pressure_candidates": _aggregate(
                pressure_candidates,
                reconciliation_by_id,
                total_positive_premium=total_positive_premium,
                total_complaints=total_complaints,
            ),
            "conduct_pressure_noncomparable": _aggregate(
                pressure_noncomparable,
                reconciliation_by_id,
                total_positive_premium=total_positive_premium,
                total_complaints=total_complaints,
            ),
            "by_joint_readiness": readiness_coverage,
            "by_pressure_comparability_reason": comparability_coverage,
        },
        "excluded_materiality": {
            "joint_incomplete_reason_counts": dict(sorted(reason_counts.items())),
            "top_10_incomplete_positive_premium_share_of_universe": (
                top10_positive_premium / total_positive_premium
                if total_positive_premium > 0
                else None
            ),
            "largest_incomplete_entities_by_positive_direct_premium": excluded_by_premium[:15],
        },
        "interpretation": {
            "full_market_representativeness_established": False,
            "subset_comparison_requires_explicit_coverage_disclosure": True,
            "current_joint_conclusive_population_can_be_called_full_market_ranking": False,
            "reason": (
                "The jointly conclusive subset excludes a material share of positive insurance premium and observed complaints, including large insurers with unresolved product/subject/carrier comparability."
            ),
        },
    }


def main() -> None:
    stage1 = json.loads(STAGE1_PATH.read_text(encoding="utf-8"))
    reconciliation = json.loads(RECONCILIATION_PATH.read_text(encoding="utf-8"))
    payload = build_cross_pillar_coverage_audit(stage1, reconciliation)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "artifact": payload["artifact"],
                "version": payload["version"],
                "status": payload["status"],
                "universe": payload["universe"],
                "coverage": {
                    "joint_core_conclusive": payload["coverage"]["joint_core_conclusive"],
                    "joint_core_incomplete": payload["coverage"]["joint_core_incomplete"],
                    "conduct_pressure_candidates": payload["coverage"]["conduct_pressure_candidates"],
                    "conduct_pressure_noncomparable": payload["coverage"]["conduct_pressure_noncomparable"],
                },
                "excluded_materiality": payload["excluded_materiality"],
                "interpretation": payload["interpretation"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    print(f"written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
