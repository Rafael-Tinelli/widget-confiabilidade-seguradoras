from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REGULATORY_PATH = Path("data/derived/v2/entity_eligibility_inventory.json")
SEMANTIC_PATH = Path(
    "data/derived/v2/cross_pillar_assessment_semantic_contract.json"
)
OUTPUT_PATH = Path("data/derived/v2/assessment_eligibility_contract.json")

VERSION = "2.0-draft-assessment-eligibility-contract-2"
OPERATIONAL_FRESHNESS_POLICY = (
    "single_generation_workspace_cross_run_latest_successful_restore_forbidden"
)

ACCEPTED_FINANCIAL_CONFIDENCE = {
    "historico_estabelecido",
    "historico_limitado",
}

READINESS_REASON = {
    "avaliacao_conjunta_central_completa": "joint_core_complete",
    "conduta_nao_comparavel_com_seguranca": "conduct_not_comparable",
    "conduta_com_cobertura_temporal_insuficiente": (
        "conduct_insufficient_temporal_coverage"
    ),
    "conduta_sensivel_ao_denominador": "conduct_denominator_sensitive",
    "financeiro_central_incompleto": "financial_core_incomplete",
    "conduta_inconclusiva_ou_indisponivel": "conduct_inconclusive_or_unavailable",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _regulatory_eligible(
    payload: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for entity in payload.get("entities") or []:
        eligibility = entity.get("eligibility") or {}
        if not eligibility.get("regulatory_universe_eligible"):
            continue
        entity_id = str(entity.get("entity_id") or "")
        if not entity_id:
            raise ValueError("regulatory eligible entity without entity_id")
        if entity_id in result:
            raise ValueError(f"duplicate regulatory eligible entity: {entity_id}")
        result[entity_id] = entity
    return result


def _semantic_by_id(
    payload: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in payload.get("entities") or []:
        entity_id = str(row.get("entity_id") or "")
        if not entity_id:
            raise ValueError("semantic entity without entity_id")
        if entity_id in result:
            raise ValueError(f"duplicate semantic entity: {entity_id}")
        result[entity_id] = row
    return result


def _confidence_gate(row: dict[str, Any]) -> dict[str, Any]:
    confidence = str(
        ((row.get("qualifiers") or {}).get("financial_confidence")) or ""
    )
    accepted = confidence in ACCEPTED_FINANCIAL_CONFIDENCE
    return {
        "financial_history_confidence": confidence or "confianca_nao_classificada",
        "accepted_for_assessment": accepted,
        "limited_history_is_performance_penalty": False,
        "limited_history_blocks_assessment": False,
        "role": (
            "disclosure_only_not_performance"
            if accepted
            else "blocks_when_core_evidence_is_insufficient_or_unclassified"
        ),
    }


def _assessment_state(
    semantic_row: dict[str, Any],
    confidence: dict[str, Any],
) -> tuple[str, bool, list[str]]:
    supported = bool(semantic_row.get("semantic_public_assessment_supported"))
    completeness = str(semantic_row.get("assessment_completeness") or "")
    readiness = str(semantic_row.get("evidence_readiness") or "")

    if not supported or completeness != "joint_core_complete":
        reason = READINESS_REASON.get(readiness, "joint_evidence_incomplete")
        return (
            "not_eligible_joint_evidence_incomplete",
            False,
            [reason, "missingness_not_treated_as_neutral"],
        )

    if not confidence["accepted_for_assessment"]:
        return (
            "not_eligible_core_evidence_confidence",
            False,
            [
                "joint_core_complete",
                "financial_core_confidence_insufficient_or_unclassified",
            ],
        )

    if not semantic_row.get("matrix_state"):
        raise ValueError(
            f"semantic complete entity without matrix state: {semantic_row.get('entity_id')}"
        )
    public = semantic_row.get("public_assessment") or {}
    if not public.get("public_class") or not public.get("title"):
        raise ValueError(
            f"semantic complete entity without public assessment: {semantic_row.get('entity_id')}"
        )

    return (
        "eligible_complete_joint_assessment",
        True,
        [
            "regulatory_gate_passed",
            "joint_core_complete",
            "financial_core_evidence_sufficient",
            "conduct_conclusion_comparable_and_conclusive",
            "semantic_public_assessment_supported",
        ],
    )


def build_assessment_eligibility_contract(
    regulatory: dict[str, Any],
    semantic: dict[str, Any],
) -> dict[str, Any]:
    if regulatory.get("artifact") != "v2_entity_eligibility_inventory":
        raise ValueError("unexpected regulatory eligibility artifact")
    if semantic.get("status") != "cross_pillar_assessment_semantic_contract_closed":
        raise ValueError("semantic assessment contract is not closed")
    if semantic.get("scoring") != "forbidden_in_this_artifact":
        raise ValueError("semantic contract must forbid scoring")
    if semantic.get("ranking") != "forbidden_in_this_artifact":
        raise ValueError("semantic contract must forbid ranking")

    regulatory_by_id = _regulatory_eligible(regulatory)
    semantic_by_id = _semantic_by_id(semantic)
    semantic_population = semantic.get("population") or {}
    expected_regulatory = int(
        semantic_population.get("regulatory_universe") or 0
    )

    if not expected_regulatory:
        raise ValueError("semantic contract missing regulatory universe count")
    if len(regulatory_by_id) != expected_regulatory:
        raise ValueError(
            "regulatory universe count mismatch: "
            f"regulatory={len(regulatory_by_id)} semantic={expected_regulatory}"
        )
    if len(semantic_by_id) != expected_regulatory:
        raise ValueError(
            "semantic entity count mismatch: "
            f"entities={len(semantic_by_id)} expected={expected_regulatory}"
        )
    if set(regulatory_by_id) != set(semantic_by_id):
        missing_semantic = sorted(set(regulatory_by_id) - set(semantic_by_id))
        missing_regulatory = sorted(set(semantic_by_id) - set(regulatory_by_id))
        raise ValueError(
            "regulatory/semantic populations differ: "
            f"missing_semantic={missing_semantic[:5]} "
            f"missing_regulatory={missing_regulatory[:5]}"
        )

    rows: list[dict[str, Any]] = []
    state_counts: Counter[str] = Counter()
    reason_counts: Counter[str] = Counter()
    eligible_public_classes: Counter[str] = Counter()
    eligible_confidence: Counter[str] = Counter()
    ineligible_readiness: Counter[str] = Counter()

    for entity_id in sorted(semantic_by_id):
        semantic_row = semantic_by_id[entity_id]
        regulatory_row = regulatory_by_id[entity_id]
        confidence = _confidence_gate(semantic_row)
        assessment_state, assessment_eligible, reasons = _assessment_state(
            semantic_row,
            confidence,
        )
        state_counts[assessment_state] += 1
        reason_counts.update(reasons)

        public = semantic_row.get("public_assessment") or {}
        public_class = str(public.get("public_class") or "")
        if assessment_eligible:
            eligible_public_classes[public_class] += 1
            eligible_confidence[
                confidence["financial_history_confidence"]
            ] += 1
            if public_class == "evidence_incomplete":
                raise ValueError(
                    f"incomplete public class became assessment eligible: {entity_id}"
                )
        else:
            ineligible_readiness[
                str(semantic_row.get("evidence_readiness") or "unclassified")
            ] += 1

        regulatory_state = regulatory_row.get("eligibility") or {}
        if not regulatory_state.get("regulatory_universe_eligible"):
            raise ValueError(
                f"non-regulatory entity reached assessment gate: {entity_id}"
            )

        rows.append(
            {
                "entity_id": entity_id,
                "fip_code": semantic_row.get("fip_code")
                or regulatory_row.get("fip_code"),
                "legal_name": semantic_row.get("legal_name")
                or regulatory_row.get("legal_name"),
                "regulatory_universe_eligible": True,
                "assessment_state": assessment_state,
                "assessment_eligible": assessment_eligible,
                "assessment_scope": (
                    "ordinary_current_insurers_joint_core_v2"
                    if assessment_eligible
                    else None
                ),
                "assessment_reason_codes": reasons,
                "evidence_confidence": confidence,
                "semantic_assessment": {
                    "assessment_completeness": semantic_row.get(
                        "assessment_completeness"
                    ),
                    "matrix_state": semantic_row.get("matrix_state"),
                    "public_class": public.get("public_class"),
                    "public_title": public.get("title"),
                    "evidence_readiness": semantic_row.get("evidence_readiness"),
                },
                "performance_used_to_decide_eligibility": False,
                "ranking_state": (
                    "pending_ranking_eligibility_contract"
                    if assessment_eligible
                    else "blocked_by_assessment_ineligibility"
                ),
                "ranking_eligible": False,
                "comparison_cohort": None,
            }
        )

    assessment_eligible_count = sum(
        bool(row["assessment_eligible"]) for row in rows
    )
    semantic_supported = int(
        semantic_population.get("semantic_public_assessment_supported") or 0
    )
    if assessment_eligible_count > semantic_supported:
        raise ValueError(
            "assessment eligible count exceeds semantic supported population"
        )

    return {
        "artifact": "v2_assessment_eligibility_contract",
        "generated_at": _utc_now(),
        "version": VERSION,
        "status": "assessment_eligibility_contract_closed",
        "scoring": "forbidden_in_this_artifact",
        "ranking": "forbidden_in_this_artifact",
        "human_model": {
            "primary_question": (
                "Temos base regulatória, comparabilidade e evidência suficientes "
                "para publicar uma avaliação conjunta desta seguradora?"
            ),
            "rule": (
                "elegibilidade mede se a avaliação pode ser feita com segurança; "
                "não mede se o resultado é favorável ou adverso"
            ),
        },
        "source_contracts": {
            "regulatory_eligibility_artifact": regulatory.get("artifact"),
            "regulatory_eligibility_version": (
                (regulatory.get("meta") or {}).get("eligibility_version")
            ),
            "regulatory_generated_at": regulatory.get("generated_at"),
            "semantic_artifact": semantic.get("artifact"),
            "semantic_version": semantic.get("version"),
            "semantic_generated_at": semantic.get("generated_at"),
            "operational_freshness_policy": OPERATIONAL_FRESHNESS_POLICY,
        },
        "gate_contract": {
            "regulatory_universe_membership_required": True,
            "joint_core_complete_required": True,
            "semantic_public_assessment_support_required": True,
            "financial_core_evidence_required": True,
            "conduct_comparability_and_credibility_required": True,
            "limited_financial_history_blocks_assessment": False,
            "insufficient_or_unclassified_core_confidence_blocks_assessment": True,
            "performance_result_used_for_eligibility": False,
            "adverse_result_blocks_assessment": False,
            "missingness_treated_as_neutral": False,
            "ranking_gate_independent": True,
            "comparison_cohort_selected": False,
        },
        "population": {
            "regulatory_universe": len(rows),
            "semantic_public_assessment_supported": semantic_supported,
            "assessment_eligible": assessment_eligible_count,
            "assessment_not_eligible": len(rows) - assessment_eligible_count,
            "ranking_eligible": 0,
        },
        "diagnostics": {
            "assessment_state_counts": dict(sorted(state_counts.items())),
            "assessment_reason_counts": dict(sorted(reason_counts.items())),
            "eligible_public_class_counts": dict(
                sorted(eligible_public_classes.items())
            ),
            "eligible_financial_confidence_counts": dict(
                sorted(eligible_confidence.items())
            ),
            "ineligible_evidence_readiness_counts": dict(
                sorted(ineligible_readiness.items())
            ),
        },
        "closure_decision": {
            "assessment_eligibility_gate_opened": True,
            "ranking_eligibility_gate_opened": False,
            "assessment_eligible_is_not_quality_label": True,
            "assessment_eligible_is_not_ranking_eligible": True,
            "full_market_ranking_supported": False,
            "next_methodological_gate": "ranking_eligibility_preflight",
        },
        "entities": rows,
    }


def main() -> None:
    regulatory = json.loads(REGULATORY_PATH.read_text(encoding="utf-8"))
    semantic = json.loads(SEMANTIC_PATH.read_text(encoding="utf-8"))
    payload = build_assessment_eligibility_contract(regulatory, semantic)
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
                "population": payload["population"],
                "diagnostics": payload["diagnostics"],
                "closure_decision": payload["closure_decision"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    print(f"written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
