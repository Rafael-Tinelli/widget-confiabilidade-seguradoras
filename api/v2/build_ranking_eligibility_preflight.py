from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ASSESSMENT_PATH = Path("data/derived/v2/assessment_eligibility_contract.json")
STAGE1_PATH = Path("data/derived/v2/cross_pillar_calibration_diagnostic.json")
STAGE2_PATH = Path("data/derived/v2/cross_pillar_architecture_experiment.json")
COVERAGE_PATH = Path("data/derived/v2/cross_pillar_coverage_audit.json")
OUTPUT_PATH = Path("data/derived/v2/ranking_eligibility_preflight.json")

VERSION = "2.0-draft-ranking-eligibility-preflight-1"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _entity_rows(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in payload.get("entities") or []:
        entity_id = str(row.get("entity_id") or "")
        if not entity_id:
            raise ValueError("assessment eligibility entity without entity_id")
        if entity_id in result:
            raise ValueError(f"duplicate assessment entity: {entity_id}")
        result[entity_id] = row
    return result


def _validate_sources(
    assessment: dict[str, Any],
    stage1: dict[str, Any],
    stage2: dict[str, Any],
    coverage: dict[str, Any],
) -> tuple[int, int]:
    if assessment.get("status") != "assessment_eligibility_contract_closed":
        raise ValueError("assessment eligibility contract is not closed")
    decision = assessment.get("closure_decision") or {}
    if decision.get("assessment_eligibility_gate_opened") is not True:
        raise ValueError("assessment eligibility gate is not open")
    if decision.get("ranking_eligibility_gate_opened") is not False:
        raise ValueError("ranking gate must still be closed before preflight")
    if assessment.get("scoring") != "forbidden_in_this_artifact":
        raise ValueError("assessment eligibility must forbid scoring")
    if assessment.get("ranking") != "forbidden_in_this_artifact":
        raise ValueError("assessment eligibility must forbid ranking")

    if stage1.get("status") != "cross_pillar_calibration_stage_1_diagnostic":
        raise ValueError("unexpected Stage 1 status")
    if stage1.get("scoring") != "forbidden_in_this_artifact":
        raise ValueError("Stage 1 must forbid scoring")
    if stage1.get("ranking") != "forbidden_in_this_artifact":
        raise ValueError("Stage 1 must forbid ranking")

    if stage2.get("status") != "cross_pillar_architecture_stage_2_experiment":
        raise ValueError("unexpected Stage 2 status")
    if stage2.get("scoring") != "forbidden_in_this_artifact":
        raise ValueError("Stage 2 must forbid scoring")
    if stage2.get("ranking") != "forbidden_in_this_artifact":
        raise ValueError("Stage 2 must forbid ranking")

    if coverage.get("status") != "cross_pillar_market_coverage_audit":
        raise ValueError("unexpected coverage audit status")
    if coverage.get("scoring") != "forbidden_in_this_artifact":
        raise ValueError("coverage audit must forbid scoring")
    if coverage.get("ranking") != "forbidden_in_this_artifact":
        raise ValueError("coverage audit must forbid ranking")

    population = assessment.get("population") or {}
    regulatory = int(population.get("regulatory_universe") or 0)
    assessable = int(population.get("assessment_eligible") or 0)
    if not regulatory:
        raise ValueError("assessment contract missing regulatory universe")
    if assessable <= 0 or assessable > regulatory:
        raise ValueError("invalid assessment eligible population")
    if int(population.get("ranking_eligible") or 0) != 0:
        raise ValueError("ranking eligibility must be zero entering preflight")

    stage1_population = stage1.get("population") or {}
    if int(stage1_population.get("regulatory_universe") or 0) != regulatory:
        raise ValueError("Stage 1 regulatory population mismatch")
    if int(stage1_population.get("joint_core_conclusive") or 0) != assessable:
        raise ValueError("Stage 1 conclusive population mismatch")

    stage2_coverage = stage2.get("coverage_constraint") or {}
    if int(stage2_coverage.get("joint_conclusive_entities") or 0) != assessable:
        raise ValueError("Stage 2 conclusive population mismatch")

    coverage_joint = (coverage.get("coverage") or {}).get("joint_core_conclusive") or {}
    if int(coverage_joint.get("entity_count") or 0) != assessable:
        raise ValueError("coverage audit conclusive population mismatch")

    return regulatory, assessable


def _ordering_diagnostic(
    stage1: dict[str, Any], stage2: dict[str, Any]
) -> dict[str, Any]:
    safe_resolution = (stage1.get("diagnostics") or {}).get(
        "safe_core_signature_resolution"
    ) or {}
    pareto = (stage1.get("diagnostics") or {}).get("pareto_partial_order") or {}
    pairwise = pareto.get("pairwise_orderability") or {}
    architecture = stage2.get("architecture_decision") or {}
    matrix = stage2.get("matrix_architecture") or {}
    tradeoffs = stage2.get("normative_tradeoffs") or {}
    scenario = stage2.get("scenario_comparison") or {}

    selected_total_order = any(
        bool(architecture.get(field))
        for field in (
            "continuous_weighted_score_selected",
            "lexicographic_total_order_selected",
            "capital_gate_total_order_selected",
        )
    )
    pareto_public_tier = bool(
        architecture.get("pareto_front_number_selected_as_public_tier")
    )

    tied_pairs = int(pairwise.get("tied_pairs") or 0)
    incomparable_pairs = int(pairwise.get("incomparable_pairs") or 0)
    entities_in_tied_groups = int(
        safe_resolution.get("entities_in_tied_groups") or 0
    )
    tradeoff_pairs = int(tradeoffs.get("tradeoff_entity_pairs") or 0)
    scenario_disagreement = int(
        ((scenario.get("pairwise_scenario_disagreement") or {}).get(
            "financial_vs_conduct_lexicographic"
        ))
        or 0
    )

    return {
        "matrix_pretends_total_order": bool(
            (matrix.get("properties") or {}).get("pretends_total_order")
        ),
        "approved_total_order_selected": selected_total_order,
        "pareto_public_tier_selected": pareto_public_tier,
        "within_state_tiebreaker_selected": False,
        "unique_semantic_groups": int(safe_resolution.get("unique_groups") or 0),
        "largest_semantic_group": int(safe_resolution.get("largest_group") or 0),
        "entities_in_tied_semantic_groups": entities_in_tied_groups,
        "pair_count": int(pairwise.get("pair_count") or 0),
        "strictly_comparable_pairs": int(
            pairwise.get("strictly_comparable_pairs") or 0
        ),
        "tied_pairs": tied_pairs,
        "incomparable_pairs": incomparable_pairs,
        "normative_tradeoff_pairs": tradeoff_pairs,
        "financial_vs_conduct_lexicographic_disagreement_pairs": scenario_disagreement,
        "total_1_to_n_order_supported": (
            selected_total_order
            and tied_pairs == 0
            and incomparable_pairs == 0
            and entities_in_tied_groups == 0
        ),
    }


def _coverage_diagnostic(
    coverage: dict[str, Any], regulatory: int, assessable: int
) -> dict[str, Any]:
    coverage_block = coverage.get("coverage") or {}
    joint = coverage_block.get("joint_core_conclusive") or {}
    incomplete = coverage_block.get("joint_core_incomplete") or {}
    excluded = coverage.get("excluded_materiality") or {}
    interpretation = coverage.get("interpretation") or {}

    return {
        "regulatory_entity_count": regulatory,
        "assessment_eligible_entity_count": assessable,
        "assessment_eligible_entity_share": assessable / regulatory,
        "assessment_ineligible_entity_count": regulatory - assessable,
        "assessment_eligible_positive_premium_share": float(
            joint.get("positive_premium_share") or 0.0
        ),
        "assessment_eligible_complaint_share": float(
            joint.get("complaint_share") or 0.0
        ),
        "assessment_ineligible_positive_premium_share": float(
            incomplete.get("positive_premium_share") or 0.0
        ),
        "assessment_ineligible_complaint_share": float(
            incomplete.get("complaint_share") or 0.0
        ),
        "top_10_ineligible_positive_premium_share_of_universe": float(
            excluded.get("top_10_incomplete_positive_premium_share_of_universe")
            or 0.0
        ),
        "full_market_representativeness_established": bool(
            interpretation.get("full_market_representativeness_established")
        ),
        "subset_comparison_requires_explicit_coverage_disclosure": bool(
            interpretation.get("subset_comparison_requires_explicit_coverage_disclosure")
        ),
        "arbitrary_representativeness_threshold_selected": False,
        "full_market_scope_complete": assessable == regulatory,
    }


def build_ranking_eligibility_preflight(
    assessment: dict[str, Any],
    stage1: dict[str, Any],
    stage2: dict[str, Any],
    coverage: dict[str, Any],
) -> dict[str, Any]:
    regulatory, assessable = _validate_sources(
        assessment, stage1, stage2, coverage
    )
    by_id = _entity_rows(assessment)
    if len(by_id) != regulatory:
        raise ValueError("assessment entity population mismatch")

    ordering = _ordering_diagnostic(stage1, stage2)
    market_coverage = _coverage_diagnostic(coverage, regulatory, assessable)

    candidate_class_counts: Counter[str] = Counter()
    preflight_state_counts: Counter[str] = Counter()
    rows: list[dict[str, Any]] = []

    for entity_id in sorted(by_id):
        source = by_id[entity_id]
        assessment_eligible = bool(source.get("assessment_eligible"))
        if source.get("ranking_eligible") is not False:
            raise ValueError(f"ranking already opened upstream: {entity_id}")
        if source.get("comparison_cohort") is not None:
            raise ValueError(f"ranking cohort selected upstream: {entity_id}")

        public_class = str(
            ((source.get("semantic_assessment") or {}).get("public_class")) or ""
        )
        if assessment_eligible:
            preflight_state = "candidate_but_ranking_contract_not_supported"
            candidate_class_counts[public_class] += 1
        else:
            preflight_state = "blocked_by_assessment_ineligibility"
        preflight_state_counts[preflight_state] += 1

        rows.append(
            {
                "entity_id": entity_id,
                "fip_code": source.get("fip_code"),
                "legal_name": source.get("legal_name"),
                "assessment_eligible": assessment_eligible,
                "ranking_preflight_candidate": assessment_eligible,
                "ranking_preflight_state": preflight_state,
                "ranking_eligible": False,
                "ranking_position": None,
                "comparison_cohort": None,
                "performance_used_to_decide_ranking_candidacy": False,
                "public_class_preserved_for_diagnostics_only": public_class or None,
            }
        )

    candidates = sum(row["ranking_preflight_candidate"] for row in rows)
    if candidates != assessable:
        raise ValueError("ranking preflight candidate count mismatch")

    full_market_blockers: list[str] = []
    if not market_coverage["full_market_scope_complete"]:
        full_market_blockers.append("assessment_does_not_cover_full_regulatory_universe")
    if not market_coverage["full_market_representativeness_established"]:
        full_market_blockers.append("full_market_representativeness_not_established")
    if not ordering["approved_total_order_selected"]:
        full_market_blockers.append("no_approved_total_order_rule")
    if ordering["normative_tradeoff_pairs"]:
        full_market_blockers.append("normative_cross_pillar_tradeoffs_unresolved")
    if ordering["tied_pairs"]:
        full_market_blockers.append("within_state_order_not_supported")

    subset_blockers: list[str] = []
    if not ordering["approved_total_order_selected"]:
        subset_blockers.append("no_approved_total_order_rule")
    if ordering["normative_tradeoff_pairs"]:
        subset_blockers.append("normative_cross_pillar_tradeoffs_unresolved")
    if ordering["tied_pairs"]:
        subset_blockers.append("within_state_order_not_supported")
    if ordering["entities_in_tied_semantic_groups"]:
        subset_blockers.append("all_current_candidates_share_semantic_ties")

    full_market_total_ranking_supported = not full_market_blockers
    eligible_subset_total_ranking_supported = not subset_blockers
    if full_market_total_ranking_supported or eligible_subset_total_ranking_supported:
        raise ValueError(
            "current preflight unexpectedly supports ranking; a formal ranking contract is required"
        )

    return {
        "artifact": "v2_ranking_eligibility_preflight",
        "generated_at": _utc_now(),
        "version": VERSION,
        "status": "ranking_eligibility_preflight_closed_gate_remains_blocked",
        "scoring": "forbidden_in_this_artifact",
        "ranking": "forbidden_in_this_artifact",
        "human_model": {
            "primary_question": (
                "Existe hoje uma coorte e uma regra de ordenação defensáveis para chamar "
                "o resultado de ranking, sem esconder exclusões, empates ou escolhas normativas?"
            ),
            "rule": (
                "avaliabilidade individual não autoriza ranking; escopo, representatividade "
                "e ordenação precisam ser sustentados separadamente"
            ),
        },
        "source_contracts": {
            "assessment_eligibility_artifact": assessment.get("artifact"),
            "assessment_eligibility_version": assessment.get("version"),
            "stage1_artifact": stage1.get("artifact"),
            "stage1_version": stage1.get("version"),
            "stage2_artifact": stage2.get("artifact"),
            "stage2_version": stage2.get("version"),
            "coverage_artifact": coverage.get("artifact"),
            "coverage_version": coverage.get("version"),
            "operational_freshness_policy": (
                "single_generation_workspace_cross_run_latest_successful_restore_forbidden"
            ),
        },
        "claim_contract": {
            "full_market_total_ranking_supported": full_market_total_ranking_supported,
            "assessment_eligible_subset_total_ranking_supported": (
                eligible_subset_total_ranking_supported
            ),
            "assessment_eligible_subset_semantic_comparison_supported": True,
            "semantic_comparison_may_be_called_total_ranking": False,
            "public_assessment_classes_may_be_used_as_rank_order": False,
            "pareto_partial_order_may_be_called_total_ranking": False,
            "excluded_entities_may_be_assigned_bottom_rank": False,
            "missingness_may_be_used_as_tiebreaker": False,
            "arbitrary_coverage_threshold_selected": False,
            "explicit_subset_scope_disclosure_required": True,
        },
        "population": {
            "regulatory_universe": regulatory,
            "assessment_eligible": assessable,
            "assessment_not_eligible": regulatory - assessable,
            "ranking_preflight_candidates": candidates,
            "ranking_eligible": 0,
        },
        "coverage_diagnostic": market_coverage,
        "ordering_diagnostic": ordering,
        "blocking_reasons": {
            "full_market_total_ranking": full_market_blockers,
            "assessment_eligible_subset_total_ranking": subset_blockers,
        },
        "diagnostics": {
            "ranking_preflight_state_counts": dict(
                sorted(preflight_state_counts.items())
            ),
            "candidate_public_class_counts": dict(
                sorted(candidate_class_counts.items())
            ),
        },
        "closure_decision": {
            "ranking_eligibility_preflight_closed": True,
            "ranking_eligibility_gate_opened": False,
            "ranking_eligible": 0,
            "full_market_ranking_supported": False,
            "assessment_eligible_subset_total_ranking_supported": False,
            "semantic_comparison_of_assessment_eligible_subset_supported": True,
            "comparison_is_not_ranking": True,
            "next_methodological_decision": (
                "choose_scope_and_defensible_ordering_rule_or_keep_product_as_semantic_comparator"
            ),
        },
        "entities": rows,
    }


def main() -> None:
    assessment = json.loads(ASSESSMENT_PATH.read_text(encoding="utf-8"))
    stage1 = json.loads(STAGE1_PATH.read_text(encoding="utf-8"))
    stage2 = json.loads(STAGE2_PATH.read_text(encoding="utf-8"))
    coverage = json.loads(COVERAGE_PATH.read_text(encoding="utf-8"))
    payload = build_ranking_eligibility_preflight(
        assessment, stage1, stage2, coverage
    )
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "artifact": payload["artifact"],
                "status": payload["status"],
                "population": payload["population"],
                "coverage_diagnostic": payload["coverage_diagnostic"],
                "ordering_diagnostic": payload["ordering_diagnostic"],
                "blocking_reasons": payload["blocking_reasons"],
                "closure_decision": payload["closure_decision"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    print(f"written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
