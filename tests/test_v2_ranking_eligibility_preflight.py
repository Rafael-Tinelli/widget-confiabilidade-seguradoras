from __future__ import annotations

import copy

import pytest

from api.v2.build_ranking_eligibility_preflight import (
    build_ranking_eligibility_preflight,
)


def _assessment() -> dict:
    rows = []
    classes = ["favorable_reading", "attention", "prudential_warning"]
    for index, public_class in enumerate(classes, start=1):
        rows.append(
            {
                "entity_id": f"fip:00000{index}",
                "fip_code": f"00000{index}",
                "legal_name": f"SEGURADORA {index}",
                "assessment_eligible": True,
                "ranking_eligible": False,
                "comparison_cohort": None,
                "semantic_assessment": {"public_class": public_class},
            }
        )
    rows.append(
        {
            "entity_id": "fip:000004",
            "fip_code": "000004",
            "legal_name": "SEGURADORA 4",
            "assessment_eligible": False,
            "ranking_eligible": False,
            "comparison_cohort": None,
            "semantic_assessment": {"public_class": "evidence_incomplete"},
        }
    )
    return {
        "artifact": "v2_assessment_eligibility_contract",
        "version": "test",
        "status": "assessment_eligibility_contract_closed",
        "scoring": "forbidden_in_this_artifact",
        "ranking": "forbidden_in_this_artifact",
        "population": {
            "regulatory_universe": 4,
            "assessment_eligible": 3,
            "assessment_not_eligible": 1,
            "ranking_eligible": 0,
        },
        "closure_decision": {
            "assessment_eligibility_gate_opened": True,
            "ranking_eligibility_gate_opened": False,
        },
        "entities": rows,
    }


def _stage1() -> dict:
    return {
        "artifact": "v2_cross_pillar_calibration_diagnostic",
        "version": "test",
        "status": "cross_pillar_calibration_stage_1_diagnostic",
        "scoring": "forbidden_in_this_artifact",
        "ranking": "forbidden_in_this_artifact",
        "population": {
            "regulatory_universe": 4,
            "joint_core_conclusive": 3,
            "joint_core_not_conclusive": 1,
        },
        "diagnostics": {
            "safe_core_signature_resolution": {
                "unique_groups": 2,
                "largest_group": 2,
                "groups_with_more_than_one_entity": 1,
                "entities_in_tied_groups": 2,
                "counts": {"F0|C0": 2, "F1|C0": 1},
            },
            "pareto_partial_order": {
                "pairwise_orderability": {
                    "entity_count": 3,
                    "pair_count": 3,
                    "strictly_comparable_pairs": 1,
                    "tied_pairs": 1,
                    "incomparable_pairs": 1,
                }
            },
        },
    }


def _stage2() -> dict:
    return {
        "artifact": "v2_cross_pillar_architecture_experiment",
        "version": "test",
        "status": "cross_pillar_architecture_stage_2_experiment",
        "scoring": "forbidden_in_this_artifact",
        "ranking": "forbidden_in_this_artifact",
        "matrix_architecture": {
            "properties": {"pretends_total_order": False}
        },
        "normative_tradeoffs": {"tradeoff_entity_pairs": 1},
        "scenario_comparison": {
            "pairwise_scenario_disagreement": {
                "financial_vs_conduct_lexicographic": 1
            }
        },
        "coverage_constraint": {
            "joint_conclusive_entities": 3,
            "full_market_ranking_supported": False,
        },
        "architecture_decision": {
            "continuous_weighted_score_selected": False,
            "lexicographic_total_order_selected": False,
            "pareto_front_number_selected_as_public_tier": False,
            "capital_gate_total_order_selected": False,
        },
    }


def _coverage() -> dict:
    return {
        "artifact": "v2_cross_pillar_coverage_audit",
        "version": "test",
        "status": "cross_pillar_market_coverage_audit",
        "scoring": "forbidden_in_this_artifact",
        "ranking": "forbidden_in_this_artifact",
        "coverage": {
            "joint_core_conclusive": {
                "entity_count": 3,
                "positive_premium_share": 0.70,
                "complaint_share": 0.55,
            },
            "joint_core_incomplete": {
                "entity_count": 1,
                "positive_premium_share": 0.30,
                "complaint_share": 0.45,
            },
        },
        "excluded_materiality": {
            "top_10_incomplete_positive_premium_share_of_universe": 0.25
        },
        "interpretation": {
            "full_market_representativeness_established": False,
            "subset_comparison_requires_explicit_coverage_disclosure": True,
        },
    }


def _build() -> dict:
    return build_ranking_eligibility_preflight(
        _assessment(), _stage1(), _stage2(), _coverage()
    )


def test_preflight_keeps_ranking_gate_closed_but_preserves_comparison():
    payload = _build()

    assert payload["population"] == {
        "regulatory_universe": 4,
        "assessment_eligible": 3,
        "assessment_not_eligible": 1,
        "ranking_preflight_candidates": 3,
        "ranking_eligible": 0,
    }
    assert payload["closure_decision"]["ranking_eligibility_gate_opened"] is False
    assert (
        payload["closure_decision"][
            "semantic_comparison_of_assessment_eligible_subset_supported"
        ]
        is True
    )
    assert payload["closure_decision"]["comparison_is_not_ranking"] is True


def test_adverse_result_does_not_remove_ranking_preflight_candidate():
    payload = _build()
    candidates = {
        row["public_class_preserved_for_diagnostics_only"]
        for row in payload["entities"]
        if row["ranking_preflight_candidate"]
    }

    assert candidates == {"favorable_reading", "attention", "prudential_warning"}
    assert payload["diagnostics"]["candidate_public_class_counts"] == {
        "attention": 1,
        "favorable_reading": 1,
        "prudential_warning": 1,
    }


def test_ineligible_assessment_is_not_silently_sent_to_bottom_of_ranking():
    payload = _build()
    blocked = [
        row for row in payload["entities"] if not row["assessment_eligible"]
    ]

    assert len(blocked) == 1
    assert blocked[0]["ranking_preflight_candidate"] is False
    assert blocked[0]["ranking_eligible"] is False
    assert blocked[0]["ranking_position"] is None
    assert blocked[0]["comparison_cohort"] is None
    assert payload["claim_contract"]["excluded_entities_may_be_assigned_bottom_rank"] is False


def test_full_market_and_subset_ranking_fail_for_different_reasons():
    payload = _build()
    full = payload["blocking_reasons"]["full_market_total_ranking"]
    subset = payload["blocking_reasons"][
        "assessment_eligible_subset_total_ranking"
    ]

    assert "assessment_does_not_cover_full_regulatory_universe" in full
    assert "full_market_representativeness_not_established" in full
    assert "assessment_does_not_cover_full_regulatory_universe" not in subset
    assert "no_approved_total_order_rule" in subset
    assert "normative_cross_pillar_tradeoffs_unresolved" in subset
    assert "within_state_order_not_supported" in subset


def test_complete_scope_without_ordering_still_does_not_authorize_ranking():
    assessment = _assessment()
    assessment["entities"] = assessment["entities"][:3]
    assessment["population"] = {
        "regulatory_universe": 3,
        "assessment_eligible": 3,
        "assessment_not_eligible": 0,
        "ranking_eligible": 0,
    }
    stage1 = _stage1()
    stage1["population"] = {
        "regulatory_universe": 3,
        "joint_core_conclusive": 3,
        "joint_core_not_conclusive": 0,
    }
    coverage = _coverage()
    coverage["coverage"]["joint_core_incomplete"] = {
        "entity_count": 0,
        "positive_premium_share": 0.0,
        "complaint_share": 0.0,
    }
    coverage["coverage"]["joint_core_conclusive"]["positive_premium_share"] = 1.0
    coverage["coverage"]["joint_core_conclusive"]["complaint_share"] = 1.0
    coverage["interpretation"]["full_market_representativeness_established"] = True

    payload = build_ranking_eligibility_preflight(
        assessment, stage1, _stage2(), coverage
    )

    assert payload["coverage_diagnostic"]["full_market_scope_complete"] is True
    assert payload["claim_contract"]["full_market_total_ranking_supported"] is False
    assert "no_approved_total_order_rule" in payload["blocking_reasons"][
        "full_market_total_ranking"
    ]


def test_preflight_refuses_to_silently_open_when_future_inputs_support_total_order():
    assessment = _assessment()
    assessment["entities"] = assessment["entities"][:3]
    assessment["population"] = {
        "regulatory_universe": 3,
        "assessment_eligible": 3,
        "assessment_not_eligible": 0,
        "ranking_eligible": 0,
    }
    stage1 = _stage1()
    stage1["population"] = {
        "regulatory_universe": 3,
        "joint_core_conclusive": 3,
        "joint_core_not_conclusive": 0,
    }
    stage1["diagnostics"]["safe_core_signature_resolution"].update(
        {
            "unique_groups": 3,
            "largest_group": 1,
            "groups_with_more_than_one_entity": 0,
            "entities_in_tied_groups": 0,
        }
    )
    stage1["diagnostics"]["pareto_partial_order"]["pairwise_orderability"] = {
        "entity_count": 3,
        "pair_count": 3,
        "strictly_comparable_pairs": 3,
        "tied_pairs": 0,
        "incomparable_pairs": 0,
    }
    stage2 = _stage2()
    stage2["normative_tradeoffs"]["tradeoff_entity_pairs"] = 0
    stage2["scenario_comparison"]["pairwise_scenario_disagreement"][
        "financial_vs_conduct_lexicographic"
    ] = 0
    stage2["architecture_decision"]["lexicographic_total_order_selected"] = True
    coverage = _coverage()
    coverage["coverage"]["joint_core_incomplete"] = {
        "entity_count": 0,
        "positive_premium_share": 0.0,
        "complaint_share": 0.0,
    }
    coverage["coverage"]["joint_core_conclusive"]["positive_premium_share"] = 1.0
    coverage["coverage"]["joint_core_conclusive"]["complaint_share"] = 1.0
    coverage["interpretation"]["full_market_representativeness_established"] = True

    with pytest.raises(ValueError, match="formal ranking contract is required"):
        build_ranking_eligibility_preflight(assessment, stage1, stage2, coverage)


def test_population_mismatch_fails_closed():
    stage2 = copy.deepcopy(_stage2())
    stage2["coverage_constraint"]["joint_conclusive_entities"] = 2

    with pytest.raises(ValueError, match="Stage 2 conclusive population mismatch"):
        build_ranking_eligibility_preflight(
            _assessment(), _stage1(), stage2, _coverage()
        )
