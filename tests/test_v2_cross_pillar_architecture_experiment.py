from __future__ import annotations

from api.v2.build_cross_pillar_architecture_experiment import build_architecture_experiment


def _stage1() -> dict:
    signatures = (
        ["F0|C0"] * 46
        + ["F0|C1"] * 14
        + ["F1|C0"] * 8
        + ["F1|C1"] * 8
        + ["F2|C0"] * 5
        + ["F2|C1"] * 4
    )
    entities = []
    for index in range(157):
        if index < len(signatures):
            signature = signatures[index]
            f_level, c_level = (int(part[1]) for part in signature.split("|"))
            conduct_above = c_level == 1
            entities.append(
                {
                    "entity_id": f"fip:{index:06d}",
                    "legal_name": f"Seguradora {index}",
                    "joint_evidence_readiness": "joint_core_conclusive",
                    "core_signature": signature,
                    "core_coordinate": [f_level, c_level],
                    "adverse_signature": {
                        "capital_shortfall": f_level == 2,
                        "liquidity_pressure": f_level == 1,
                        "conduct_above_expected": conduct_above,
                    },
                    "financial": {"operating_context": "balanced_persistent"},
                    "conduct": {
                        "persistence": (
                            "persistent_above_expected"
                            if conduct_above
                            else "not_distinguishable_from_expected"
                        ),
                        "trend": "no_clear_change",
                    },
                }
            )
        else:
            entities.append(
                {
                    "entity_id": f"fip:{index:06d}",
                    "legal_name": f"Seguradora {index}",
                    "joint_evidence_readiness": "conduct_not_comparable",
                    "core_signature": None,
                    "core_coordinate": None,
                    "adverse_signature": {},
                    "financial": {"operating_context": "balanced_persistent"},
                    "conduct": {"persistence": None, "trend": None},
                }
            )
    return {
        "status": "cross_pillar_calibration_stage_1_diagnostic",
        "scoring": "forbidden_in_this_artifact",
        "entities": entities,
    }


def _coverage() -> dict:
    return {
        "status": "cross_pillar_market_coverage_audit",
        "scoring": "forbidden_in_this_artifact",
        "coverage": {
            "joint_core_conclusive": {
                "entity_count": 85,
                "positive_premium_share": 0.70,
                "complaint_share": 0.54,
            },
            "joint_core_incomplete": {"entity_count": 72},
        },
    }


def test_matrix_preserves_six_exact_states_and_missingness() -> None:
    payload = build_architecture_experiment(_stage1(), _coverage())

    counts = payload["matrix_architecture"]["state_counts"]
    assert counts == {
        "capital_shortfall_and_conduct_pressure": 4,
        "capital_shortfall_without_conduct_pressure": 5,
        "conduct_pressure_only": 14,
        "evidence_incomplete_for_joint_assessment": 72,
        "liquidity_and_conduct_pressure": 8,
        "liquidity_pressure_only": 8,
        "no_current_core_adverse_signal": 46,
    }
    assert payload["scoring"] == "forbidden_in_this_artifact"
    assert payload["ranking"] == "forbidden_in_this_artifact"
    assert payload["matrix_architecture"]["properties"]["noncompensatory"] is True


def test_tradeoff_pairs_are_exposed_not_hidden_as_math() -> None:
    payload = build_architecture_experiment(_stage1(), _coverage())
    tradeoffs = payload["normative_tradeoffs"]

    assert tradeoffs["tradeoff_entity_pairs"] == 222
    counts = {
        tuple(item["left_coordinate"] + item["right_coordinate"]): item[
            "entity_pair_count"
        ]
        for item in tradeoffs["coordinate_pair_details"]
    }
    assert counts[(0, 1, 1, 0)] == 112
    assert counts[(0, 1, 2, 0)] == 70
    assert counts[(1, 1, 2, 0)] == 40
    assert (
        payload["scenario_comparison"]["pairwise_scenario_disagreement"]
        ["financial_vs_conduct_lexicographic"]
        == 222
    )


def test_persistence_only_qualifies_current_adverse_conduct() -> None:
    payload = build_architecture_experiment(_stage1(), _coverage())

    diagnostic = payload["adverse_qualifier_diagnostic"]
    assert diagnostic["conduct_above_expected_entities"] == 26
    assert diagnostic["persistence_counts"] == {"conduct_adverse_persistent": 26}
    assert (
        payload["architecture_decision"]["continuous_weighted_score_selected"]
        is False
    )
    assert (
        payload["architecture_decision"]["leading_public_assessment_candidate"]
        == "noncompensatory_state_matrix_with_adverse_qualifiers"
    )
    assert payload["coverage_constraint"]["full_market_ranking_supported"] is False
