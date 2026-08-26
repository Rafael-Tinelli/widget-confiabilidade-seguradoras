from __future__ import annotations

from api.v2.build_cross_pillar_assessment_semantic_contract import (
    build_semantic_contract,
)


def _inputs() -> tuple[dict, dict]:
    signatures = (
        ["F0|C0"] * 46
        + ["F0|C1"] * 14
        + ["F1|C0"] * 8
        + ["F1|C1"] * 8
        + ["F2|C0"] * 5
        + ["F2|C1"] * 4
    )
    state_by_signature = {
        "F0|C0": "no_current_core_adverse_signal",
        "F0|C1": "conduct_pressure_only",
        "F1|C0": "liquidity_pressure_only",
        "F1|C1": "liquidity_and_conduct_pressure",
        "F2|C0": "capital_shortfall_without_conduct_pressure",
        "F2|C1": "capital_shortfall_and_conduct_pressure",
    }
    financial_by_level = {
        0: "core_indicators_without_current_shortfall",
        1: "capital_requirement_met_with_liquidity_pressure",
        2: "capital_requirement_shortfall_observed",
    }

    stage1_rows = []
    stage2_rows = []
    below_remaining = 41
    neutral_remaining = 18

    for index, signature in enumerate(signatures):
        f_level, c_level = (int(part[1]) for part in signature.split("|"))
        if c_level == 1:
            conduct_state = "above_expected_with_sufficient_evidence"
            persistence = (
                "persistent_above_expected"
                if index % 4 != 0
                else "episodic_or_sparse_above_expected"
            )
            trend = "no_clear_change"
        elif below_remaining:
            conduct_state = "below_expected_with_sufficient_evidence"
            below_remaining -= 1
            persistence = "persistent_below_expected"
            trend = "improving_pressure"
        else:
            conduct_state = "not_distinguishable_from_expected"
            neutral_remaining -= 1
            persistence = "not_distinguishable_from_expected"
            trend = "deteriorating_pressure"

        entity_id = f"fip:{index:06d}"
        stage1_rows.append(
            {
                "entity_id": entity_id,
                "fip_code": f"{index:06d}",
                "legal_name": f"Seguradora {index}",
                "joint_evidence_readiness": "joint_core_conclusive",
                "core_signature": signature,
                "financial": {
                    "core_state": financial_by_level[f_level],
                    "evidence_confidence": "established_core_history",
                    "operating_context": "balanced_persistent",
                },
                "conduct": {
                    "pressure_state": conduct_state,
                    "persistence": persistence,
                    "trend": trend,
                },
                "adverse_signature": {
                    "capital_shortfall": f_level == 2,
                    "liquidity_pressure": f_level == 1,
                    "conduct_above_expected": c_level == 1,
                },
            }
        )
        stage2_rows.append(
            {
                "entity_id": entity_id,
                "matrix_state": state_by_signature[signature],
                # Deliberately inject an experimental Stage-2 qualifier leak.
                # The semantic contract must derive public qualifiers from the
                # final Stage-1 Conduct conclusion instead of trusting it.
                "qualifiers": {"conduct_trend": "conduct_pressure_deteriorating"},
            }
        )

    incomplete_conduct_states = (
        ["pressure_unavailable_not_comparable"] * 54
        + ["pressure_inconclusive_denominator_sensitivity"] * 6
        + ["pressure_unavailable_insufficient_temporal_coverage"] * 12
    )
    incomplete_financial_states = (
        ["capital_requirement_shortfall_observed"] * 5
        + ["capital_requirement_met_with_liquidity_pressure"] * 5
        + ["core_indicators_without_current_shortfall"] * 60
        + ["core_financial_signal_unavailable"] * 2
    )

    for offset in range(72):
        index = len(signatures) + offset
        entity_id = f"fip:{index:06d}"
        financial_state = incomplete_financial_states[offset]
        conduct_state = incomplete_conduct_states[offset]
        if financial_state == "core_financial_signal_unavailable":
            readiness = "financial_core_incomplete"
        elif conduct_state == "pressure_unavailable_not_comparable":
            readiness = "conduct_not_comparable"
        elif conduct_state == "pressure_inconclusive_denominator_sensitivity":
            readiness = "conduct_denominator_sensitive"
        else:
            readiness = "conduct_insufficient_temporal_coverage"

        stage1_rows.append(
            {
                "entity_id": entity_id,
                "fip_code": f"{index:06d}",
                "legal_name": f"Seguradora {index}",
                "joint_evidence_readiness": readiness,
                "core_signature": None,
                "financial": {
                    "core_state": financial_state,
                    "evidence_confidence": "established_core_history",
                    "operating_context": "balanced_persistent",
                },
                "conduct": {
                    "pressure_state": conduct_state,
                    "persistence": "episodic_or_sparse_above_expected",
                    "trend": "no_clear_change",
                },
                "adverse_signature": {
                    "capital_shortfall": (
                        financial_state == "capital_requirement_shortfall_observed"
                    ),
                    "liquidity_pressure": (
                        financial_state
                        == "capital_requirement_met_with_liquidity_pressure"
                    ),
                    "conduct_above_expected": False,
                },
            }
        )
        stage2_rows.append(
            {
                "entity_id": entity_id,
                "matrix_state": None,
                "qualifiers": {
                    "conduct_persistence": "conduct_adverse_episodic_or_sparse",
                    "conduct_trend": "conduct_pressure_no_clear_change",
                },
            }
        )

    state_counts = {
        "no_current_core_adverse_signal": 46,
        "conduct_pressure_only": 14,
        "liquidity_pressure_only": 8,
        "liquidity_and_conduct_pressure": 8,
        "capital_shortfall_without_conduct_pressure": 5,
        "capital_shortfall_and_conduct_pressure": 4,
        "evidence_incomplete_for_joint_assessment": 72,
    }
    stage1 = {
        "status": "cross_pillar_calibration_stage_1_diagnostic",
        "scoring": "forbidden_in_this_artifact",
        "entities": stage1_rows,
    }
    stage2 = {
        "status": "cross_pillar_architecture_stage_2_experiment",
        "scoring": "forbidden_in_this_artifact",
        "ranking": "forbidden_in_this_artifact",
        "matrix_architecture": {"state_counts": state_counts},
        "coverage_constraint": {
            "joint_conclusive_entities": 85,
            "joint_incomplete_entities": 72,
        },
        "entities": stage2_rows,
    }
    return stage1, stage2


def test_semantic_contract_closes_for_all_157_without_opening_ranking() -> None:
    stage1, stage2 = _inputs()
    payload = build_semantic_contract(stage1, stage2)

    assert payload["status"] == "cross_pillar_assessment_semantic_contract_closed"
    assert payload["population"] == {
        "regulatory_universe": 157,
        "semantic_public_assessment_supported": 85,
        "joint_core_incomplete": 72,
    }
    assert payload["diagnostics"]["public_class_counts"] == {
        "attention": 30,
        "evidence_incomplete": 72,
        "favorable_reading": 46,
        "prudential_warning": 9,
    }
    assert payload["closure_decision"]["formal_assessment_eligibility_gate_opened"] is False
    assert payload["closure_decision"]["formal_ranking_eligibility_gate_opened"] is False
    assert payload["scoring"] == "forbidden_in_this_artifact"
    assert payload["ranking"] == "forbidden_in_this_artifact"


def test_incomplete_joint_assessment_never_hides_available_capital_warning() -> None:
    stage1, stage2 = _inputs()
    payload = build_semantic_contract(stage1, stage2)

    incomplete_capital = [
        row
        for row in payload["entities"]
        if row["assessment_completeness"] == "joint_core_incomplete"
        and any(
            alert["kind"] == "prudential_capital_warning"
            for alert in row["mandatory_available_alerts"]
        )
    ]
    assert len(incomplete_capital) == 5
    assert all(
        row["public_assessment"]["title"] == "Avaliação conjunta incompleta"
        for row in incomplete_capital
    )
    assert payload["diagnostics"]["incomplete_available_alert_counts"] == {
        "liquidity_attention": 5,
        "prudential_capital_warning": 5,
    }


def test_conduct_detail_is_preserved_without_turning_below_expected_into_bonus() -> None:
    stage1, stage2 = _inputs()
    payload = build_semantic_contract(stage1, stage2)

    favorable = [
        row
        for row in payload["entities"]
        if row["matrix_state"] == "no_current_core_adverse_signal"
    ]
    details = {row["available_pillar_reading"]["conduct"] for row in favorable}
    assert details == {"abaixo_do_esperado", "sem_diferenca_clara"}
    assert all(row["public_assessment"]["title"] == "Leitura central favorável" for row in favorable)


def test_public_conduct_qualifiers_only_exist_for_final_adverse_pressure() -> None:
    stage1, stage2 = _inputs()
    payload = build_semantic_contract(stage1, stage2)
    source_by_id = {row["entity_id"]: row for row in stage1["entities"]}

    for row in payload["entities"]:
        source_state = source_by_id[row["entity_id"]]["conduct"]["pressure_state"]
        has_qualifier = bool(
            row["qualifiers"]["conduct_persistence"]
            or row["qualifiers"]["conduct_trend"]
        )
        assert has_qualifier is (source_state == "above_expected_with_sufficient_evidence")
