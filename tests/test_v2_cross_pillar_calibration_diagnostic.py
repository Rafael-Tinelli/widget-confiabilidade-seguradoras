from __future__ import annotations

from copy import deepcopy

from api.v2.build_cross_pillar_calibration_diagnostic import (
    build_cross_pillar_diagnostic,
)


def _financial_payload() -> dict:
    entities = []
    for index in range(157):
        entity_id = f"fip:{index:06d}"
        entities.append(
            {
                "entity_id": entity_id,
                "fip_code": f"{index:06d}",
                "legal_name": f"Seguradora {index}",
                "core_financial_signal": "core_indicators_without_current_shortfall",
                "evidence_confidence": "established_core_history",
                "operating_context": {"signal": "balanced_persistent"},
            }
        )
    return {
        "status": "financial_methodology_closed_for_signal_design",
        "scoring": "forbidden_in_this_artifact",
        "entities": entities,
    }


def _candidate(entity_id: str, state: str) -> dict:
    return {
        "entity_id": entity_id,
        "pressure_conclusion": {"state": state},
        "direct_pressure": {
            "persistence": {"state": "not_distinguishable_from_expected"},
            "trend": {"state": "no_clear_change"},
        },
    }


def _non_comparable(entity_id: str) -> dict:
    return {
        "entity_id": entity_id,
        "pressure_conclusion": {"state": "pressure_unavailable_not_comparable"},
    }


def _conduct_payload() -> dict:
    candidates = [
        _candidate(f"fip:{index:06d}", "not_distinguishable_from_expected")
        for index in range(103)
    ]
    non_comparable = [
        _non_comparable(f"fip:{index:06d}") for index in range(103, 157)
    ]
    return {
        "status": "conduct_methodology_closed_for_signal_design",
        "scoring": "forbidden_in_this_artifact",
        "candidate_entities": candidates,
        "non_comparable_entities": non_comparable,
    }


def _entity(payload: dict, entity_id: str) -> dict:
    return next(row for row in payload["entities"] if row["entity_id"] == entity_id)


def test_stage1_preserves_full_universe_without_scoring() -> None:
    payload = build_cross_pillar_diagnostic(_financial_payload(), _conduct_payload())

    assert payload["population"] == {
        "regulatory_universe": 157,
        "joint_core_conclusive": 103,
        "joint_core_not_conclusive": 54,
    }
    assert payload["scoring"] == "forbidden_in_this_artifact"
    assert payload["ranking"] == "forbidden_in_this_artifact"
    assert (
        payload["architecture_candidates"]["weighted_continuous_score"]["stage_1_status"]
        == "not_supported_by_closed_contracts_alone"
    )
    assert (
        payload["diagnostics"]["joint_evidence_readiness_counts"]
        ["conduct_not_comparable"]
        == 54
    )


def test_below_expected_is_not_rewarded_over_indistinguishable() -> None:
    conduct = _conduct_payload()
    conduct["candidate_entities"][0]["pressure_conclusion"]["state"] = (
        "below_expected_with_sufficient_evidence"
    )
    conduct["candidate_entities"][1]["pressure_conclusion"]["state"] = (
        "not_distinguishable_from_expected"
    )
    conduct["candidate_entities"][2]["pressure_conclusion"]["state"] = (
        "above_expected_with_sufficient_evidence"
    )
    conduct["candidate_entities"][2]["direct_pressure"]["persistence"]["state"] = (
        "persistent_above_expected"
    )

    payload = build_cross_pillar_diagnostic(_financial_payload(), conduct)
    below = _entity(payload, "fip:000000")
    indistinguishable = _entity(payload, "fip:000001")
    above = _entity(payload, "fip:000002")

    assert below["conduct"]["ordinal_adverse_level_for_stage1_only"] == 0
    assert indistinguishable["conduct"]["ordinal_adverse_level_for_stage1_only"] == 0
    assert below["conduct"]["below_expected_is_positive_merit"] is False
    assert below["core_coordinate"] == indistinguishable["core_coordinate"] == [0, 0]
    assert above["core_coordinate"] == [0, 1]
    assert below["pareto_front_stage1"] == 1
    assert indistinguishable["pareto_front_stage1"] == 1
    assert above["pareto_front_stage1"] > 1


def test_incomplete_evidence_never_becomes_neutral_coordinate() -> None:
    financial = deepcopy(_financial_payload())
    conduct = deepcopy(_conduct_payload())

    financial["entities"][0]["core_financial_signal"] = (
        "core_financial_signal_unavailable"
    )
    conduct["candidate_entities"][1]["pressure_conclusion"]["state"] = (
        "pressure_inconclusive_denominator_sensitivity"
    )

    payload = build_cross_pillar_diagnostic(financial, conduct)
    financial_missing = _entity(payload, "fip:000000")
    conduct_inconclusive = _entity(payload, "fip:000001")

    assert financial_missing["joint_evidence_readiness"] == "financial_core_incomplete"
    assert financial_missing["core_coordinate"] is None
    assert financial_missing["pareto_front_stage1"] is None

    assert conduct_inconclusive["joint_evidence_readiness"] == "conduct_denominator_sensitive"
    assert conduct_inconclusive["core_coordinate"] is None
    assert conduct_inconclusive["pareto_front_stage1"] is None

    assert payload["closed_contract_preservation"]["missing_pillar_treated_as_neutral"] is False
