from __future__ import annotations

from api.v2.build_cross_pillar_coverage_audit import build_cross_pillar_coverage_audit


def _stage1() -> dict:
    entities = []
    for index in range(157):
        entities.append(
            {
                "entity_id": f"fip:{index:06d}",
                "joint_evidence_readiness": (
                    "joint_core_conclusive" if index < 85 else "conduct_not_comparable"
                ),
            }
        )
    return {
        "status": "cross_pillar_calibration_stage_1_diagnostic",
        "scoring": "forbidden_in_this_artifact",
        "entities": entities,
    }


def _reconciliation() -> dict:
    entities = []
    for index in range(157):
        candidate = index < 103
        entities.append(
            {
                "entity_id": f"fip:{index:06d}",
                "legal_name": f"Seguradora {index}",
                "complaints_12m": 10,
                "insurance_exposure_12m": {"insurance_premium_direct": 100.0},
                "pressure_comparability": {
                    "state": (
                        "direct_one_to_one_candidate"
                        if candidate
                        else "hybrid_insurance_pension_requires_product_numerator"
                    ),
                    "pressure_eligible_candidate": candidate,
                },
            }
        )
    return {
        "scoring": "forbidden_in_this_artifact",
        "entities": entities,
    }


def test_audit_quantifies_joint_market_coverage() -> None:
    payload = build_cross_pillar_coverage_audit(_stage1(), _reconciliation())

    joint = payload["coverage"]["joint_core_conclusive"]
    incomplete = payload["coverage"]["joint_core_incomplete"]

    assert joint["entity_count"] == 85
    assert incomplete["entity_count"] == 72
    assert joint["positive_premium_share"] == 85 / 157
    assert incomplete["positive_premium_share"] == 72 / 157
    assert joint["complaint_share"] == 85 / 157
    assert payload["interpretation"]["full_market_representativeness_established"] is False


def test_negative_premium_does_not_subtract_market_footprint() -> None:
    stage1 = _stage1()
    reconciliation = _reconciliation()
    reconciliation["entities"][156]["insurance_exposure_12m"][
        "insurance_premium_direct"
    ] = -50.0

    payload = build_cross_pillar_coverage_audit(stage1, reconciliation)

    assert payload["universe"]["positive_direct_premium_12m"] == 15600.0
    assert payload["universe"]["signed_direct_premium_12m"] == 15550.0
    assert (
        payload["market_footprint_policy"]["positive_footprint_uses_max_premium_zero"]
        is True
    )


def test_pressure_candidate_coverage_is_separate_from_joint_conclusion() -> None:
    payload = build_cross_pillar_coverage_audit(_stage1(), _reconciliation())

    candidates = payload["coverage"]["conduct_pressure_candidates"]
    joint = payload["coverage"]["joint_core_conclusive"]

    assert candidates["entity_count"] == 103
    assert joint["entity_count"] == 85
    assert candidates["positive_premium_share"] > joint["positive_premium_share"]
    assert payload["scoring"] == "forbidden_in_this_artifact"
    assert payload["ranking"] == "forbidden_in_this_artifact"
