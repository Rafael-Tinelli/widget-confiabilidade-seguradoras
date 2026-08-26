from __future__ import annotations

import pytest

from api.v2.build_conduct_portfolio_mix_diagnostic import (
    build_portfolio_mix_diagnostic,
)


def _entity(
    entity_id: str,
    complaints: int,
    premium: float,
    expected: float,
    mix: dict[str, float],
) -> dict:
    return {
        "entity_id": entity_id,
        "fip_code": entity_id.split(":", 1)[1],
        "legal_name": entity_id,
        "complaints_12m": complaints,
        "premium_direct_12m": premium,
        "pressure_12m": {
            "expected_complaints": expected,
            "ratio": complaints / expected if expected > 0 else None,
        },
        "portfolio_12m": {
            "positive_branch_mix": mix,
            "positive_branch_count": len(mix),
            "hhi": sum(value * value for value in mix.values()),
            "top_branch_share": max(mix.values()),
            "distance_from_market_mix": 0.0,
        },
    }


def _credibility(entity_ids: list[str]) -> dict:
    return {
        "version": "cred-test",
        "scoring": "forbidden_in_this_artifact",
        "ranking": "forbidden_in_this_artifact",
        "entities": [
            {
                "entity_id": entity_id,
                "direct_candidate": {
                    "familywise_exact_interval": {
                        "state": "not_distinguishable_from_size_proportional_reference"
                    }
                },
                "temporal_overlap": {
                    "premium_direct": {
                        "complaints_in_non_positive_premium_months": 0
                    }
                },
                "denominator_sensitivity": {
                    "familywise_state_consistency": "same_state"
                },
            }
            for entity_id in entity_ids
        ],
    }


def test_portfolio_distance_and_local_population_are_aligned() -> None:
    entities = [
        _entity("fip:000001", 20, 20.0, 10.0, {"A": 1.0}),
        _entity("fip:000002", 10, 80.0, 40.0, {"A": 0.9, "B": 0.1}),
        _entity("fip:000003", 70, 100.0, 50.0, {"B": 1.0}),
    ]
    calibration = {
        "version": "cal-test",
        "scoring": "forbidden_in_this_artifact",
        "ranking": "forbidden_in_this_artifact",
        "entities": entities,
    }
    payload = build_portfolio_mix_diagnostic(
        calibration,
        _credibility([row["entity_id"] for row in entities]),
    )
    by_id = {row["entity_id"]: row for row in payload["entities"]}
    first = by_id["fip:000001"]

    assert payload["scoring"] == "forbidden_in_this_artifact"
    assert payload["ranking"] == "forbidden_in_this_artifact"
    assert payload["methodology"]["peer_groups_selected"] is False
    assert payload["methodology"]["distance_threshold_selected"] is False
    assert first["portfolio"]["nearest_distance"] == pytest.approx(0.1)

    point = next(
        row
        for row in first["peer_distance_curve"]
        if row["max_total_variation_distance"] == 0.1
    )
    local = point["local_aligned_pressure"]
    assert point["peer_count"] == 1
    assert local["group_complaints"] == 30
    assert local["group_premium_direct"] == pytest.approx(100.0)
    assert local["expected_complaints"] == pytest.approx(6.0)
    assert local["ratio"] == pytest.approx(20.0 / 6.0)


def test_nearest_observation_does_not_become_approved_peer_group() -> None:
    entities = [
        _entity("fip:000001", 0, 10.0, 1.0, {"A": 1.0}),
        _entity("fip:000002", 5, 10.0, 1.0, {"B": 1.0}),
        _entity("fip:000003", 5, 10.0, 1.0, {"C": 1.0}),
    ]
    calibration = {
        "version": "cal-test",
        "scoring": "forbidden_in_this_artifact",
        "ranking": "forbidden_in_this_artifact",
        "entities": entities,
    }
    payload = build_portfolio_mix_diagnostic(
        calibration,
        _credibility([row["entity_id"] for row in entities]),
    )
    first = next(row for row in payload["entities"] if row["entity_id"] == "fip:000001")

    assert first["portfolio"]["nearest_distance"] == pytest.approx(1.0)
    assert all(
        point["peer_count"] == 0 for point in first["peer_distance_curve"]
    )
    assert payload["diagnostics"]["peer_coverage_curve"][-1][
        "entities_with_at_least_1_peer"
    ] == 0
    assert (
        payload["methodology"]["guardrails"][0]
        == "nearest_entity_is_not_automatically_an_adequate_peer"
    )
