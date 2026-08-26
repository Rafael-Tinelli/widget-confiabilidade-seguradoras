from __future__ import annotations

import pytest

from api.v2.build_conduct_credibility_diagnostic import (
    build_credibility_diagnostic,
)


def _entity(
    entity_id: str,
    complaints: int,
    direct: float,
    earned: float,
    monthly: list[tuple[int, float, float]],
) -> dict:
    return {
        "entity_id": entity_id,
        "fip_code": entity_id.split(":", 1)[1],
        "legal_name": entity_id,
        "complaints_12m": complaints,
        "premium_direct_12m": direct,
        "premium_earned_12m_diagnostic": earned,
        "monthly": [
            {
                "month": f"2026-0{index}",
                "complaints": row[0],
                "premium_direct": row[1],
                "premium_earned_diagnostic": row[2],
            }
            for index, row in enumerate(monthly, start=1)
        ],
    }


def test_credibility_guard_distinguishes_extreme_ratio_from_denominator_sensitivity() -> None:
    calibration = {
        "version": "upstream-test",
        "scoring": "forbidden_in_this_artifact",
        "ranking": "forbidden_in_this_artifact",
        "source": {"months": ["2026-01", "2026-02"]},
        "market_12m": {"complaints": 100, "premium_direct": 100.0},
        "entities": [
            _entity(
                "fip:000001",
                complaints=5,
                direct=1.0,
                earned=10.0,
                monthly=[(5, 0.0, 5.0), (0, 1.0, 5.0)],
            ),
            _entity(
                "fip:000002",
                complaints=95,
                direct=99.0,
                earned=90.0,
                monthly=[(45, 49.0, 45.0), (50, 50.0, 45.0)],
            ),
        ],
    }

    payload = build_credibility_diagnostic(calibration)
    by_id = {row["entity_id"]: row for row in payload["entities"]}
    small = by_id["fip:000001"]

    assert payload["scoring"] == "forbidden_in_this_artifact"
    assert payload["ranking"] == "forbidden_in_this_artifact"
    assert payload["methodology"]["shrinkage_applied"] is False
    assert payload["methodology"]["denominator_selected"] is False
    assert small["direct_candidate"]["expected_complaints"] == pytest.approx(1.0)
    assert small["direct_candidate"]["ratio"] == pytest.approx(5.0)
    assert (
        small["direct_candidate"]["familywise_exact_interval"]["state"]
        == "above_size_proportional_reference"
    )
    assert small["earned_diagnostic"]["ratio"] == pytest.approx(0.5)
    assert (
        small["denominator_sensitivity"]["raw_neutral_side_consistency"]
        == "crosses_neutral"
    )
    assert (
        small["temporal_overlap"]["premium_direct"]["state"]
        == "all_observed_complaints_outside_positive_premium_months"
    )
    assert (
        small["temporal_overlap"]["premium_direct"][
            "complaints_in_non_positive_premium_months"
        ]
        == 5
    )
    assert payload["diagnostics"]["temporal_overlap"][
        "entities_with_complaints_in_non_positive_direct_premium_months"
    ] == 1
    assert payload["diagnostics"]["denominator_sensitivity"][
        "raw_neutral_side_changes"
    ] == 1


def test_earned_diagnostic_uses_its_own_aligned_population() -> None:
    calibration = {
        "version": "upstream-test",
        "scoring": "forbidden_in_this_artifact",
        "ranking": "forbidden_in_this_artifact",
        "source": {"months": ["2026-01"]},
        "market_12m": {"complaints": 10, "premium_direct": 100.0},
        "entities": [
            _entity(
                "fip:000001",
                complaints=10,
                direct=50.0,
                earned=100.0,
                monthly=[(10, 50.0, 100.0)],
            ),
            _entity(
                "fip:000002",
                complaints=0,
                direct=50.0,
                earned=-5.0,
                monthly=[(0, 50.0, -5.0)],
            ),
        ],
    }

    payload = build_credibility_diagnostic(calibration)
    by_id = {row["entity_id"]: row for row in payload["entities"]}

    assert payload["population"]["direct_candidate_entities"] == 2
    assert payload["population"]["earned_diagnostic_entities"] == 1
    assert payload["population"]["earned_unavailable_entities"] == 1
    assert payload["market"]["earned_diagnostic"]["complaints"] == 10
    assert payload["market"]["earned_diagnostic"]["premium"] == pytest.approx(100.0)
    assert by_id["fip:000001"]["earned_diagnostic"]["ratio"] == pytest.approx(1.0)
    assert (
        by_id["fip:000002"]["earned_diagnostic"]["state"]
        == "unavailable_non_positive_annual_premium_earned"
    )
