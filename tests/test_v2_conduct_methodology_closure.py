from __future__ import annotations

import pytest

from api.v2.build_conduct_methodology_closure import (
    _baselines,
    _final_pressure_state,
    _series,
)


def _entity(
    entity_id: str,
    complaints: list[int],
    direct: list[float],
    earned: list[float],
) -> dict:
    return {
        "entity_id": entity_id,
        "monthly": [
            {
                "month": f"2025-{month:02d}" if month <= 6 else f"2026-{month - 6:02d}",
                "complaints": complaints[index],
                "premium_direct": direct[index],
                "premium_earned_diagnostic": earned[index],
            }
            for index, month in enumerate(range(1, 13))
        ],
    }


def test_temporal_alignment_excludes_complaints_without_positive_exposure() -> None:
    target = _entity(
        "fip:target",
        complaints=[5] + [0] * 11,
        direct=[0.0] + [10.0] * 11,
        earned=[10.0] * 12,
    )
    peer = _entity(
        "fip:peer",
        complaints=[95] + [10] * 11,
        direct=[100.0] * 12,
        earned=[100.0] * 12,
    )
    entities = [target, peer]
    direct_baselines = _baselines(entities, "premium_direct")

    series = _series(
        target,
        "premium_direct",
        direct_baselines,
        annual_alpha=0.05 / 2,
        monthly_alpha=0.05 / 12,
    )

    assert series["temporal_coverage"]["comparable_months"] == 11
    assert (
        series["temporal_coverage"][
            "complaints_excluded_from_pressure_in_non_comparable_months"
        ]
        == 5
    )
    assert series["annual"]["observed_complaints"] == 0
    assert series["annual"]["expected_complaints"] > 0
    assert series["annual"]["ratio"] == pytest.approx(0.0)
    assert series["monthly"][0]["state"] == "unavailable_non_positive_comparable_exposure"


def test_final_pressure_state_blocks_material_denominator_disagreement() -> None:
    direct = {
        "temporal_coverage": {"comparable_months": 12},
        "annual": {
            "uncertainty": {"state": "above_size_proportional_reference"}
        },
    }
    earned = {
        "annual": {
            "uncertainty": {
                "state": "not_distinguishable_from_size_proportional_reference"
            }
        }
    }

    state, _ = _final_pressure_state(direct, earned)
    assert state == "pressure_inconclusive_denominator_sensitivity"


def test_final_pressure_state_requires_temporal_coverage() -> None:
    direct = {
        "temporal_coverage": {"comparable_months": 4},
        "annual": {
            "uncertainty": {"state": "above_size_proportional_reference"}
        },
    }
    earned = {
        "annual": {
            "uncertainty": {"state": "above_size_proportional_reference"}
        }
    }

    state, _ = _final_pressure_state(direct, earned)
    assert state == "pressure_unavailable_insufficient_temporal_coverage"
