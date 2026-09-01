from __future__ import annotations

import pytest

from api.v2.build_conduct_methodology_closure import (
    ConductMethodologyClosureError,
    _baselines,
    _final_pressure_state,
    _series,
)


def _entity(
    entity_id: str,
    complaints: list[int],
    direct: list[float],
    earned: list[float | None],
) -> dict:
    months = [
        "2025-07",
        "2025-08",
        "2025-09",
        "2025-10",
        "2025-11",
        "2025-12",
        "2026-01",
        "2026-02",
        "2026-03",
        "2026-04",
        "2026-05",
        "2026-06",
    ]
    return {
        "entity_id": entity_id,
        "monthly": [
            {
                "month": month,
                "complaints": complaints[index],
                "premium_direct": direct[index],
                "premium_earned_diagnostic": earned[index],
            }
            for index, month in enumerate(months)
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


def test_baselines_reject_gapped_twelve_month_window() -> None:
    entity = _entity(
        "fip:gapped",
        complaints=[1] * 12,
        direct=[10.0] * 12,
        earned=[10.0] * 12,
    )
    entity["monthly"][5]["month"] = "2024-12"

    with pytest.raises(
        ConductMethodologyClosureError,
        match="consecutive calendar months",
    ):
        _baselines([entity], "premium_direct")


def test_final_pressure_state_blocks_material_denominator_disagreement() -> None:
    direct = {
        "temporal_coverage": {"comparable_months": 12},
        "annual": {
            "uncertainty": {"state": "above_size_proportional_reference"}
        },
    }
    earned = {
        "temporal_coverage": {"comparable_months": 12},
        "annual": {
            "uncertainty": {
                "state": "not_distinguishable_from_size_proportional_reference"
            }
        },
    }

    state, _ = _final_pressure_state(direct, earned)
    assert state == "pressure_inconclusive_denominator_sensitivity"


def test_sparse_earned_diagnostic_cannot_veto_complete_direct_signal() -> None:
    direct = {
        "temporal_coverage": {"comparable_months": 12},
        "annual": {
            "uncertainty": {"state": "above_size_proportional_reference"}
        },
    }
    earned = {
        "temporal_coverage": {"comparable_months": 4},
        "annual": {
            "uncertainty": {
                "state": "not_distinguishable_from_size_proportional_reference"
            }
        },
    }

    state, _ = _final_pressure_state(direct, earned)
    assert state == "above_expected_with_sufficient_evidence"


def test_final_pressure_state_requires_temporal_coverage() -> None:
    direct = {
        "temporal_coverage": {"comparable_months": 4},
        "annual": {
            "uncertainty": {"state": "above_size_proportional_reference"}
        },
    }
    earned = {
        "temporal_coverage": {"comparable_months": 12},
        "annual": {
            "uncertainty": {"state": "above_size_proportional_reference"}
        },
    }

    state, _ = _final_pressure_state(direct, earned)
    assert state == "pressure_unavailable_insufficient_temporal_coverage"
