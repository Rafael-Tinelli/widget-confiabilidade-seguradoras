from __future__ import annotations

import pytest

from api.v2.build_conduct_methodology_closure import (
    ConductMethodologyClosureError,
    _assert_calibration_alignment,
    _baselines,
    _final_pressure_state,
    _series,
    _validate_unit_contract,
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

    coverage = series["temporal_coverage"]
    assert coverage["comparable_months"] == 11
    assert coverage["missing_exposure_months"] == 0
    assert coverage["non_positive_exposure_months"] == 1
    assert coverage["complaints_excluded_non_positive_exposure"] == 5
    assert coverage["complaints_excluded_from_pressure_in_non_comparable_months"] == 5
    assert series["annual"]["observed_complaints"] == 0
    assert series["annual"]["expected_complaints"] > 0
    assert series["annual"]["ratio"] == pytest.approx(0.0)
    assert series["monthly"][0]["state"] == "unavailable_non_positive_comparable_exposure"


def test_missing_earned_exposure_remains_missing_not_zero() -> None:
    target = _entity(
        "fip:target",
        complaints=[5] + [1] * 11,
        direct=[10.0] * 12,
        earned=[None] + [10.0] * 11,
    )
    peer = _entity(
        "fip:peer",
        complaints=[5] + [1] * 11,
        direct=[100.0] * 12,
        earned=[100.0] * 12,
    )
    baselines = _baselines([target, peer], "premium_earned_diagnostic")

    assert baselines["2025-07"]["missing_exposure_entities"] == 1
    assert baselines["2025-07"]["non_positive_exposure_entities"] == 0
    assert baselines["2025-07"]["comparable_entities"] == 1

    series = _series(
        target,
        "premium_earned_diagnostic",
        baselines,
        annual_alpha=0.05 / 2,
        monthly_alpha=0.05 / 12,
    )

    first = series["monthly"][0]
    coverage = series["temporal_coverage"]
    assert first["state"] == "unavailable_missing_comparable_exposure"
    assert first["reason_code"] == "missing_entity_premium"
    assert first["premium"] is None
    assert coverage["missing_exposure_months"] == 1
    assert coverage["non_positive_exposure_months"] == 0
    assert coverage["complaints_excluded_missing_exposure"] == 5
    assert coverage["complaints_excluded_non_positive_exposure"] == 0
    assert coverage["complaints_excluded_from_pressure_in_non_comparable_months"] == 5


def test_zero_market_complaint_month_is_unavailable_not_neutral() -> None:
    target = _entity(
        "fip:target",
        complaints=[0] + [1] * 11,
        direct=[10.0] * 12,
        earned=[10.0] * 12,
    )
    peer = _entity(
        "fip:peer",
        complaints=[0] + [1] * 11,
        direct=[10.0] * 12,
        earned=[10.0] * 12,
    )
    baselines = _baselines([target, peer], "premium_direct")

    series = _series(
        target,
        "premium_direct",
        baselines,
        annual_alpha=0.05 / 2,
        monthly_alpha=0.05 / 12,
    )

    assert series["monthly"][0]["state"] == "not_comparable_zero_market_complaints"
    assert series["monthly"][0]["expected_complaints"] == pytest.approx(0.0)
    assert series["monthly"][0]["pressure_ratio"] is None
    assert series["monthly"][0]["uncertainty"] is None
    assert series["temporal_coverage"]["comparable_months"] == 11
    assert series["temporal_coverage"]["zero_market_complaint_months"] == 1
    assert series["temporal_coverage"]["complaints_excluded_zero_market_complaints"] == 0
    assert series["annual"]["observed_complaints"] == 11
    assert series["annual"]["expected_complaints"] == pytest.approx(11.0)
    assert series["annual"]["ratio"] == pytest.approx(1.0)


def test_baselines_reject_fractional_complaint_count() -> None:
    entity = _entity(
        "fip:fractional",
        complaints=[1] * 12,
        direct=[10.0] * 12,
        earned=[10.0] * 12,
    )
    entity["monthly"][0]["complaints"] = 1.5

    with pytest.raises(ConductMethodologyClosureError, match="non-integer complaints"):
        _baselines([entity], "premium_direct")


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


def test_unit_contract_requires_brl_raw_ses_scale() -> None:
    calibration = {
        "denominator": {
            "currency": "BRL",
            "source_unit_label": "R$",
            "scale_factor_applied": 1.0,
        },
        "source": {
            "ses_currency": "BRL",
            "ses_source_unit_label": "R$",
            "ses_scale_factor_applied": 1.0,
            "ses_source_documentation_url": "https://example.invalid/official.rtf",
        },
    }

    contract = _validate_unit_contract(calibration)
    assert contract == {
        "currency": "BRL",
        "source_unit_label": "R$",
        "scale_factor_applied": 1.0,
        "source_documentation_url": "https://example.invalid/official.rtf",
    }

    calibration["denominator"]["scale_factor_applied"] = 1000.0
    with pytest.raises(
        ConductMethodologyClosureError,
        match="scale factor must be 1.0",
    ):
        _validate_unit_contract(calibration)


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


def test_calibration_alignment_includes_comparable_and_zero_month_counts() -> None:
    entity = {
        "entity_id": "fip:test",
        "pressure_12m": {
            "aggregation_policy": "sum_monthly_expected_then_observed_divided_by_expected",
            "observed_complaints": 11,
            "expected_complaints": 11.0,
            "ratio": 1.0,
            "comparable_months": 11,
            "zero_market_complaint_months": 1,
        },
    }
    direct = {
        "annual": {
            "observed_complaints": 11,
            "expected_complaints": 11.0,
            "ratio": 1.0,
        },
        "temporal_coverage": {
            "comparable_months": 11,
            "zero_market_complaint_months": 1,
        },
    }

    _assert_calibration_alignment(entity, direct)
    direct["temporal_coverage"]["comparable_months"] = 12
    with pytest.raises(
        ConductMethodologyClosureError,
        match="comparable-month mismatch",
    ):
        _assert_calibration_alignment(entity, direct)
