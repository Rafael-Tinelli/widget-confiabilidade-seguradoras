from __future__ import annotations

import math

import pytest

from api.v2.conduct_comparative import (
    branch_mix_distance,
    comparable_market_totals,
    expected_complaints,
    exposure_comparability_state,
    persistence_diagnostics,
    pressure_ratio,
    shrunken_pressure_ratio,
)


def test_pressure_is_scale_adjusted_not_raw_volume() -> None:
    small = pressure_ratio(10, 100, 100, 1000)
    large = pressure_ratio(40, 400, 100, 1000)
    assert small == pytest.approx(1.0)
    assert large == pytest.approx(1.0)


def test_pressure_above_one_means_more_complaints_than_exposure_share_predicts() -> None:
    assert expected_complaints(200, 100, 1000) == pytest.approx(20.0)
    assert pressure_ratio(50, 200, 100, 1000) == pytest.approx(2.5)


def test_small_samples_are_pulled_more_toward_neutral() -> None:
    small = shrunken_pressure_ratio(4, 1, 10)
    large = shrunken_pressure_ratio(400, 100, 10)
    assert small == pytest.approx(14 / 11)
    assert large == pytest.approx(410 / 110)
    assert abs(small - 1.0) < abs(large - 1.0)


def test_branch_mix_distance_distinguishes_like_for_like_from_disjoint_mix() -> None:
    assert branch_mix_distance({1: 80, 2: 20}, {1: 800, 2: 200}) == pytest.approx(0.0)
    assert branch_mix_distance({1: 100}, {2: 100}) == pytest.approx(1.0)


def test_persistence_separates_level_from_direction() -> None:
    result = persistence_diagnostics([2.0, 2.0, 1.8, 1.5, 1.3, 1.2])
    assert result["above_neutral_months"] == 6
    assert result["longest_above_neutral_run"] == 6
    assert result["direction"] == "improving"
    assert result["scoring"] == "forbidden_in_diagnostic"


def test_invalid_exposure_never_generates_pressure() -> None:
    assert pressure_ratio(10, 0, 100, 1000) is None
    assert pressure_ratio(10, 100, 100, 0) is None


def test_complaints_without_exposure_are_evidence_conflict_not_adverse_signal() -> None:
    state = exposure_comparability_state(1367, 0)
    assert state == {
        "state": "complaints_without_comparable_exposure",
        "pressure_eligible": False,
        "reason_code": "complaint_exposure_mismatch_requires_investigation",
    }
    assert pressure_ratio(1367, 0, 82423, 1_000_000) is None


def test_no_complaints_and_no_exposure_is_not_a_bad_conduct_signal() -> None:
    state = exposure_comparability_state(0, 0)
    assert state["state"] == "no_comparable_exposure"
    assert state["pressure_eligible"] is False


def test_missing_exposure_is_distinct_from_zero_exposure() -> None:
    state = exposure_comparability_state(10, None)
    assert state["state"] == "exposure_unavailable"
    assert state["pressure_eligible"] is False


def test_non_finite_complaints_are_invalid_before_comparability() -> None:
    for value in (math.nan, math.inf, -math.inf):
        state = exposure_comparability_state(value, 100)
        assert state == {
            "state": "invalid_complaint_count",
            "pressure_eligible": False,
            "reason_code": "non_finite_observed_complaints",
        }
        assert pressure_ratio(value, 100, 100, 1000) is None


def test_market_totals_exclude_non_finite_complaints_without_contamination() -> None:
    market = comparable_market_totals(
        [
            {"entity": "valid", "complaints": 10, "exposure": 100},
            {"entity": "nan", "complaints": math.nan, "exposure": 200},
            {"entity": "inf", "complaints": math.inf, "exposure": 300},
        ]
    )

    assert market["state"] == "available"
    assert market["comparable_entities"] == 1
    assert market["market_complaints"] == pytest.approx(10.0)
    assert market["market_exposure"] == pytest.approx(100.0)
    assert math.isfinite(market["market_complaints"])
    assert math.isfinite(market["market_exposure"])
    assert market["excluded_by_state"] == {"invalid_complaint_count": 2}


def test_expected_and_pressure_reject_non_finite_market_inputs() -> None:
    assert expected_complaints(100, math.nan, 1000) is None
    assert expected_complaints(100, 10, math.inf) is None
    assert expected_complaints(math.inf, 10, 1000) is None
    assert pressure_ratio(10, 100, math.nan, 1000) is None
    assert pressure_ratio(10, 100, 10, math.inf) is None


def test_shrinkage_rejects_non_finite_inputs() -> None:
    assert shrunken_pressure_ratio(math.nan, 10, 2) is None
    assert shrunken_pressure_ratio(10, math.inf, 2) is None
    assert shrunken_pressure_ratio(10, 2, math.nan) is None


def test_market_totals_exclude_complaints_when_matching_exposure_is_unavailable() -> None:
    market = comparable_market_totals(
        [
            {"entity": "small", "complaints": 10, "exposure": 100},
            {"entity": "large", "complaints": 40, "exposure": 400},
            {"entity": "mismatch", "complaints": 1367, "exposure": 0},
        ]
    )
    assert market["state"] == "available"
    assert market["comparable_entities"] == 2
    assert market["market_complaints"] == pytest.approx(50.0)
    assert market["market_exposure"] == pytest.approx(500.0)
    assert market["excluded_by_state"] == {
        "complaints_without_comparable_exposure": 1
    }
    assert pressure_ratio(
        10,
        100,
        market["market_complaints"],
        market["market_exposure"],
    ) == pytest.approx(1.0)
    assert pressure_ratio(
        40,
        400,
        market["market_complaints"],
        market["market_exposure"],
    ) == pytest.approx(1.0)


def test_market_totals_exclude_zero_zero_entities_without_penalty() -> None:
    market = comparable_market_totals(
        [
            {"complaints": 5, "exposure": 50},
            {"complaints": 0, "exposure": 0},
        ]
    )
    assert market["comparable_entities"] == 1
    assert market["excluded_by_state"] == {"no_comparable_exposure": 1}
    assert market["market_complaints"] == pytest.approx(5.0)
    assert market["market_exposure"] == pytest.approx(50.0)
