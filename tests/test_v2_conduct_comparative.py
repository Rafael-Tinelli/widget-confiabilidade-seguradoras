from __future__ import annotations

import pytest

from api.v2.conduct_comparative import (
    branch_mix_distance,
    expected_complaints,
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
