from __future__ import annotations

import math

from api.v2.ilpl_experiment import (
    SURVIVAL_CRITERIA,
    SURVIVAL_CRITERIA_VERSION,
    calculate_ilpl_observation,
    evaluate_survival,
)


def test_ilpl_formula_uses_current_and_prior_december_average_equity() -> None:
    current = {518: 20.0, 3333: 120.0}
    prior_december = {3333: 80.0}

    result = calculate_ilpl_observation(current, prior_december)

    assert result["state"] == "derivable"
    assert result["average_equity"] == 100.0
    assert math.isclose(result["value"], 0.20)


def test_ilpl_is_not_annualized() -> None:
    current = {518: 10.0, 3333: 100.0}
    prior_december = {3333: 100.0}

    result = calculate_ilpl_observation(current, prior_december)

    assert result["value"] == 0.10


def test_missing_prior_december_equity_is_not_imputed() -> None:
    result = calculate_ilpl_observation({518: 10.0, 3333: 100.0}, {})

    assert result["state"] == "missing_components"
    assert result["value"] is None
    assert result["missing_components"] == ["prior_december_equity_3333"]


def test_non_positive_average_equity_is_not_divided() -> None:
    result = calculate_ilpl_observation(
        {518: 10.0, 3333: -50.0},
        {3333: 40.0},
    )

    assert result["state"] == "non_positive_average_equity"
    assert result["value"] is None
    assert "average_equity_negative" in result["flags"]


def test_duplicate_formula_rows_invalidate_observation() -> None:
    result = calculate_ilpl_observation(
        {518: 10.0, 3333: 100.0},
        {3333: 100.0},
        duplicate_current_rows=1,
    )

    assert result["state"] == "source_duplicate_components"
    assert result["value"] is None


def _summary_for_survival() -> dict:
    return {
        "current_coverage": 0.95,
        "paired_prior_equivalent_coverage": 0.80,
        "same_month_rank_stability": {
            "summary": {"count": 3, "median_spearman": 0.75}
        },
        "year_end_rank_stability": {
            "summary": {"count": 2, "median_spearman": 0.72}
        },
        "sign_persistence": {"rate": 0.75},
        "scale_bias_abs_ilpl_vs_average_equity": {"spearman": -0.20},
        "redundancy": {
            "pla_cmr": {"spearman": 0.25},
            "ilt": {"spearman": -0.40},
        },
    }


def test_all_preregistered_hard_gates_must_pass() -> None:
    result = evaluate_survival(_summary_for_survival())

    assert result["criteria_version"] == SURVIVAL_CRITERIA_VERSION
    assert result["survives_as_independent_scoring_candidate"] is True
    assert result["failed_gates"] == []


def test_one_failed_gate_rejects_without_rescue_iteration() -> None:
    summary = _summary_for_survival()
    summary["same_month_rank_stability"]["summary"]["median_spearman"] = 0.69

    result = evaluate_survival(summary)

    assert result["survives_as_independent_scoring_candidate"] is False
    assert result["failed_gates"] == ["same_month_rank_stability"]
    assert result["verdict"] == "reject_independent_scoring_candidate_no_rescue_iteration"


def test_frozen_criteria_match_preregistered_contract() -> None:
    assert SURVIVAL_CRITERIA == {
        "current_coverage_min": 0.90,
        "paired_prior_equivalent_coverage_min": 0.75,
        "same_month_rank_stability_median_min": 0.70,
        "year_end_rank_stability_median_min": 0.70,
        "sign_persistence_min": 0.70,
        "scale_bias_abs_spearman_max": 0.30,
        "redundancy_abs_spearman_max": 0.50,
        "minimum_same_month_comparisons": 2,
        "minimum_year_end_comparisons": 2,
    }
