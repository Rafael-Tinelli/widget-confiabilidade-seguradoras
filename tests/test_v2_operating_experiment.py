from __future__ import annotations

from api.v2.operating_experiment import (
    _capital_ratio,
    calculate_operating_observation,
    equivalent_month_periods,
    prior_year_end_periods,
)


def _values() -> dict[int, float]:
    return {
        4027: 100.0,
        7186: 0.0,
        6238: 0.0,
        6256: 0.0,
        11232: -20.0,
        11248: 0.0,
        11237: -10.0,
        11249: 0.0,
        6202: -5.0,
        11231: 0.0,
        6261: 0.0,
        11238: -2.0,
        11250: 0.0,
        4069: -3.0,
        4070: 0.0,
        6322: 10.0,
    }


def test_ic_and_ica_follow_signed_susep_terms() -> None:
    values = _values()
    ic = calculate_operating_observation(values, "IC")
    ica = calculate_operating_observation(values, "ICA")

    assert ic["state"] == "derivable"
    assert ic["value"] == 0.4
    assert ic["operating_base"] == 100.0
    assert ic["cost_total"] == 40.0
    assert sum(ic["components"].values()) == 0.4

    assert ica["state"] == "derivable"
    assert ica["value"] == 40.0 / 110.0
    assert ica["denominator"] == 110.0


def test_negative_operating_base_is_not_interpreted_as_good_ratio() -> None:
    values = _values()
    values[4027] = -100.0

    result = calculate_operating_observation(values, "IC")

    assert result["state"] == "non_positive_operating_base"
    assert result["value"] is None
    assert "operating_base_negative" in result["flags"]


def test_ica_negative_denominator_is_unavailable_not_extreme_ratio() -> None:
    values = _values()
    values[6322] = -150.0

    result = calculate_operating_observation(values, "ICA")

    assert result["state"] == "non_positive_denominator"
    assert result["value"] is None
    assert "negative_financial_result" in result["flags"]
    assert "denominator_negative" in result["flags"]


def test_missing_component_is_not_imputed_to_zero() -> None:
    values = _values()
    del values[11237]

    result = calculate_operating_observation(values, "IC")

    assert result["state"] == "missing_components"
    assert result["value"] is None
    assert result["missing_cmpids"] == [11237]


def test_operating_capital_redundancy_uses_new_pla_not_plajustado() -> None:
    source = {
        "capital_history": {
            202606: {
                "new_pla": 110.0,
                "pla_adjusted": 70.0,
                "cmr": 100.0,
            }
        }
    }

    assert _capital_ratio(source, 202606) == 1.10


def test_equivalent_horizons_cross_years_without_monthly_comparison() -> None:
    assert equivalent_month_periods(202605, 4) == [202305, 202405, 202505, 202605]
    assert prior_year_end_periods(202605, 3) == [202312, 202412, 202512]
