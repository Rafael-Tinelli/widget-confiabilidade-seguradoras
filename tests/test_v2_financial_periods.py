from __future__ import annotations

import pytest

from api.v2.financial_periods import (
    FinancialPeriodMaturityError,
    apply_mature_financial_reference_period,
    build_financial_period_maturity,
)


def _entity(capital_by_period: dict[int, tuple[float | None, float | None]]) -> dict:
    periods = set(capital_by_period)
    return {
        "capital_history": {
            period: {
                "period": period,
                "pla_adjusted": pla,
                "cmr": cmr,
            }
            for period, (pla, cmr) in capital_by_period.items()
        },
        "balance_periods": set(periods),
        "insurance_operation_periods": set(periods),
    }


def test_latest_immature_period_rolls_back_to_latest_mature_common_period() -> None:
    source = {
        "reference_periods": {
            "capital": 202606,
            "balance": 202606,
            "insurance_operations": 202606,
        },
        "entities": {
            "000001": _entity({202605: (100.0, 80.0), 202606: (100.0, 80.0)}),
            "000002": _entity({202605: (100.0, 80.0), 202606: (0.0, 0.0)}),
            "000003": _entity({202605: (100.0, 80.0), 202606: (0.0, 0.0)}),
            "000004": _entity({202605: (100.0, 80.0), 202606: (0.0, 0.0)}),
        },
    }

    maturity = build_financial_period_maturity(source, min_relative_coverage=0.95)

    assert maturity["capital_derivable_counts"] == {"202605": 4, "202606": 1}
    assert maturity["latest_common_period"] == 202606
    assert maturity["selected_period"] == 202605
    assert maturity["status"] == "latest_common_immature_rolled_back"

    aligned = apply_mature_financial_reference_period(
        source,
        min_relative_coverage=0.95,
    )
    assert aligned["reference_periods"] == {
        "capital": 202605,
        "balance": 202605,
        "insurance_operations": 202605,
    }
    assert aligned["period_maturity"]["selected_period"] == 202605


def test_latest_common_period_is_kept_when_coverage_remains_near_peak() -> None:
    source = {
        "reference_periods": {
            "capital": 202606,
            "balance": 202606,
            "insurance_operations": 202606,
        },
        "entities": {
            "000001": _entity({202605: (100.0, 80.0), 202606: (100.0, 80.0)}),
            "000002": _entity({202605: (100.0, 80.0), 202606: (100.0, 80.0)}),
            "000003": _entity({202605: (100.0, 80.0), 202606: (100.0, 80.0)}),
        },
    }

    maturity = build_financial_period_maturity(source)

    assert maturity["selected_period"] == 202606
    assert maturity["status"] == "latest_common_mature"


def test_period_maturity_requires_common_financial_period() -> None:
    source = {
        "reference_periods": {
            "capital": 202606,
            "balance": 202606,
            "insurance_operations": 202605,
        },
        "entities": {
            "000001": {
                "capital_history": {202606: {"pla_adjusted": 100.0, "cmr": 80.0}},
                "balance_periods": {202606},
                "insurance_operation_periods": {202605},
            }
        },
    }

    with pytest.raises(FinancialPeriodMaturityError):
        build_financial_period_maturity(source)
