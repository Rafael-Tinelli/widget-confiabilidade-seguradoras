from __future__ import annotations

from typing import Any

ZERO_MARKET_COMPLAINTS_STATE = "not_comparable_zero_market_complaints"
ZERO_MARKET_COMPLAINTS_REASON = "zero_market_complaints_no_pressure_baseline"
ZERO_MARKET_COMPLAINTS_POLICY = "month_preserved_but_excluded_from_pressure_not_neutral"


def zero_market_complaints_guard(
    market_complaints: int,
    market_premium: float,
) -> dict[str, Any] | None:
    """Return an explicit unavailable pressure state for a 0/0 complaint month.

    A month with positive comparable premium but zero complaints in the aligned
    market has no complaint-pressure baseline. It is evidence of zero observed
    market events, not evidence that every entity has neutral pressure.
    """
    if market_premium > 0 and market_complaints <= 0:
        return {
            "state": ZERO_MARKET_COMPLAINTS_STATE,
            "reason_code": ZERO_MARKET_COMPLAINTS_REASON,
            "expected_complaints": 0.0,
            "pressure_ratio": None,
        }
    return None
