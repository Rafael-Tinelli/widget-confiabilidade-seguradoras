from __future__ import annotations

import math
from statistics import median
from typing import Any

COMPARATIVE_VERSION = "2.0-draft-conduct-comparative-2"


def exposure_comparability_state(
    observed_complaints: float,
    company_exposure: float | None,
) -> dict[str, Any]:
    """Classify whether an exposure denominator can support complaint pressure.

    Complaints with no positive comparable exposure are an evidence conflict,
    not an adverse conduct signal. They must never be converted into an
    infinite or extreme pressure ratio.
    """
    if observed_complaints < 0:
        return {
            "state": "invalid_complaint_count",
            "pressure_eligible": False,
            "reason_code": "negative_observed_complaints",
        }
    if company_exposure is None or not math.isfinite(float(company_exposure)):
        return {
            "state": "exposure_unavailable",
            "pressure_eligible": False,
            "reason_code": "comparable_exposure_unavailable",
        }
    exposure = float(company_exposure)
    if exposure <= 0:
        if observed_complaints > 0:
            return {
                "state": "complaints_without_comparable_exposure",
                "pressure_eligible": False,
                "reason_code": "complaint_exposure_mismatch_requires_investigation",
            }
        return {
            "state": "no_comparable_exposure",
            "pressure_eligible": False,
            "reason_code": "no_positive_exposure_in_comparison_window",
        }
    return {
        "state": "comparable",
        "pressure_eligible": True,
        "reason_code": None,
    }


def expected_complaints(
    company_exposure: float,
    market_complaints: float,
    market_exposure: float,
) -> float | None:
    if company_exposure < 0 or market_complaints < 0 or market_exposure <= 0:
        return None
    return float(market_complaints * company_exposure / market_exposure)


def pressure_ratio(
    observed_complaints: float,
    company_exposure: float,
    market_complaints: float,
    market_exposure: float,
) -> float | None:
    """Observed/expected complaint pressure; 1.0 means proportional to exposure."""
    comparability = exposure_comparability_state(observed_complaints, company_exposure)
    if not comparability["pressure_eligible"]:
        return None
    expected = expected_complaints(company_exposure, market_complaints, market_exposure)
    if expected is None or expected <= 0:
        return None
    ratio = observed_complaints / expected
    return float(ratio) if math.isfinite(ratio) else None


def shrunken_pressure_ratio(
    observed_complaints: float,
    expected_count: float,
    prior_strength: float,
) -> float | None:
    """Diagnostic credibility adjustment centered on neutral pressure 1.0.

    prior_strength is expressed in expected-complaint units. It is deliberately
    configurable and receives no scoring interpretation in this experiment.
    """
    if observed_complaints < 0 or expected_count <= 0 or prior_strength < 0:
        return None
    denominator = expected_count + prior_strength
    if denominator <= 0:
        return None
    return float((observed_complaints + prior_strength) / denominator)


def branch_mix(branch_exposure: dict[int | str, float]) -> dict[str, float]:
    positive = {
        str(branch): float(value)
        for branch, value in branch_exposure.items()
        if value is not None and float(value) > 0 and math.isfinite(float(value))
    }
    total = sum(positive.values())
    if total <= 0:
        return {}
    return {branch: value / total for branch, value in sorted(positive.items())}


def branch_mix_distance(
    left: dict[int | str, float],
    right: dict[int | str, float],
) -> float | None:
    """Total-variation distance between two branch mixes, in the [0, 1] range."""
    left_mix = branch_mix(left)
    right_mix = branch_mix(right)
    if not left_mix or not right_mix:
        return None
    branches = set(left_mix) | set(right_mix)
    distance = 0.5 * sum(
        abs(left_mix.get(branch, 0.0) - right_mix.get(branch, 0.0))
        for branch in branches
    )
    return float(distance)


def persistence_diagnostics(monthly_ratios: list[float | None]) -> dict[str, Any]:
    valid = [
        float(value)
        for value in monthly_ratios
        if value is not None and math.isfinite(float(value))
    ]
    if not valid:
        return {
            "state": "insufficient_history",
            "months": 0,
            "above_neutral_months": 0,
            "longest_above_neutral_run": 0,
            "median_ratio": None,
            "direction": "unavailable",
        }

    longest = 0
    current = 0
    above = 0
    for value in monthly_ratios:
        if value is not None and math.isfinite(float(value)) and float(value) > 1.0:
            above += 1
            current += 1
            longest = max(longest, current)
        else:
            current = 0

    midpoint = len(valid) // 2
    direction = "insufficient_for_direction"
    if midpoint >= 2 and len(valid) - midpoint >= 2:
        early = median(valid[:midpoint])
        late = median(valid[midpoint:])
        tolerance = 0.05
        if late < early * (1.0 - tolerance):
            direction = "improving"
        elif late > early * (1.0 + tolerance):
            direction = "deteriorating"
        else:
            direction = "stable"

    return {
        "state": "available",
        "months": len(valid),
        "above_neutral_months": above,
        "longest_above_neutral_run": longest,
        "median_ratio": float(median(valid)),
        "direction": direction,
        "scoring": "forbidden_in_diagnostic",
        "version": COMPARATIVE_VERSION,
    }
