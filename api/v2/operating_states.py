from __future__ import annotations

from collections import Counter
from typing import Any

OPERATING_STATE_VERSION = "2.0-draft-operating-states-1"
PARITY_REFERENCE = 1.0
HISTORY_WINDOW_MONTHS = 12


def _previous_month(period: int) -> int:
    year = period // 100
    month = period % 100
    if month == 1:
        return (year - 1) * 100 + 12
    return year * 100 + month - 1


def month_window(reference_period: int | None, months: int) -> list[int]:
    if not reference_period or months <= 0:
        return []
    output = [reference_period]
    current = reference_period
    for _ in range(months - 1):
        current = _previous_month(current)
        output.append(current)
    return sorted(output)


def prior_equivalent_period(reference_period: int | None) -> int | None:
    if not reference_period:
        return None
    return ((reference_period // 100) - 1) * 100 + (reference_period % 100)


def _metric_series(entity: dict[str, Any], metric: str) -> dict[int, dict[str, Any]]:
    payload = ((entity.get("metrics") or {}).get(metric) or {})
    return {
        int(row["period"]): row
        for row in (payload.get("series_last_48") or [])
        if row.get("period") is not None
    }


def _formula_state(entity: dict[str, Any]) -> tuple[str, list[str]]:
    current = (((entity.get("metrics") or {}).get("ICA") or {}).get("current") or {})
    state = current.get("state")
    reason_codes: list[str] = []

    if not current:
        return "missing_source_period", ["ica_current_missing"]
    if state == "derivable":
        return "derivable", []
    if state == "non_positive_operating_base":
        return "no_positive_operating_base", list(current.get("flags") or [])
    if state == "non_positive_denominator":
        financial_result = current.get("financial_result")
        operating_base = current.get("operating_base")
        if (
            isinstance(financial_result, (int, float))
            and isinstance(operating_base, (int, float))
            and operating_base > 0
            and financial_result < 0
            and operating_base + financial_result <= 0
        ):
            reason_codes.append("negative_financial_result_exceeds_operating_base")
        reason_codes.extend(current.get("flags") or [])
        return "non_positive_expanded_denominator", list(dict.fromkeys(reason_codes))
    if state == "missing_components":
        return "missing_formula_components", ["ica_formula_components_missing"]
    if state == "source_duplicate_components":
        return "source_quality_issue", ["duplicate_candidate_balance_cmpids"]
    return "invalid_formula_result", [str(state or "unknown_ica_state")]


def _history_state(
    entity: dict[str, Any], reference_period: int | None
) -> tuple[str, dict[str, Any]]:
    ic_series = _metric_series(entity, "IC")
    ica_series = _metric_series(entity, "ICA")
    expected = month_window(reference_period, HISTORY_WINDOW_MONTHS)
    positive_base_periods = [
        period
        for period in expected
        if period in ic_series
        and isinstance(ic_series[period].get("operating_base"), (int, float))
        and float(ic_series[period]["operating_base"]) > 0
        and ic_series[period].get("state") != "source_duplicate_components"
    ]
    prior_period = prior_equivalent_period(reference_period)
    prior_ica = ica_series.get(prior_period or -1) or {}
    comparable_prior = prior_ica.get("state") == "derivable"
    complete_positive_base_history = len(positive_base_periods) == len(expected) == 12

    details = {
        "window_months": HISTORY_WINDOW_MONTHS,
        "expected_positive_base_months": len(expected),
        "positive_base_months": len(positive_base_periods),
        "complete_positive_base_history": complete_positive_base_history,
        "comparison_period": prior_period,
        "comparable_prior_ica": comparable_prior,
    }
    if complete_positive_base_history and comparable_prior:
        return "established", details
    return "limited", details


def _operating_signal(
    entity: dict[str, Any],
    reference_period: int | None,
    history_state: str,
    formula_state: str,
) -> tuple[str, dict[str, Any]]:
    prior_period = prior_equivalent_period(reference_period)
    ica_series = _metric_series(entity, "ICA")
    current = (((entity.get("metrics") or {}).get("ICA") or {}).get("current") or {})
    prior = ica_series.get(prior_period or -1) or {}

    current_value = current.get("value") if current.get("state") == "derivable" else None
    prior_value = prior.get("value") if prior.get("state") == "derivable" else None
    details = {
        "reference_metric": "ICA",
        "supporting_metric": "IC",
        "parity_reference": PARITY_REFERENCE,
        "reference_period": reference_period,
        "comparison_period": prior_period,
        "current_value": current_value,
        "comparison_value": prior_value,
        "parity_note": (
            "1.0 is an arithmetic cost/revenue parity reference in the formula, "
            "not a SUSEP prudential approval threshold."
        ),
    }

    if history_state != "established" or formula_state != "derivable":
        return "indeterminate", details
    if not isinstance(current_value, (int, float)) or not isinstance(prior_value, (int, float)):
        return "indeterminate", details

    current_balanced = float(current_value) < PARITY_REFERENCE
    prior_balanced = float(prior_value) < PARITY_REFERENCE
    if current_balanced and prior_balanced:
        return "balanced_persistent", details
    if current_balanced and not prior_balanced:
        return "improved", details
    if not current_balanced and prior_balanced:
        return "recent_pressure", details
    return "persistent_pressure", details


def build_operating_state(
    entity: dict[str, Any], reference_period: int | None
) -> dict[str, Any]:
    formula_state, reason_codes = _formula_state(entity)
    history_state, history_details = _history_state(entity, reference_period)
    operating_signal, signal_details = _operating_signal(
        entity,
        reference_period,
        history_state,
        formula_state,
    )
    if history_state == "limited":
        reason_codes.append("limited_comparable_operating_history")
    if operating_signal == "indeterminate" and formula_state != "derivable":
        reason_codes.append("current_operating_signal_not_derivable")

    return {
        "version": OPERATING_STATE_VERSION,
        "formula_state": formula_state,
        "history_state": history_state,
        "operating_signal": operating_signal,
        "reason_codes": list(dict.fromkeys(reason_codes)),
        "history": history_details,
        "signal": signal_details,
        "score": None,
        "assessment_effect": "none_experimental",
        "ranking_effect": "none_experimental",
    }


def operating_state_summary(entities: list[dict[str, Any]]) -> dict[str, Any]:
    formula = Counter()
    history = Counter()
    signal = Counter()
    for entity in entities:
        state = entity.get("operating_state") or {}
        formula[str(state.get("formula_state") or "missing")] += 1
        history[str(state.get("history_state") or "missing")] += 1
        signal[str(state.get("operating_signal") or "missing")] += 1
    return {
        "version": OPERATING_STATE_VERSION,
        "formula_state_counts": dict(formula),
        "history_state_counts": dict(history),
        "operating_signal_counts": dict(signal),
        "note": (
            "Experimental semantic states only. They do not change assessment eligibility, "
            "ranking eligibility or score."
        ),
    }
