from __future__ import annotations

from api.v2.operating_states import build_operating_state, month_window


def _row(
    period: int,
    *,
    value: float | None = 0.9,
    operating_base: float = 100.0,
    state: str = "derivable",
    financial_result: float = 10.0,
    flags: list[str] | None = None,
) -> dict:
    return {
        "period": period,
        "state": state,
        "value": value,
        "operating_base": operating_base,
        "financial_result": financial_result,
        "denominator": operating_base + financial_result,
        "flags": flags or [],
    }


def _entity(
    *,
    current_ica: dict,
    prior_ica: dict | None,
    positive_base_months: int = 12,
) -> dict:
    reference = 202605
    window = month_window(reference, 12)
    positive_periods = set(window[-positive_base_months:]) if positive_base_months else set()
    ic_series = [
        _row(
            period,
            value=0.95,
            operating_base=100.0 if period in positive_periods else 0.0,
            state="derivable" if period in positive_periods else "non_positive_operating_base",
        )
        for period in window
    ]
    ica_series = [current_ica]
    if prior_ica is not None:
        ica_series.append(prior_ica)
    return {
        "metrics": {
            "IC": {
                "current": ic_series[-1],
                "series_last_48": ic_series,
            },
            "ICA": {
                "current": current_ica,
                "series_last_48": ica_series,
            },
        }
    }


def test_persistent_balanced_requires_established_comparable_history() -> None:
    entity = _entity(
        current_ica=_row(202605, value=0.91),
        prior_ica=_row(202505, value=0.94),
        positive_base_months=12,
    )

    state = build_operating_state(entity, 202605)

    assert state["formula_state"] == "derivable"
    assert state["history_state"] == "established"
    assert state["operating_signal"] == "balanced_persistent"
    assert state["assessment_effect"] == "none_experimental"
    assert state["ranking_effect"] == "none_experimental"
    assert "score" not in state


def test_limited_history_keeps_signal_indeterminate() -> None:
    entity = _entity(
        current_ica=_row(202605, value=0.91),
        prior_ica=_row(202505, value=0.94),
        positive_base_months=8,
    )

    state = build_operating_state(entity, 202605)

    assert state["formula_state"] == "derivable"
    assert state["history_state"] == "limited"
    assert state["operating_signal"] == "indeterminate"
    assert "limited_comparable_operating_history" in state["reason_codes"]


def test_signal_describes_direction_without_creating_a_score() -> None:
    improved = _entity(
        current_ica=_row(202605, value=0.92),
        prior_ica=_row(202505, value=1.08),
    )
    pressure = _entity(
        current_ica=_row(202605, value=1.07),
        prior_ica=_row(202505, value=0.93),
    )

    assert build_operating_state(improved, 202605)["operating_signal"] == "improved"
    assert build_operating_state(pressure, 202605)["operating_signal"] == "recent_pressure"


def test_negative_financial_result_exceeding_base_has_specific_formula_state() -> None:
    current = _row(
        202605,
        value=None,
        operating_base=100.0,
        state="non_positive_denominator",
        financial_result=-150.0,
        flags=["negative_financial_result", "denominator_negative"],
    )
    entity = _entity(
        current_ica=current,
        prior_ica=_row(202505, value=0.95),
    )

    state = build_operating_state(entity, 202605)

    assert state["formula_state"] == "non_positive_expanded_denominator"
    assert state["operating_signal"] == "indeterminate"
    assert "negative_financial_result_exceeds_operating_base" in state["reason_codes"]
