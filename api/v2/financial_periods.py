from __future__ import annotations

import math
from typing import Any

MATURITY_POLICY_VERSION = "2.0-draft-financial-period-maturity-1"
MATURITY_LOOKBACK_PERIODS = 6
MATURITY_MIN_RELATIVE_COVERAGE = 0.95


class FinancialPeriodMaturityError(RuntimeError):
    """Raised when no common financial period is usable for methodology work."""


def _as_period_set(values: Any) -> set[int]:
    output: set[int] = set()
    for value in values or []:
        try:
            period = int(value)
        except (TypeError, ValueError):
            continue
        if period > 0:
            output.add(period)
    return output


def _capital_metric_derivable(record: dict[str, Any] | None) -> bool:
    if not record:
        return False
    pla = record.get("pla_adjusted")
    cmr = record.get("cmr")
    if pla is None or cmr is None:
        return False
    try:
        return float(cmr) > 0.0
    except (TypeError, ValueError):
        return False


def _observed_periods(source_payload: dict[str, Any]) -> dict[str, set[int]]:
    capital: set[int] = set()
    balance: set[int] = set()
    operations: set[int] = set()

    for source_entity in (source_payload.get("entities") or {}).values():
        capital.update(_as_period_set((source_entity.get("capital_history") or {}).keys()))
        balance.update(_as_period_set(source_entity.get("balance_periods") or set()))
        operations.update(
            _as_period_set(source_entity.get("insurance_operation_periods") or set())
        )

    return {
        "capital": capital,
        "balance": balance,
        "insurance_operations": operations,
    }


def build_financial_period_maturity(
    source_payload: dict[str, Any],
    *,
    lookback_periods: int = MATURITY_LOOKBACK_PERIODS,
    min_relative_coverage: float = MATURITY_MIN_RELATIVE_COVERAGE,
) -> dict[str, Any]:
    """Select the latest common financial period with mature capital coverage.

    The raw SES reader reports the latest observed period in each table. A newly
    published period may exist for the whole table while a material share of the
    insurer universe still has unusable prudential rows. This diagnostic keeps
    raw observation separate from the period used for methodology work.

    Maturity is based on PLA/CMR derivability because capital adequacy is the
    core prudential metric in the current financial evidence gate. The selected
    period must also exist in balance and insurance-operation sources so the
    financial components are aligned to one reference month.
    """
    if lookback_periods <= 0:
        raise ValueError("lookback_periods must be positive")
    if not 0 < min_relative_coverage <= 1:
        raise ValueError("min_relative_coverage must be in (0, 1]")

    entities = source_payload.get("entities") or {}
    observed = _observed_periods(source_payload)
    common_periods = sorted(
        observed["capital"]
        & observed["balance"]
        & observed["insurance_operations"]
    )
    if not common_periods:
        raise FinancialPeriodMaturityError(
            "No common period across capital, balance and insurance operations"
        )

    candidates = common_periods[-lookback_periods:]
    coverage: dict[int, int] = {}
    for period in candidates:
        count = 0
        for source_entity in entities.values():
            record = (source_entity.get("capital_history") or {}).get(period)
            if _capital_metric_derivable(record):
                count += 1
        coverage[period] = count

    peak = max(coverage.values(), default=0)
    if peak <= 0:
        raise FinancialPeriodMaturityError(
            "No derivable PLA/CMR observations in common maturity lookback"
        )

    minimum_count = max(1, math.ceil(peak * min_relative_coverage))
    mature_candidates = [
        period for period in candidates if coverage.get(period, 0) >= minimum_count
    ]
    if not mature_candidates:
        raise FinancialPeriodMaturityError(
            "No common period meets the financial maturity coverage policy"
        )

    selected = max(mature_candidates)
    latest_common = max(candidates)
    source_reference_periods = dict(source_payload.get("reference_periods") or {})

    return {
        "policy_version": MATURITY_POLICY_VERSION,
        "selection_basis": "pla_cmr_derivable_coverage",
        "lookback_common_periods": lookback_periods,
        "min_relative_coverage": min_relative_coverage,
        "source_observed_reference_periods": source_reference_periods,
        "candidate_periods": candidates,
        "capital_derivable_counts": {
            str(period): coverage[period] for period in candidates
        },
        "peak_derivable_count": peak,
        "minimum_derivable_count": minimum_count,
        "latest_common_period": latest_common,
        "latest_common_derivable_count": coverage[latest_common],
        "latest_common_relative_to_peak": coverage[latest_common] / peak,
        "selected_period": selected,
        "selected_derivable_count": coverage[selected],
        "selected_relative_to_peak": coverage[selected] / peak,
        "status": (
            "latest_common_mature"
            if selected == latest_common
            else "latest_common_immature_rolled_back"
        ),
    }


def apply_mature_financial_reference_period(
    source_payload: dict[str, Any],
    *,
    lookback_periods: int = MATURITY_LOOKBACK_PERIODS,
    min_relative_coverage: float = MATURITY_MIN_RELATIVE_COVERAGE,
) -> dict[str, Any]:
    """Return a shallow source payload view aligned to one mature period."""
    maturity = build_financial_period_maturity(
        source_payload,
        lookback_periods=lookback_periods,
        min_relative_coverage=min_relative_coverage,
    )
    selected = int(maturity["selected_period"])
    output = dict(source_payload)
    output["reference_periods"] = {
        "capital": selected,
        "balance": selected,
        "insurance_operations": selected,
    }
    output["period_maturity"] = maturity
    return output
