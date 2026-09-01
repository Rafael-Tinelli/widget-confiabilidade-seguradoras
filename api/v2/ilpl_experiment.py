from __future__ import annotations

import math
from collections import Counter
from statistics import median
from typing import Any

import numpy as np
from scipy.stats import spearmanr

from api.v2.financial_capital_semantics import capital_pla_cmr_ratio

ILPL_EXPERIMENT_VERSION = "2.0-closed-ilpl-experiment-1"
SURVIVAL_CRITERIA_VERSION = "1.0-locked-before-first-basecompleta-run"
ILPL_NET_INCOME_CMPID = 518
ILPL_EQUITY_CMPID = 3333

SURVIVAL_CRITERIA: dict[str, float | int] = {
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


class ILPLExperimentInvariantError(ValueError):
    """Raised when the closed ILPL experiment violates its frozen contract."""


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def calculate_ilpl_observation(
    current_values: dict[int, float],
    prior_december_values: dict[int, float],
    *,
    duplicate_current_rows: int = 0,
    duplicate_prior_december_rows: int = 0,
) -> dict[str, Any]:
    """Calculate SUSEP ILPL exactly as published, without annualization.

    ILPL = net income in the reference month / average equity, where average
    equity is the mean of current equity and prior-December equity. Result
    accounts are therefore compared only at equivalent horizons elsewhere in
    this experiment.
    """
    net_income = _finite(current_values.get(ILPL_NET_INCOME_CMPID))
    current_equity = _finite(current_values.get(ILPL_EQUITY_CMPID))
    prior_december_equity = _finite(prior_december_values.get(ILPL_EQUITY_CMPID))

    missing: list[str] = []
    if net_income is None:
        missing.append("current_net_income_518")
    if current_equity is None:
        missing.append("current_equity_3333")
    if prior_december_equity is None:
        missing.append("prior_december_equity_3333")

    if duplicate_current_rows or duplicate_prior_december_rows:
        return {
            "state": "source_duplicate_components",
            "value": None,
            "net_income": net_income,
            "current_equity": current_equity,
            "prior_december_equity": prior_december_equity,
            "average_equity": None,
            "missing_components": missing,
            "duplicate_current_rows": int(duplicate_current_rows),
            "duplicate_prior_december_rows": int(duplicate_prior_december_rows),
            "flags": ["duplicate_ilpl_cmpids"],
        }

    if missing:
        return {
            "state": "missing_components",
            "value": None,
            "net_income": net_income,
            "current_equity": current_equity,
            "prior_december_equity": prior_december_equity,
            "average_equity": None,
            "missing_components": missing,
            "duplicate_current_rows": 0,
            "duplicate_prior_december_rows": 0,
            "flags": [],
        }

    assert net_income is not None
    assert current_equity is not None
    assert prior_december_equity is not None
    average_equity = (current_equity + prior_december_equity) / 2.0
    if average_equity <= 0:
        return {
            "state": "non_positive_average_equity",
            "value": None,
            "net_income": net_income,
            "current_equity": current_equity,
            "prior_december_equity": prior_december_equity,
            "average_equity": average_equity,
            "missing_components": [],
            "duplicate_current_rows": 0,
            "duplicate_prior_december_rows": 0,
            "flags": [
                "average_equity_zero" if average_equity == 0 else "average_equity_negative"
            ],
        }

    value = net_income / average_equity
    if not math.isfinite(value):
        return {
            "state": "non_finite_result",
            "value": None,
            "net_income": net_income,
            "current_equity": current_equity,
            "prior_december_equity": prior_december_equity,
            "average_equity": average_equity,
            "missing_components": [],
            "duplicate_current_rows": 0,
            "duplicate_prior_december_rows": 0,
            "flags": [],
        }

    flags: list[str] = []
    if net_income < 0:
        flags.append("negative_net_income")
    return {
        "state": "derivable",
        "value": float(value),
        "net_income": net_income,
        "current_equity": current_equity,
        "prior_december_equity": prior_december_equity,
        "average_equity": average_equity,
        "missing_components": [],
        "duplicate_current_rows": 0,
        "duplicate_prior_december_rows": 0,
        "flags": flags,
    }


def equivalent_month_periods(reference_period: int | None, years: int = 4) -> list[int]:
    if not reference_period or years <= 0:
        return []
    year = reference_period // 100
    month = reference_period % 100
    return [(year - offset) * 100 + month for offset in range(years - 1, -1, -1)]


def prior_year_end_periods(reference_period: int | None, years: int = 3) -> list[int]:
    if not reference_period or years <= 0:
        return []
    year = reference_period // 100
    return [(year - offset) * 100 + 12 for offset in range(years, 0, -1)]


def _robust_stats(values: list[float]) -> dict[str, Any]:
    if not values:
        return {"count": 0}
    array = np.asarray(values, dtype=float)
    percentiles = np.percentile(array, [1, 5, 10, 25, 50, 75, 90, 95, 99])
    return {
        "count": int(array.size),
        "min": float(np.min(array)),
        "p01": float(percentiles[0]),
        "p05": float(percentiles[1]),
        "p10": float(percentiles[2]),
        "p25": float(percentiles[3]),
        "median": float(percentiles[4]),
        "p75": float(percentiles[5]),
        "p90": float(percentiles[6]),
        "p95": float(percentiles[7]),
        "p99": float(percentiles[8]),
        "max": float(np.max(array)),
        "mean": float(np.mean(array)),
        "stddev_population": float(np.std(array)),
    }


def _safe_correlation(x: list[float], y: list[float]) -> dict[str, Any]:
    if len(x) < 3 or len(x) != len(y):
        return {"count": len(x), "pearson": None, "spearman": None}
    x_array = np.asarray(x, dtype=float)
    y_array = np.asarray(y, dtype=float)
    pearson = float(np.corrcoef(x_array, y_array)[0, 1])
    spearman = float(spearmanr(x_array, y_array).statistic)
    return {
        "count": len(x),
        "pearson": pearson if math.isfinite(pearson) else None,
        "spearman": spearman if math.isfinite(spearman) else None,
    }


def build_entity_ilpl_experiment(
    entity: dict[str, Any],
    ilpl_values: dict[int, dict[int, float]],
    duplicate_rows_by_period: dict[int, int],
    reference_period: int | None,
) -> dict[str, Any]:
    observations: list[dict[str, Any]] = []
    for period in sorted(int(item) for item in ilpl_values):
        year = period // 100
        prior_december = (year - 1) * 100 + 12
        observation = calculate_ilpl_observation(
            ilpl_values.get(period, {}),
            ilpl_values.get(prior_december, {}),
            duplicate_current_rows=duplicate_rows_by_period.get(period, 0),
            duplicate_prior_december_rows=duplicate_rows_by_period.get(prior_december, 0),
        )
        observations.append({"period": period, **observation})
    by_period = {int(item["period"]): item for item in observations}
    return {
        "entity_id": entity.get("entity_id"),
        "fip_code": entity.get("fip_code"),
        "legal_name": entity.get("legal_name"),
        "reference_period": reference_period,
        "current": by_period.get(reference_period or -1),
        "equivalent_month_history": [
            by_period[period]
            for period in equivalent_month_periods(reference_period)
            if period in by_period
        ],
        "year_end_history": [
            by_period[period]
            for period in prior_year_end_periods(reference_period)
            if period in by_period
        ],
        "series_last_48": observations[-48:],
    }


def _series(entity: dict[str, Any]) -> dict[int, dict[str, Any]]:
    return {
        int(row["period"]): row
        for row in (entity.get("series_last_48") or [])
        if row.get("period") is not None
    }


def _derivable_value(row: dict[str, Any] | None) -> float | None:
    if not row or row.get("state") != "derivable":
        return None
    return _finite(row.get("value"))


def _rank_stability_vs_period(
    entities: list[dict[str, Any]], reference_period: int, comparison_periods: list[int]
) -> dict[str, Any]:
    comparisons: list[dict[str, Any]] = []
    for comparison_period in comparison_periods:
        pairs: list[tuple[float, float]] = []
        for entity in entities:
            by_period = _series(entity)
            reference_value = _derivable_value(by_period.get(reference_period))
            comparison_value = _derivable_value(by_period.get(comparison_period))
            if reference_value is not None and comparison_value is not None:
                pairs.append((reference_value, comparison_value))
        correlation = _safe_correlation(
            [left for left, _ in pairs], [right for _, right in pairs]
        )
        comparisons.append(
            {
                "period": comparison_period,
                "common_entities": len(pairs),
                "spearman": correlation.get("spearman"),
            }
        )
    values = [
        float(item["spearman"])
        for item in comparisons
        if item.get("spearman") is not None
    ]
    return {
        "comparisons": comparisons,
        "summary": {
            "count": len(values),
            "median_spearman": float(median(values)) if values else None,
            "min_spearman": min(values) if values else None,
            "max_spearman": max(values) if values else None,
        },
    }


def _sign(value: float) -> int:
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def _current_rows(entities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for entity in entities:
        current = entity.get("current") or {}
        value = _derivable_value(current)
        average_equity = _finite(current.get("average_equity"))
        if value is None or average_equity is None:
            continue
        rows.append(
            {
                "entity_id": entity.get("entity_id"),
                "fip_code": entity.get("fip_code"),
                "legal_name": entity.get("legal_name"),
                "value": value,
                "average_equity": average_equity,
                "net_income": _finite(current.get("net_income")),
            }
        )
    return rows


def _capital_ratio(source_entity: dict[str, Any], period: int | None) -> float | None:
    if not period:
        return None
    record = (source_entity.get("capital_history") or {}).get(period) or {}
    return capital_pla_cmr_ratio(record)


def _ilt_value(source_entity: dict[str, Any], period: int | None) -> float | None:
    if not period:
        return None
    from api.v2.liquidity_experiment import calculate_liquidity_observation

    observation = calculate_liquidity_observation(
        (source_entity.get("balance_values") or {}).get(period, {}), "ILT"
    )
    if observation.get("state") != "derivable":
        return None
    return _finite(observation.get("value"))


def _redundancy(
    current_rows: list[dict[str, Any]],
    source_entities: dict[str, Any],
    reference_period: int | None,
    comparator: str,
) -> dict[str, Any]:
    pairs: list[tuple[float, float]] = []
    for row in current_rows:
        fip = str(row.get("fip_code") or "").zfill(6)
        source_entity = source_entities.get(fip, {})
        other = (
            _capital_ratio(source_entity, reference_period)
            if comparator == "pla_cmr"
            else _ilt_value(source_entity, reference_period)
        )
        if other is not None:
            pairs.append((float(row["value"]), other))
    return _safe_correlation([x for x, _ in pairs], [y for _, y in pairs])


def ilpl_experiment_summary(
    entities: list[dict[str, Any]],
    source_payload: dict[str, Any],
    reference_period: int | None,
) -> dict[str, Any]:
    universe = len(entities)
    current_rows = _current_rows(entities)
    current_count = len(current_rows)
    current_values = [float(row["value"]) for row in current_rows]
    current_states = Counter(
        str((entity.get("current") or {}).get("state") or "missing_current")
        for entity in entities
    )

    prior_equivalent = (
        ((reference_period // 100) - 1) * 100 + (reference_period % 100)
        if reference_period
        else None
    )
    paired: list[tuple[float, float]] = []
    for entity in entities:
        by_period = _series(entity)
        current = _derivable_value(by_period.get(reference_period or -1))
        prior = _derivable_value(by_period.get(prior_equivalent or -1))
        if current is not None and prior is not None:
            paired.append((current, prior))

    same_month_periods = equivalent_month_periods(reference_period, 4)[:-1]
    same_month_stability = (
        _rank_stability_vs_period(entities, reference_period, same_month_periods)
        if reference_period
        else {"comparisons": [], "summary": {"count": 0}}
    )

    year_end_periods = prior_year_end_periods(reference_period, 3)
    if year_end_periods:
        year_end_reference = year_end_periods[-1]
        year_end_stability = _rank_stability_vs_period(
            entities, year_end_reference, year_end_periods[:-1]
        )
    else:
        year_end_reference = None
        year_end_stability = {"comparisons": [], "summary": {"count": 0}}

    sign_matches = sum(_sign(current) == _sign(prior) for current, prior in paired)
    sign_persistence = sign_matches / len(paired) if paired else None

    scale_correlation = _safe_correlation(
        [abs(float(row["value"])) for row in current_rows],
        [float(row["average_equity"]) for row in current_rows],
    )

    source_entities = source_payload.get("entities") or {}
    capital_redundancy = _redundancy(
        current_rows, source_entities, reference_period, "pla_cmr"
    )
    ilt_redundancy = _redundancy(current_rows, source_entities, reference_period, "ilt")

    return {
        "version": ILPL_EXPERIMENT_VERSION,
        "reference_period": reference_period,
        "entity_count": universe,
        "criteria_version": SURVIVAL_CRITERIA_VERSION,
        "criteria_locked_before_first_basecompleta_run": dict(SURVIVAL_CRITERIA),
        "current_state_counts": dict(current_states),
        "current_derivable_count": current_count,
        "current_coverage": current_count / universe if universe else None,
        "current_distribution": _robust_stats(current_values),
        "current_sign_counts": {
            "positive": sum(value > 0 for value in current_values),
            "negative": sum(value < 0 for value in current_values),
            "zero": sum(value == 0 for value in current_values),
        },
        "prior_equivalent_period": prior_equivalent,
        "paired_prior_equivalent_count": len(paired),
        "paired_prior_equivalent_coverage": len(paired) / universe if universe else None,
        "same_month_rank_stability": same_month_stability,
        "year_end_reference_period": year_end_reference,
        "year_end_rank_stability": year_end_stability,
        "sign_persistence": {
            "paired_count": len(paired),
            "same_sign_count": sign_matches,
            "rate": sign_persistence,
        },
        "scale_bias_abs_ilpl_vs_average_equity": scale_correlation,
        "redundancy": {
            "pla_cmr": capital_redundancy,
            "ilt": ilt_redundancy,
        },
        "extreme_diagnostics": {
            "largest_absolute_current": sorted(
                current_rows, key=lambda row: abs(float(row["value"])), reverse=True
            )[:10],
            "note": (
                "Extreme values are diagnostic only. They do not alter the pre-registered "
                "survival criteria and will not trigger a rescue transformation."
            ),
        },
        "methodology_note": (
            "ILPL follows the official SUSEP formula without annualization. Because net income "
            "is accumulated within the fiscal year, longitudinal stability is tested only at "
            "equivalent month horizons and comparable year-end horizons."
        ),
    }


def _gate_min(observed: float | None, threshold: float) -> bool:
    return observed is not None and observed >= threshold


def _gate_max_abs(observed: float | None, threshold: float) -> bool:
    return observed is not None and abs(observed) <= threshold


def evaluate_survival(summary: dict[str, Any]) -> dict[str, Any]:
    criteria = SURVIVAL_CRITERIA
    same_month = summary.get("same_month_rank_stability", {}).get("summary", {})
    year_end = summary.get("year_end_rank_stability", {}).get("summary", {})
    sign = summary.get("sign_persistence", {}).get("rate")
    scale = summary.get("scale_bias_abs_ilpl_vs_average_equity", {}).get("spearman")
    redundancies = summary.get("redundancy") or {}
    capital_corr = (redundancies.get("pla_cmr") or {}).get("spearman")
    ilt_corr = (redundancies.get("ilt") or {}).get("spearman")
    redundancy_values = [
        abs(float(value))
        for value in (capital_corr, ilt_corr)
        if value is not None and math.isfinite(float(value))
    ]
    max_redundancy = max(redundancy_values) if redundancy_values else None

    gates = {
        "current_coverage": {
            "observed": summary.get("current_coverage"),
            "operator": ">=",
            "threshold": criteria["current_coverage_min"],
            "pass": _gate_min(
                summary.get("current_coverage"), float(criteria["current_coverage_min"])
            ),
        },
        "paired_prior_equivalent_coverage": {
            "observed": summary.get("paired_prior_equivalent_coverage"),
            "operator": ">=",
            "threshold": criteria["paired_prior_equivalent_coverage_min"],
            "pass": _gate_min(
                summary.get("paired_prior_equivalent_coverage"),
                float(criteria["paired_prior_equivalent_coverage_min"]),
            ),
        },
        "same_month_rank_stability": {
            "observed": same_month.get("median_spearman"),
            "comparison_count": same_month.get("count", 0),
            "operator": ">=",
            "threshold": criteria["same_month_rank_stability_median_min"],
            "minimum_comparisons": criteria["minimum_same_month_comparisons"],
            "pass": (
                int(same_month.get("count") or 0)
                >= int(criteria["minimum_same_month_comparisons"])
                and _gate_min(
                    same_month.get("median_spearman"),
                    float(criteria["same_month_rank_stability_median_min"]),
                )
            ),
        },
        "year_end_rank_stability": {
            "observed": year_end.get("median_spearman"),
            "comparison_count": year_end.get("count", 0),
            "operator": ">=",
            "threshold": criteria["year_end_rank_stability_median_min"],
            "minimum_comparisons": criteria["minimum_year_end_comparisons"],
            "pass": (
                int(year_end.get("count") or 0)
                >= int(criteria["minimum_year_end_comparisons"])
                and _gate_min(
                    year_end.get("median_spearman"),
                    float(criteria["year_end_rank_stability_median_min"]),
                )
            ),
        },
        "sign_persistence": {
            "observed": sign,
            "operator": ">=",
            "threshold": criteria["sign_persistence_min"],
            "pass": _gate_min(sign, float(criteria["sign_persistence_min"])),
        },
        "scale_bias": {
            "observed_spearman": scale,
            "observed_absolute": abs(scale) if scale is not None else None,
            "operator": "<=",
            "threshold": criteria["scale_bias_abs_spearman_max"],
            "pass": _gate_max_abs(
                scale, float(criteria["scale_bias_abs_spearman_max"])
            ),
        },
        "redundancy": {
            "pla_cmr_spearman": capital_corr,
            "ilt_spearman": ilt_corr,
            "observed_max_absolute": max_redundancy,
            "operator": "<=",
            "threshold": criteria["redundancy_abs_spearman_max"],
            "pass": (
                max_redundancy is not None
                and max_redundancy <= float(criteria["redundancy_abs_spearman_max"])
            ),
        },
    }
    failed = [name for name, gate in gates.items() if not gate.get("pass")]
    survives = not failed
    return {
        "criteria_version": SURVIVAL_CRITERIA_VERSION,
        "criteria_were_locked_before_first_basecompleta_run": True,
        "gates": gates,
        "failed_gates": failed,
        "survives_as_independent_scoring_candidate": survives,
        "verdict": (
            "survives_closed_investigation"
            if survives
            else "reject_independent_scoring_candidate_no_rescue_iteration"
        ),
    }


def validate_ilpl_experiment(payload: dict[str, Any]) -> None:
    if payload.get("status") != "experimental_closed":
        raise ILPLExperimentInvariantError("ILPL artifact must remain experimental_closed")
    if payload.get("criteria_version") != SURVIVAL_CRITERIA_VERSION:
        raise ILPLExperimentInvariantError("unexpected ILPL survival criteria version")
    if payload.get("criteria_locked_before_first_basecompleta_run") != SURVIVAL_CRITERIA:
        raise ILPLExperimentInvariantError("ILPL survival criteria changed from frozen contract")

    forbidden = {"score", "weight", "assessment_eligible", "ranking_eligible"}

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            overlap = forbidden & set(value)
            if overlap:
                raise ILPLExperimentInvariantError(
                    f"forbidden decision fields in closed ILPL experiment: {sorted(overlap)}"
                )
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(payload)
