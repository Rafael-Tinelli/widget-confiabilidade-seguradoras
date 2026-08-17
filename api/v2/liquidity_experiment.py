from __future__ import annotations

import math
from collections import Counter
from copy import deepcopy
from statistics import median
from typing import Any

import numpy as np
from scipy.stats import spearmanr

from api.v2.financial_evidence import month_window

LIQUIDITY_EXPERIMENT_VERSION = "2.0-draft-liquidity-experiment-1"
HISTORY_WINDOWS = (12, 24, 36)

# SUSEP, "Índices para Análise Econômico-Financeira das Supervisionadas" (2018).
# The formulas are reproduced as signed CMPID terms so the experiment does not
# silently reinterpret missing balance-sheet components as zero.
LIQUIDITY_FORMULAS: dict[str, dict[str, Any]] = {
    "ILC": {
        "numerator": ((1479, 1.0), (11160, -1.0), (351, -1.0)),
        "denominator": ((1040, 1.0),),
        "description": "Índice de Liquidez Corrente",
    },
    "ILT": {
        "numerator": (
            (1479, 1.0),
            (11160, -1.0),
            (351, -1.0),
            (331, 1.0),
            (11187, -1.0),
            (5503, -1.0),
        ),
        "denominator": ((1040, 1.0), (6449, 1.0)),
        "description": "Índice de Liquidez Total",
    },
}


class LiquidityExperimentInvariantError(ValueError):
    """Raised when the experimental liquidity artifact violates its contract."""


def _finite_number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _sum_terms(
    values: dict[int, float], terms: tuple[tuple[int, float], ...]
) -> tuple[float | None, list[int]]:
    missing: list[int] = []
    total = 0.0
    for cmpid, coefficient in terms:
        value = _finite_number(values.get(cmpid))
        if value is None:
            missing.append(cmpid)
            continue
        total += coefficient * value
    if missing:
        return None, sorted(missing)
    return total, []


def calculate_liquidity_observation(
    values: dict[int, float], metric: str
) -> dict[str, Any]:
    """Calculate one experimental liquidity observation from official CMPID terms.

    The function intentionally refuses to impute missing accounts or to divide by
    a non-positive denominator. Negative numerators remain mathematically
    derivable, but are explicitly flagged for economic review.
    """
    spec = LIQUIDITY_FORMULAS[metric]
    numerator, missing_num = _sum_terms(values, spec["numerator"])
    denominator, missing_den = _sum_terms(values, spec["denominator"])
    missing = sorted(set(missing_num + missing_den))
    if missing:
        return {
            "state": "missing_components",
            "value": None,
            "numerator": numerator,
            "denominator": denominator,
            "missing_cmpids": missing,
            "flags": [],
        }
    if numerator is None or denominator is None:
        return {
            "state": "non_finite_components",
            "value": None,
            "numerator": numerator,
            "denominator": denominator,
            "missing_cmpids": [],
            "flags": [],
        }
    if denominator <= 0:
        return {
            "state": "non_positive_denominator",
            "value": None,
            "numerator": numerator,
            "denominator": denominator,
            "missing_cmpids": [],
            "flags": ["denominator_zero" if denominator == 0 else "denominator_negative"],
        }
    value = numerator / denominator
    if not math.isfinite(value):
        return {
            "state": "non_finite_result",
            "value": None,
            "numerator": numerator,
            "denominator": denominator,
            "missing_cmpids": [],
            "flags": [],
        }
    flags: list[str] = []
    if numerator < 0:
        flags.append("negative_numerator")
    if value < 0:
        flags.append("negative_ratio")
    return {
        "state": "derivable",
        "value": float(value),
        "numerator": float(numerator),
        "denominator": float(denominator),
        "missing_cmpids": [],
        "flags": flags,
    }


def _robust_stats(values: list[float]) -> dict[str, Any]:
    if not values:
        return {"count": 0}
    array = np.asarray(values, dtype=float)
    percentiles = np.percentile(array, [1, 5, 10, 25, 50, 75, 90, 95, 99])
    q1 = float(percentiles[3])
    q3 = float(percentiles[5])
    iqr = q3 - q1
    return {
        "count": int(array.size),
        "min": float(np.min(array)),
        "p01": float(percentiles[0]),
        "p05": float(percentiles[1]),
        "p10": float(percentiles[2]),
        "p25": q1,
        "median": float(percentiles[4]),
        "p75": q3,
        "p90": float(percentiles[6]),
        "p95": float(percentiles[7]),
        "p99": float(percentiles[8]),
        "max": float(np.max(array)),
        "mean": float(np.mean(array)),
        "stddev_population": float(np.std(array)),
        "iqr": float(iqr),
        "tukey_lower_fence": float(q1 - 1.5 * iqr),
        "tukey_upper_fence": float(q3 + 1.5 * iqr),
    }


def _history_profile(
    series: list[dict[str, Any]], reference_period: int | None
) -> dict[str, Any]:
    by_period = {int(item["period"]): item for item in series}
    output: dict[str, Any] = {}
    for months in HISTORY_WINDOWS:
        window = month_window(reference_period, months)
        observations = [by_period.get(period) for period in window]
        derivable = [
            item
            for item in observations
            if item is not None and item.get("state") == "derivable"
        ]
        values = [float(item["value"]) for item in derivable]
        stats = _robust_stats(values)
        current = by_period.get(reference_period or -1)
        current_value = (
            float(current["value"])
            if current and current.get("state") == "derivable"
            else None
        )
        med = float(median(values)) if values else None
        mad = (
            float(median([abs(value - med) for value in values]))
            if values and med is not None
            else None
        )
        output[str(months)] = {
            "expected_months": months,
            "observed_months": sum(item is not None for item in observations),
            "derivable_months": len(values),
            "complete_derivable": len(values) == months,
            "stats": stats,
            "median_absolute_deviation": mad,
            "current_minus_window_median": (
                current_value - med
                if current_value is not None and med is not None
                else None
            ),
            "current_to_window_median": (
                current_value / med
                if current_value is not None and med not in {None, 0.0}
                else None
            ),
        }
    return output


def build_entity_liquidity_experiment(
    entity: dict[str, Any],
    source_entity: dict[str, Any],
    reference_period: int | None,
) -> dict[str, Any]:
    balance_values = source_entity.get("balance_values") or {}
    duplicate_count = int(source_entity.get("duplicate_balance_cmpid_rows") or 0)
    quality_excluded = duplicate_count > 0
    metrics: dict[str, Any] = {}
    for metric in LIQUIDITY_FORMULAS:
        series: list[dict[str, Any]] = []
        for period in sorted(int(item) for item in balance_values):
            observation = calculate_liquidity_observation(
                balance_values.get(period, {}), metric
            )
            series.append({"period": period, **observation})
        current = next(
            (item for item in series if item["period"] == reference_period),
            None,
        )
        metrics[metric] = {
            "current": deepcopy(current),
            "history": _history_profile(series, reference_period),
            "series_last_36": [
                item
                for item in series
                if item["period"] in set(month_window(reference_period, 36))
            ],
        }
    return {
        "entity_id": entity.get("entity_id"),
        "fip_code": entity.get("fip_code"),
        "legal_name": entity.get("legal_name"),
        "reference_period": reference_period,
        "quality_excluded_from_statistics": quality_excluded,
        "quality_reason_codes": (
            ["duplicate_candidate_balance_cmpids_require_review"]
            if quality_excluded
            else []
        ),
        "duplicate_candidate_balance_cmpid_rows": duplicate_count,
        "metrics": metrics,
    }


def _metric_current_rows(
    entities: list[dict[str, Any]], metric: str
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for entity in entities:
        if entity.get("quality_excluded_from_statistics"):
            continue
        current = ((entity.get("metrics") or {}).get(metric) or {}).get("current") or {}
        if current.get("state") != "derivable":
            continue
        value = _finite_number(current.get("value"))
        if value is None:
            continue
        rows.append(
            {
                "entity_id": entity.get("entity_id"),
                "fip_code": entity.get("fip_code"),
                "legal_name": entity.get("legal_name"),
                "value": value,
                "flags": list(current.get("flags") or []),
            }
        )
    return rows


def _safe_correlation(x: list[float], y: list[float]) -> dict[str, Any]:
    if len(x) < 3 or len(x) != len(y):
        return {"count": len(x), "pearson": None, "spearman": None}
    x_arr = np.asarray(x, dtype=float)
    y_arr = np.asarray(y, dtype=float)
    pearson = float(np.corrcoef(x_arr, y_arr)[0, 1])
    spearman_result = spearmanr(x_arr, y_arr)
    spearman = float(spearman_result.statistic)
    return {
        "count": len(x),
        "pearson": pearson if math.isfinite(pearson) else None,
        "spearman": spearman if math.isfinite(spearman) else None,
    }


def _paired_current_correlation(entities: list[dict[str, Any]]) -> dict[str, Any]:
    pairs: list[tuple[float, float]] = []
    for entity in entities:
        if entity.get("quality_excluded_from_statistics"):
            continue
        metrics = entity.get("metrics") or {}
        ilc = ((metrics.get("ILC") or {}).get("current") or {})
        ilt = ((metrics.get("ILT") or {}).get("current") or {})
        if ilc.get("state") != "derivable" or ilt.get("state") != "derivable":
            continue
        ilc_value = _finite_number(ilc.get("value"))
        ilt_value = _finite_number(ilt.get("value"))
        if ilc_value is not None and ilt_value is not None:
            pairs.append((ilc_value, ilt_value))
    return _safe_correlation(
        [item[0] for item in pairs],
        [item[1] for item in pairs],
    )


def _cross_section_rank_stability(
    entities: list[dict[str, Any]], metric: str, reference_period: int | None
) -> dict[str, Any]:
    if not reference_period:
        return {"months": [], "summary": {"count": 0}}
    current_by_entity: dict[str, float] = {}
    series_by_entity: dict[str, dict[int, float]] = {}
    for entity in entities:
        if entity.get("quality_excluded_from_statistics"):
            continue
        metric_payload = ((entity.get("metrics") or {}).get(metric) or {})
        current = metric_payload.get("current") or {}
        if current.get("state") == "derivable":
            value = _finite_number(current.get("value"))
            if value is not None:
                current_by_entity[str(entity.get("entity_id"))] = value
        history: dict[int, float] = {}
        for row in metric_payload.get("series_last_36") or []:
            if row.get("state") != "derivable":
                continue
            value = _finite_number(row.get("value"))
            if value is not None:
                history[int(row["period"])] = value
        series_by_entity[str(entity.get("entity_id"))] = history

    months: list[dict[str, Any]] = []
    for period in month_window(reference_period, 12):
        if period == reference_period:
            continue
        common = [
            entity_id
            for entity_id in current_by_entity
            if period in series_by_entity.get(entity_id, {})
        ]
        if len(common) < 30:
            months.append({"period": period, "common_entities": len(common), "spearman": None})
            continue
        result = _safe_correlation(
            [current_by_entity[item] for item in common],
            [series_by_entity[item][period] for item in common],
        )
        months.append(
            {
                "period": period,
                "common_entities": len(common),
                "spearman": result["spearman"],
            }
        )
    correlations = [
        float(item["spearman"])
        for item in months
        if item.get("spearman") is not None
    ]
    return {
        "months": months,
        "summary": {
            "count": len(correlations),
            "median_spearman": float(median(correlations)) if correlations else None,
            "min_spearman": min(correlations) if correlations else None,
            "max_spearman": max(correlations) if correlations else None,
        },
    }


def liquidity_experiment_summary(
    entities: list[dict[str, Any]], reference_period: int | None
) -> dict[str, Any]:
    quality_excluded = [
        entity for entity in entities if entity.get("quality_excluded_from_statistics")
    ]
    metric_summary: dict[str, Any] = {}
    for metric in LIQUIDITY_FORMULAS:
        state_counts = Counter()
        flag_counts = Counter()
        complete_windows = Counter()
        for entity in entities:
            current = (((entity.get("metrics") or {}).get(metric) or {}).get("current") or {})
            state_counts[current.get("state", "missing_current")] += 1
            flag_counts.update(current.get("flags") or [])
            history = ((entity.get("metrics") or {}).get(metric) or {}).get("history") or {}
            for months in HISTORY_WINDOWS:
                if (history.get(str(months)) or {}).get("complete_derivable"):
                    complete_windows[str(months)] += 1

        current_rows = _metric_current_rows(entities, metric)
        values = [float(item["value"]) for item in current_rows]
        stats = _robust_stats(values)
        lower = stats.get("tukey_lower_fence")
        upper = stats.get("tukey_upper_fence")
        metric_summary[metric] = {
            "description": LIQUIDITY_FORMULAS[metric]["description"],
            "current_state_counts_all_entities": dict(sorted(state_counts.items())),
            "current_flag_counts_all_entities": dict(sorted(flag_counts.items())),
            "current_distribution_excluding_quality_issues": stats,
            "current_bottom_10": sorted(current_rows, key=lambda item: item["value"])[:10],
            "current_top_10": sorted(
                current_rows, key=lambda item: item["value"], reverse=True
            )[:10],
            "current_tukey_outliers": {
                "below": [
                    item for item in current_rows if lower is not None and item["value"] < lower
                ],
                "above": [
                    item for item in current_rows if upper is not None and item["value"] > upper
                ],
            },
            "complete_derivable_history_counts": dict(complete_windows),
            "cross_section_rank_stability_vs_current": _cross_section_rank_stability(
                entities, metric, reference_period
            ),
        }

    return {
        "liquidity_experiment_version": LIQUIDITY_EXPERIMENT_VERSION,
        "reference_period": reference_period,
        "entity_count": len(entities),
        "quality_excluded_count": len(quality_excluded),
        "quality_excluded_entities": [
            {
                "entity_id": item.get("entity_id"),
                "fip_code": item.get("fip_code"),
                "legal_name": item.get("legal_name"),
                "duplicate_candidate_balance_cmpid_rows": item.get(
                    "duplicate_candidate_balance_cmpid_rows"
                ),
            }
            for item in quality_excluded
        ],
        "metrics": metric_summary,
        "current_ilc_ilt_correlation": _paired_current_correlation(entities),
        "methodology_note": (
            "Experimental calculations only. No liquidity value, distribution position, "
            "outlier flag or temporal statistic changes assessment eligibility, ranking "
            "eligibility or any score. Missing CMPIDs are not imputed as zero; non-positive "
            "denominators are not divided; entities with duplicate candidate CMPIDs are "
            "excluded from cross-sectional statistics pending source review."
        ),
    }


def validate_liquidity_experiment(payload: dict[str, Any]) -> None:
    errors: list[str] = []
    entities = payload.get("entities") or []
    summary = payload.get("summary") or {}
    if summary.get("entity_count") != len(entities):
        errors.append("summary entity_count mismatch")
    for entity in entities:
        for metric in LIQUIDITY_FORMULAS:
            metric_payload = ((entity.get("metrics") or {}).get(metric) or {})
            current = metric_payload.get("current")
            if current and current.get("state") == "derivable":
                value = _finite_number(current.get("value"))
                denominator = _finite_number(current.get("denominator"))
                if value is None:
                    errors.append(f"{entity.get('entity_id')} {metric}: non-finite value")
                if denominator is None or denominator <= 0:
                    errors.append(
                        f"{entity.get('entity_id')} {metric}: derivable with invalid denominator"
                    )
    forbidden = ("score", "rating", "ranking_eligible", "assessment_eligible")
    serialized_keys: list[str] = []

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                serialized_keys.append(str(key))
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(payload)
    for key in forbidden:
        if key in serialized_keys:
            errors.append(f"experimental liquidity artifact must not publish {key}")
    if errors:
        raise LiquidityExperimentInvariantError("; ".join(errors[:20]))
