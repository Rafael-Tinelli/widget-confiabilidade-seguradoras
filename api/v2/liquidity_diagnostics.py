from __future__ import annotations

import math
from typing import Any

import numpy as np
from scipy.stats import pearsonr, spearmanr

DESCRIPTIVE_BANDS: tuple[tuple[str, float | None, float | None], ...] = (
    ("below_0_50", None, 0.50),
    ("from_0_50_below_0_75", 0.50, 0.75),
    ("from_0_75_below_1_00", 0.75, 1.00),
    ("from_1_00_below_1_25", 1.00, 1.25),
    ("from_1_25_below_1_50", 1.25, 1.50),
    ("from_1_50_below_2_00", 1.50, 2.00),
    ("from_2_00_below_3_00", 2.00, 3.00),
    ("from_3_00_below_5_00", 3.00, 5.00),
    ("at_or_above_5_00", 5.00, None),
)


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _correlation(x: list[float], y: list[float]) -> dict[str, Any]:
    if len(x) < 3 or len(x) != len(y):
        return {"count": len(x), "pearson": None, "spearman": None}
    pearson = float(pearsonr(x, y).statistic)
    spearman = float(spearmanr(x, y).statistic)
    return {
        "count": len(x),
        "pearson": pearson if math.isfinite(pearson) else None,
        "spearman": spearman if math.isfinite(spearman) else None,
    }


def _percentiles(values: list[float]) -> dict[str, Any]:
    if not values:
        return {"count": 0}
    arr = np.asarray(values, dtype=float)
    ps = np.percentile(arr, [1, 5, 10, 25, 50, 75, 90, 95, 99])
    return {
        "count": int(arr.size),
        "min": float(np.min(arr)),
        "p01": float(ps[0]),
        "p05": float(ps[1]),
        "p10": float(ps[2]),
        "p25": float(ps[3]),
        "median": float(ps[4]),
        "p75": float(ps[5]),
        "p90": float(ps[6]),
        "p95": float(ps[7]),
        "p99": float(ps[8]),
        "max": float(np.max(arr)),
    }


def _current_rows(
    experiment_entities: list[dict[str, Any]], metric: str
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for entity in experiment_entities:
        if entity.get("quality_excluded_from_statistics"):
            continue
        current = (((entity.get("metrics") or {}).get(metric) or {}).get("current") or {})
        if current.get("state") != "derivable":
            continue
        value = _finite(current.get("value"))
        numerator = _finite(current.get("numerator"))
        denominator = _finite(current.get("denominator"))
        if value is None or denominator is None or denominator <= 0:
            continue
        rows.append(
            {
                "entity_id": entity.get("entity_id"),
                "fip_code": entity.get("fip_code"),
                "legal_name": entity.get("legal_name"),
                "value": value,
                "numerator": numerator,
                "denominator": denominator,
            }
        )
    return rows


def _band_counts(values: list[float]) -> dict[str, int]:
    result: dict[str, int] = {}
    for label, lower, upper in DESCRIPTIVE_BANDS:
        result[label] = sum(
            (lower is None or value >= lower) and (upper is None or value < upper)
            for value in values
        )
    result["below_1_00_total"] = sum(value < 1.0 for value in values)
    result["at_or_above_1_00_total"] = sum(value >= 1.0 for value in values)
    return result


def _capital_ratio(source_entity: dict[str, Any], reference_period: int | None) -> float | None:
    if not reference_period:
        return None
    current = (source_entity.get("capital_history") or {}).get(reference_period) or {}
    pla = _finite(current.get("pla_adjusted"))
    cmr = _finite(current.get("cmr"))
    if pla is None or cmr is None or cmr <= 0:
        return None
    ratio = pla / cmr
    return ratio if math.isfinite(ratio) else None


def _capital_redundancy(
    rows: list[dict[str, Any]],
    source_entities: dict[str, Any],
    reference_period: int | None,
) -> dict[str, Any]:
    paired: list[tuple[float, float]] = []
    for row in rows:
        fip = str(row.get("fip_code") or "").zfill(6)
        capital = _capital_ratio(source_entities.get(fip) or {}, reference_period)
        if capital is None:
            continue
        paired.append((float(row["value"]), capital))
    liquidity = [item[0] for item in paired]
    capital = [item[1] for item in paired]
    raw = _correlation(liquidity, capital)
    positive = [(liq, cap) for liq, cap in paired if liq > -1.0 and cap > -1.0]
    log_result = _correlation(
        [math.log1p(item[0]) for item in positive],
        [math.log1p(item[1]) for item in positive],
    )
    return {
        "paired_count": len(paired),
        "raw": raw,
        "log1p": log_result,
        "interpretation": (
            "Correlation is diagnostic only. Low or moderate correlation suggests liquidity "
            "is not merely a duplicate of current PLA/CMR; it does not prove independent "
            "predictive value or justify a score weight."
        ),
    }


def _denominator_diagnostics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    denominators = [float(row["denominator"]) for row in rows]
    values = [float(row["value"]) for row in rows]
    correlation = _correlation(values, denominators)
    return {
        "distribution": _percentiles(denominators),
        "ratio_vs_denominator_correlation": correlation,
        "smallest_denominators": sorted(rows, key=lambda item: item["denominator"])[:10],
        "interpretation": (
            "Very high liquidity ratios can be mechanically produced by small positive "
            "liability denominators. Raw magnitude must not be interpreted as linearly "
            "proportional financial strength."
        ),
    }


def _current_vs_history(
    experiment_entities: list[dict[str, Any]], metric: str
) -> dict[str, Any]:
    ratios: list[float] = []
    deviations: list[dict[str, Any]] = []
    for entity in experiment_entities:
        if entity.get("quality_excluded_from_statistics"):
            continue
        metric_payload = ((entity.get("metrics") or {}).get(metric) or {})
        current = metric_payload.get("current") or {}
        history = (metric_payload.get("history") or {}).get("12") or {}
        if current.get("state") != "derivable" or not history.get("complete_derivable"):
            continue
        current_value = _finite(current.get("value"))
        historical_median = _finite((history.get("stats") or {}).get("median"))
        if current_value is None or historical_median in {None, 0.0}:
            continue
        ratio = current_value / historical_median
        if not math.isfinite(ratio):
            continue
        ratios.append(ratio)
        deviations.append(
            {
                "entity_id": entity.get("entity_id"),
                "fip_code": entity.get("fip_code"),
                "legal_name": entity.get("legal_name"),
                "current": current_value,
                "median_12m": historical_median,
                "current_to_median_12m": ratio,
                "absolute_relative_deviation": abs(ratio - 1.0),
            }
        )
    return {
        "complete_entities": len(ratios),
        "current_to_median_12m_distribution": _percentiles(ratios),
        "largest_relative_deviations": sorted(
            deviations,
            key=lambda item: item["absolute_relative_deviation"],
            reverse=True,
        )[:15],
    }


def _non_derivable_current(
    experiment_entities: list[dict[str, Any]], metric: str
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for entity in experiment_entities:
        current = (((entity.get("metrics") or {}).get(metric) or {}).get("current") or {})
        if current.get("state") == "derivable":
            continue
        prior = [
            item
            for item in (((entity.get("metrics") or {}).get(metric) or {}).get("series_last_36") or [])
            if item.get("state") == "derivable"
        ]
        latest_prior = prior[-1] if prior else None
        result.append(
            {
                "entity_id": entity.get("entity_id"),
                "fip_code": entity.get("fip_code"),
                "legal_name": entity.get("legal_name"),
                "state": current.get("state") or "missing_current",
                "numerator": current.get("numerator"),
                "denominator": current.get("denominator"),
                "flags": list(current.get("flags") or []),
                "latest_prior_derivable": latest_prior,
            }
        )
    return result


def build_liquidity_diagnostics(
    experiment_entities: list[dict[str, Any]],
    source_payload: dict[str, Any],
) -> dict[str, Any]:
    source_entities = source_payload.get("entities") or {}
    reference_period = (source_payload.get("reference_periods") or {}).get("balance")
    metrics: dict[str, Any] = {}
    for metric in ("ILC", "ILT"):
        rows = _current_rows(experiment_entities, metric)
        values = [float(row["value"]) for row in rows]
        metrics[metric] = {
            "descriptive_bands": _band_counts(values),
            "denominator": _denominator_diagnostics(rows),
            "capital_pla_cmr_redundancy": _capital_redundancy(
                rows,
                source_entities,
                reference_period,
            ),
            "current_vs_12m_history": _current_vs_history(
                experiment_entities,
                metric,
            ),
            "non_derivable_current": _non_derivable_current(
                experiment_entities,
                metric,
            ),
        }
    return {
        "reference_period": reference_period,
        "metrics": metrics,
        "band_note": (
            "Bands around 1.0 are descriptive diagnostics, not pass/fail thresholds and not "
            "score zones. Their purpose is to inspect the empirical distribution around the "
            "asset-to-liability parity point before any methodology is selected."
        ),
    }
