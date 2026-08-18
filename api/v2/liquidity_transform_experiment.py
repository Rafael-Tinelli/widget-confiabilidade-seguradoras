from __future__ import annotations

import math
from statistics import median
from typing import Any

import numpy as np
from scipy.stats import rankdata, spearmanr

from api.v2.financial_evidence import month_window

LIQUIDITY_TRANSFORM_EXPERIMENT_VERSION = "2.0-draft-liquidity-transform-1"
HARD_CAPS = (2.0, 3.0, 5.0)
TANH_TAUS = (0.75, 1.0, 1.5)
HISTORY_CURRENT_WEIGHTS = (0.0, 0.25, 0.50, 0.75)
REFERENCE_RATIOS = (0.25, 0.50, 0.75, 1.0, 1.25, 1.5, 2.0, 3.0, 5.0, 10.0, 100.0, 600.0)


class LiquidityTransformExperimentInvariantError(ValueError):
    """Raised when the transform experiment violates its non-scoring contract."""


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def hard_log_saturation(ratio: float, cap: float) -> float | None:
    """Map a positive ratio to [0, 1] with reciprocal hard floor/cap.

    The transform is symmetric in multiplicative log space around parity (1.0).
    It is experimental geometry only; cap values are not prudential thresholds.
    """
    ratio = _finite(ratio)
    if ratio is None or ratio <= 0 or cap <= 1:
        return None
    normalized = math.log(ratio) / math.log(cap)
    clipped = min(1.0, max(-1.0, normalized))
    return 0.5 + (0.5 * clipped)


def tanh_log_transform(ratio: float, tau: float) -> float | None:
    """Map a positive ratio continuously to (0, 1), centered on parity."""
    ratio = _finite(ratio)
    if ratio is None or ratio <= 0 or tau <= 0:
        return None
    return 0.5 + (0.5 * math.tanh(math.log(ratio) / tau))


def geometric_history_ratio(
    current: float,
    history_median: float,
    current_weight: float,
) -> float | None:
    """Blend current and historical level multiplicatively in log space."""
    current = _finite(current)
    history_median = _finite(history_median)
    if (
        current is None
        or history_median is None
        or current <= 0
        or history_median <= 0
        or not 0 <= current_weight <= 1
    ):
        return None
    log_value = (
        current_weight * math.log(current)
        + (1.0 - current_weight) * math.log(history_median)
    )
    return math.exp(log_value)


def _hard_name(cap: float) -> str:
    return f"hard_log_cap_{str(cap).replace('.', '_')}"


def _tanh_name(tau: float) -> str:
    return f"tanh_log_tau_{str(tau).replace('.', '_')}"


def _history_name(weight: float) -> str:
    pct = round(weight * 100)
    return f"history_geo_current_{pct:03d}"


def transform_specs() -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = [
        {
            "name": "raw_ratio",
            "family": "baseline",
            "bounded": False,
            "uses_history": False,
        }
    ]
    specs.extend(
        {
            "name": _hard_name(cap),
            "family": "hard_log_saturation",
            "bounded": True,
            "uses_history": False,
            "cap": cap,
        }
        for cap in HARD_CAPS
    )
    specs.extend(
        {
            "name": _tanh_name(tau),
            "family": "continuous_tanh_log",
            "bounded": True,
            "uses_history": False,
            "tau": tau,
        }
        for tau in TANH_TAUS
    )
    specs.extend(
        {
            "name": _history_name(weight),
            "family": "history_geometric_12m_tanh_log",
            "bounded": True,
            "uses_history": True,
            "current_weight": weight,
            "tau": 1.0,
        }
        for weight in HISTORY_CURRENT_WEIGHTS
    )
    return specs


def _series_map(entity: dict[str, Any], metric: str) -> dict[int, float]:
    metric_payload = ((entity.get("metrics") or {}).get(metric) or {})
    result: dict[int, float] = {}
    for row in metric_payload.get("series_last_36") or []:
        if row.get("state") != "derivable":
            continue
        value = _finite(row.get("value"))
        if value is not None:
            result[int(row["period"])] = value
    return result


def _rolling_median_12(series: dict[int, float], period: int) -> float | None:
    window = month_window(period, 12)
    values = [series.get(item) for item in window]
    if any(value is None for value in values):
        return None
    finite_values = [_finite(value) for value in values]
    if any(value is None for value in finite_values):
        return None
    return float(median(float(value) for value in finite_values if value is not None))


def _apply_spec(
    ratio: float,
    spec: dict[str, Any],
    history_median: float | None,
) -> float | None:
    family = spec["family"]
    if family == "baseline":
        return _finite(ratio)
    if family == "hard_log_saturation":
        return hard_log_saturation(ratio, float(spec["cap"]))
    if family == "continuous_tanh_log":
        return tanh_log_transform(ratio, float(spec["tau"]))
    if family == "history_geometric_12m_tanh_log":
        if history_median is None:
            return None
        blended = geometric_history_ratio(
            ratio,
            history_median,
            float(spec["current_weight"]),
        )
        if blended is None:
            return None
        return tanh_log_transform(blended, float(spec["tau"]))
    raise KeyError(f"Unknown transform family: {family}")


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
        "iqr": float(ps[5] - ps[3]),
    }


def _safe_spearman(x: list[float], y: list[float]) -> float | None:
    if len(x) < 3 or len(x) != len(y):
        return None
    result = float(spearmanr(x, y).statistic)
    return result if math.isfinite(result) else None


def _rank_shift(
    raw_current: dict[str, float],
    transformed_current: dict[str, float],
) -> dict[str, Any]:
    common = sorted(set(raw_current) & set(transformed_current))
    if not common:
        return {"count": 0}
    raw_values = np.asarray([raw_current[item] for item in common], dtype=float)
    transformed_values = np.asarray(
        [transformed_current[item] for item in common], dtype=float
    )
    raw_ranks = rankdata(-raw_values, method="average")
    transformed_ranks = rankdata(-transformed_values, method="average")
    shifts = np.abs(raw_ranks - transformed_ranks)
    movers = sorted(
        (
            {
                "entity_id": entity_id,
                "raw_rank": float(raw_rank),
                "transformed_rank": float(transformed_rank),
                "absolute_rank_shift": float(shift),
            }
            for entity_id, raw_rank, transformed_rank, shift in zip(
                common,
                raw_ranks,
                transformed_ranks,
                shifts,
                strict=True,
            )
        ),
        key=lambda item: item["absolute_rank_shift"],
        reverse=True,
    )
    return {
        "count": len(common),
        "mean_absolute_rank_shift": float(np.mean(shifts)),
        "p90_absolute_rank_shift": float(np.percentile(shifts, 90)),
        "max_absolute_rank_shift": float(np.max(shifts)),
        "largest_movers": movers[:15],
    }


def _resolution(values: list[float], bounded: bool) -> dict[str, Any]:
    if not values:
        return {"count": 0}
    rounded = [round(value, 12) for value in values]
    counts: dict[float, int] = {}
    for value in rounded:
        counts[value] = counts.get(value, 0) + 1
    repeated = [count for count in counts.values() if count > 1]
    result: dict[str, Any] = {
        "count": len(values),
        "unique_values": len(counts),
        "tie_groups": len(repeated),
        "largest_tie_group": max(repeated) if repeated else 1,
    }
    if bounded:
        result.update(
            {
                "exact_floor_count": sum(value <= 1e-12 for value in values),
                "exact_ceiling_count": sum(value >= 1.0 - 1e-12 for value in values),
                "near_floor_0_01_count": sum(value <= 0.01 for value in values),
                "near_ceiling_0_99_count": sum(value >= 0.99 for value in values),
            }
        )
    return result


def _rank_stability(
    signals: dict[str, dict[int, float]],
    reference_period: int,
) -> dict[str, Any]:
    current = {
        entity_id: periods[reference_period]
        for entity_id, periods in signals.items()
        if reference_period in periods
    }
    months: list[dict[str, Any]] = []
    for period in month_window(reference_period, 12):
        if period == reference_period:
            continue
        common = [
            entity_id
            for entity_id in current
            if period in signals.get(entity_id, {})
        ]
        corr = _safe_spearman(
            [current[item] for item in common],
            [signals[item][period] for item in common],
        )
        months.append(
            {
                "period": period,
                "common_entities": len(common),
                "spearman": corr,
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
            "median_spearman": (
                float(median(correlations)) if correlations else None
            ),
            "min_spearman": min(correlations) if correlations else None,
            "max_spearman": max(correlations) if correlations else None,
        },
    }


def _shape_diagnostics(spec: dict[str, Any]) -> list[dict[str, Any]]:
    if spec["family"] not in {"hard_log_saturation", "continuous_tanh_log"}:
        return []
    rows: list[dict[str, Any]] = []
    for ratio in REFERENCE_RATIOS:
        value = _apply_spec(ratio, spec, None)
        plus_10 = _apply_spec(ratio * 1.10, spec, None)
        rows.append(
            {
                "ratio": ratio,
                "transformed_value": value,
                "delta_for_plus_10pct_ratio": (
                    plus_10 - value
                    if value is not None and plus_10 is not None
                    else None
                ),
            }
        )
    return rows


def _history_response_scenarios(weight: float) -> list[dict[str, float]]:
    scenarios = (0.25, 0.50, 0.75, 1.0, 1.25, 2.0, 4.0, 8.0)
    return [
        {
            "current_to_history_ratio": ratio,
            "effective_to_history_ratio": ratio**weight,
        }
        for ratio in scenarios
    ]


def build_liquidity_transform_experiment(
    liquidity_payload: dict[str, Any],
) -> dict[str, Any]:
    summary = liquidity_payload.get("summary") or {}
    reference_period = int(summary.get("reference_period") or 0)
    if not reference_period:
        raise LiquidityTransformExperimentInvariantError(
            "liquidity experiment has no reference period"
        )
    specs = transform_specs()
    entities = liquidity_payload.get("entities") or []
    entity_names = {
        str(entity.get("entity_id")): entity.get("legal_name") for entity in entities
    }
    output_metrics: dict[str, Any] = {}

    for metric in ("ILC", "ILT"):
        series_by_entity = {
            str(entity.get("entity_id")): _series_map(entity, metric)
            for entity in entities
        }
        signals_by_transform: dict[str, dict[str, dict[int, float]]] = {}
        for spec in specs:
            transform_signals: dict[str, dict[int, float]] = {}
            for entity_id, series in series_by_entity.items():
                period_signals: dict[int, float] = {}
                for period, ratio in series.items():
                    history_median = (
                        _rolling_median_12(series, period)
                        if spec["uses_history"]
                        else None
                    )
                    transformed = _apply_spec(ratio, spec, history_median)
                    if transformed is not None and math.isfinite(transformed):
                        period_signals[period] = float(transformed)
                transform_signals[entity_id] = period_signals
            signals_by_transform[spec["name"]] = transform_signals

        raw_current = {
            entity_id: periods[reference_period]
            for entity_id, periods in signals_by_transform["raw_ratio"].items()
            if reference_period in periods
        }
        transform_summaries: dict[str, Any] = {}
        for spec in specs:
            name = spec["name"]
            signals = signals_by_transform[name]
            current = {
                entity_id: periods[reference_period]
                for entity_id, periods in signals.items()
                if reference_period in periods
            }
            current_values = list(current.values())
            common = sorted(set(raw_current) & set(current))
            raw_corr = _safe_spearman(
                [raw_current[item] for item in common],
                [current[item] for item in common],
            )
            rank_shift = _rank_shift(raw_current, current)
            for mover in rank_shift.get("largest_movers") or []:
                mover["legal_name"] = entity_names.get(mover["entity_id"])
            transform_payload: dict[str, Any] = {
                "family": spec["family"],
                "parameters": {
                    key: value
                    for key, value in spec.items()
                    if key not in {"name", "family", "bounded", "uses_history"}
                },
                "bounded": bool(spec["bounded"]),
                "uses_history": bool(spec["uses_history"]),
                "current_distribution": _percentiles(current_values),
                "current_resolution": _resolution(
                    current_values,
                    bool(spec["bounded"]),
                ),
                "current_rank_spearman_vs_raw": raw_corr,
                "current_rank_shift_vs_raw": rank_shift,
                "rank_stability_vs_current": _rank_stability(
                    signals,
                    reference_period,
                ),
                "shape_reference": _shape_diagnostics(spec),
            }
            if spec["family"] == "history_geometric_12m_tanh_log":
                transform_payload["history_response_scenarios"] = (
                    _history_response_scenarios(float(spec["current_weight"]))
                )
            transform_summaries[name] = transform_payload

        output_metrics[metric] = {
            "raw_current_count": len(raw_current),
            "transforms": transform_summaries,
        }

    payload = {
        "artifact": "v2_liquidity_transform_experiment",
        "status": "experimental",
        "experiment_version": LIQUIDITY_TRANSFORM_EXPERIMENT_VERSION,
        "reference_period": reference_period,
        "methodology_note": (
            "Transformations are experimental diagnostics, not approved score functions. "
            "Parity at 1.0 is used only as a mathematical center. Hard caps, tanh tau values "
            "and history weights are candidate geometries tested for saturation, resolution "
            "and temporal behavior; none is a prudential threshold or production weight."
        ),
        "metrics": output_metrics,
    }
    validate_liquidity_transform_experiment(payload)
    return payload


def validate_liquidity_transform_experiment(payload: dict[str, Any]) -> None:
    errors: list[str] = []
    if payload.get("status") != "experimental":
        errors.append("transform artifact must remain experimental")
    if not payload.get("reference_period"):
        errors.append("reference period missing")
    forbidden = {"score", "rating", "assessment_eligible", "ranking_eligible"}

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                if str(key) in forbidden:
                    errors.append(f"forbidden scoring key published: {key}")
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)
        elif isinstance(value, float) and not math.isfinite(value):
            errors.append("non-finite float published")

    walk(payload)
    if errors:
        raise LiquidityTransformExperimentInvariantError("; ".join(errors[:20]))
