from __future__ import annotations

import math
from collections import Counter
from statistics import median
from typing import Any

import numpy as np
from scipy.stats import spearmanr

from api.v2.financial_capital_semantics import capital_pla_cmr_ratio

OPERATING_EXPERIMENT_VERSION = "2.0-draft-operating-experiment-1"

OPERATING_BASE_TERMS: tuple[tuple[int, float], ...] = (
    (4027, 1.0),
    (7186, 1.0),
    (6238, 1.0),
    (6256, 1.0),
)
OPERATING_COST_TERMS: tuple[tuple[int, float], ...] = (
    (11232, 1.0),
    (11248, 1.0),
    (11237, 1.0),
    (11249, 1.0),
    (6202, 1.0),
    (11231, 1.0),
    (6261, 1.0),
    (11238, 1.0),
    (11250, 1.0),
    (4069, 1.0),
    (4070, 1.0),
)
FINANCIAL_RESULT_CMPID = 6322

COMPONENT_FORMULAS: dict[str, tuple[tuple[int, float], ...]] = {
    "ISR": ((11232, 1.0), (11248, 1.0)),
    "IDC": ((11237, 1.0), (11249, 1.0)),
    "IORDO": ((6202, 1.0), (11231, 1.0), (6261, 1.0)),
    "IRRES": ((11238, 1.0), (11250, 1.0)),
    "IDA": ((4069, 1.0), (4070, 1.0)),
}


class OperatingExperimentInvariantError(ValueError):
    """Raised when the experimental operating artifact violates its contract."""


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _sum_terms(
    values: dict[int, float], terms: tuple[tuple[int, float], ...]
) -> tuple[float | None, list[int]]:
    total = 0.0
    missing: list[int] = []
    for cmpid, coefficient in terms:
        value = _finite(values.get(cmpid))
        if value is None:
            missing.append(cmpid)
            continue
        total += coefficient * value
    return (None, sorted(missing)) if missing else (total, [])


def calculate_operating_observation(
    values: dict[int, float], metric: str
) -> dict[str, Any]:
    """Calculate IC or ICA from the SUSEP 2018 formula without imputing accounts."""
    if metric not in {"IC", "ICA"}:
        raise ValueError(f"unsupported operating metric: {metric}")

    raw_costs, missing_costs = _sum_terms(values, OPERATING_COST_TERMS)
    operating_base, missing_base = _sum_terms(values, OPERATING_BASE_TERMS)
    financial_result = _finite(values.get(FINANCIAL_RESULT_CMPID))
    missing_financial = [] if financial_result is not None else [FINANCIAL_RESULT_CMPID]
    missing = sorted(
        set(missing_costs + missing_base + (missing_financial if metric == "ICA" else []))
    )
    cost_total = -raw_costs if raw_costs is not None else None

    if missing:
        return {
            "state": "missing_components",
            "value": None,
            "cost_total": cost_total,
            "operating_base": operating_base,
            "financial_result": financial_result,
            "denominator": None,
            "missing_cmpids": missing,
            "flags": [],
            "components": {},
        }
    if raw_costs is None or operating_base is None:
        return {
            "state": "non_finite_components",
            "value": None,
            "cost_total": cost_total,
            "operating_base": operating_base,
            "financial_result": financial_result,
            "denominator": None,
            "missing_cmpids": [],
            "flags": [],
            "components": {},
        }

    denominator = operating_base
    if metric == "ICA":
        assert financial_result is not None
        denominator += financial_result

    flags: list[str] = []
    if operating_base <= 0:
        flags.append(
            "operating_base_zero" if operating_base == 0 else "operating_base_negative"
        )
    if metric == "ICA" and financial_result is not None and financial_result < 0:
        flags.append("negative_financial_result")

    if operating_base <= 0:
        return {
            "state": "non_positive_operating_base",
            "value": None,
            "cost_total": float(cost_total),
            "operating_base": float(operating_base),
            "financial_result": financial_result,
            "denominator": float(denominator),
            "missing_cmpids": [],
            "flags": flags,
            "components": {},
        }

    if denominator <= 0:
        flags.append("denominator_zero" if denominator == 0 else "denominator_negative")
        return {
            "state": "non_positive_denominator",
            "value": None,
            "cost_total": float(cost_total),
            "operating_base": float(operating_base),
            "financial_result": financial_result,
            "denominator": float(denominator),
            "missing_cmpids": [],
            "flags": flags,
            "components": {},
        }

    value = cost_total / denominator
    if not math.isfinite(value):
        return {
            "state": "non_finite_result",
            "value": None,
            "cost_total": float(cost_total),
            "operating_base": float(operating_base),
            "financial_result": financial_result,
            "denominator": float(denominator),
            "missing_cmpids": [],
            "flags": flags,
            "components": {},
        }

    components: dict[str, float] = {}
    if operating_base > 0:
        for name, terms in COMPONENT_FORMULAS.items():
            raw_component, missing_component = _sum_terms(values, terms)
            if raw_component is not None and not missing_component:
                components[name] = float(-raw_component / operating_base)

    if cost_total < 0:
        flags.append("negative_total_cost")
    return {
        "state": "derivable",
        "value": float(value),
        "cost_total": float(cost_total),
        "operating_base": float(operating_base),
        "financial_result": financial_result,
        "denominator": float(denominator),
        "missing_cmpids": [],
        "flags": flags,
        "components": components,
    }


def _invalidate_duplicate_period(
    observation: dict[str, Any], duplicate_rows: int
) -> dict[str, Any]:
    if duplicate_rows <= 0:
        return observation
    output = dict(observation)
    output["raw_state_before_quality_gate"] = output.get("state")
    output["raw_value_before_quality_gate"] = output.get("value")
    output["state"] = "source_duplicate_components"
    output["value"] = None
    output["duplicate_candidate_balance_cmpid_rows"] = duplicate_rows
    output["flags"] = list(
        dict.fromkeys(
            [*(output.get("flags") or []), "duplicate_candidate_balance_cmpids"]
        )
    )
    return output


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
        "mean": float(np.mean(arr)),
        "stddev_population": float(np.std(arr)),
    }


def _safe_correlation(x: list[float], y: list[float]) -> dict[str, Any]:
    if len(x) < 3 or len(x) != len(y):
        return {"count": len(x), "pearson": None, "spearman": None}
    x_arr = np.asarray(x, dtype=float)
    y_arr = np.asarray(y, dtype=float)
    pearson = float(np.corrcoef(x_arr, y_arr)[0, 1])
    spearman = float(spearmanr(x_arr, y_arr).statistic)
    return {
        "count": len(x),
        "pearson": pearson if math.isfinite(pearson) else None,
        "spearman": spearman if math.isfinite(spearman) else None,
    }


def build_entity_operating_experiment(
    entity: dict[str, Any],
    source_entity: dict[str, Any],
    reference_period: int | None,
) -> dict[str, Any]:
    balance_values = source_entity.get("balance_values") or {}
    duplicate_by_period = {
        int(period): int(count)
        for period, count in (
            source_entity.get("duplicate_balance_cmpid_rows_by_period") or {}
        ).items()
    }
    current_duplicate_count = int(duplicate_by_period.get(reference_period or -1, 0))
    metrics: dict[str, Any] = {}

    for metric in ("IC", "ICA"):
        series: list[dict[str, Any]] = []
        for period in sorted(int(item) for item in balance_values):
            observation = calculate_operating_observation(
                balance_values.get(period, {}), metric
            )
            observation = _invalidate_duplicate_period(
                observation, duplicate_by_period.get(period, 0)
            )
            series.append({"period": period, **observation})
        by_period = {int(item["period"]): item for item in series}
        comparable_periods = equivalent_month_periods(reference_period)
        year_end_periods = prior_year_end_periods(reference_period)
        metrics[metric] = {
            "current": by_period.get(reference_period or -1),
            "equivalent_month_history": [
                by_period[period] for period in comparable_periods if period in by_period
            ],
            "year_end_history": [
                by_period[period] for period in year_end_periods if period in by_period
            ],
            "series_last_48": series[-48:],
        }

    return {
        "entity_id": entity.get("entity_id"),
        "fip_code": entity.get("fip_code"),
        "legal_name": entity.get("legal_name"),
        "reference_period": reference_period,
        "quality_excluded_from_statistics": current_duplicate_count > 0,
        "quality_reason_codes": (
            ["duplicate_candidate_balance_cmpids_current_period"]
            if current_duplicate_count > 0
            else []
        ),
        "duplicate_candidate_balance_cmpid_rows_current": current_duplicate_count,
        "metrics": metrics,
    }


def _current_rows(entities: list[dict[str, Any]], metric: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for entity in entities:
        if entity.get("quality_excluded_from_statistics"):
            continue
        current = ((entity.get("metrics") or {}).get(metric) or {}).get("current") or {}
        value = _finite(current.get("value"))
        if current.get("state") != "derivable" or value is None:
            continue
        rows.append(
            {
                "entity_id": entity.get("entity_id"),
                "fip_code": entity.get("fip_code"),
                "legal_name": entity.get("legal_name"),
                "value": value,
                "operating_base": _finite(current.get("operating_base")),
                "financial_result": _finite(current.get("financial_result")),
                "denominator": _finite(current.get("denominator")),
                "cost_total": _finite(current.get("cost_total")),
                "components": dict(current.get("components") or {}),
            }
        )
    return rows


def _paired_metric_correlation(
    entities: list[dict[str, Any]], left: str, right: str
) -> dict[str, Any]:
    pairs: list[tuple[float, float]] = []
    for entity in entities:
        if entity.get("quality_excluded_from_statistics"):
            continue
        metrics = entity.get("metrics") or {}
        lrow = ((metrics.get(left) or {}).get("current") or {})
        rrow = ((metrics.get(right) or {}).get("current") or {})
        lv = _finite(lrow.get("value"))
        rv = _finite(rrow.get("value"))
        if (
            lrow.get("state") == "derivable"
            and rrow.get("state") == "derivable"
            and lv is not None
            and rv is not None
        ):
            pairs.append((lv, rv))
    return _safe_correlation([x for x, _ in pairs], [y for _, y in pairs])


def _same_month_rank_stability(
    entities: list[dict[str, Any]], metric: str, reference_period: int | None
) -> dict[str, Any]:
    if not reference_period:
        return {"periods": [], "summary": {"count": 0}}
    comparisons: list[dict[str, Any]] = []
    current: dict[str, float] = {}
    history: dict[str, dict[int, float]] = {}
    for entity in entities:
        if entity.get("quality_excluded_from_statistics"):
            continue
        metric_payload = ((entity.get("metrics") or {}).get(metric) or {})
        current_row = metric_payload.get("current") or {}
        current_value = _finite(current_row.get("value"))
        entity_id = str(entity.get("entity_id") or "")
        if current_row.get("state") == "derivable" and current_value is not None:
            current[entity_id] = current_value
        series: dict[int, float] = {}
        for row in metric_payload.get("series_last_48") or []:
            value = _finite(row.get("value"))
            if row.get("state") == "derivable" and value is not None:
                series[int(row["period"])] = value
        history[entity_id] = series

    for prior_period in equivalent_month_periods(reference_period, 4)[:-1]:
        common = [
            entity_id
            for entity_id in current
            if prior_period in history.get(entity_id, {})
        ]
        result = _safe_correlation(
            [current[item] for item in common],
            [history[item][prior_period] for item in common],
        )
        comparisons.append(
            {
                "period": prior_period,
                "common_entities": len(common),
                "spearman": result.get("spearman"),
            }
        )
    values = [
        float(item["spearman"])
        for item in comparisons
        if item.get("spearman") is not None
    ]
    return {
        "periods": comparisons,
        "summary": {
            "count": len(values),
            "median_spearman": float(median(values)) if values else None,
            "min_spearman": min(values) if values else None,
            "max_spearman": max(values) if values else None,
        },
    }


def _year_end_rank_stability(
    entities: list[dict[str, Any]], metric: str, reference_period: int | None
) -> dict[str, Any]:
    periods = prior_year_end_periods(reference_period, 3)
    if len(periods) < 2:
        return {"reference_year_end": None, "periods": [], "summary": {"count": 0}}
    reference_year_end = periods[-1]
    by_entity: dict[str, dict[int, float]] = {}
    for entity in entities:
        if entity.get("quality_excluded_from_statistics"):
            continue
        metric_payload = ((entity.get("metrics") or {}).get(metric) or {})
        series: dict[int, float] = {}
        for row in metric_payload.get("series_last_48") or []:
            value = _finite(row.get("value"))
            if row.get("state") == "derivable" and value is not None:
                series[int(row["period"])] = value
        by_entity[str(entity.get("entity_id") or "")] = series

    comparisons: list[dict[str, Any]] = []
    for prior_period in periods[:-1]:
        common = [
            entity_id
            for entity_id, series in by_entity.items()
            if reference_year_end in series and prior_period in series
        ]
        result = _safe_correlation(
            [by_entity[item][reference_year_end] for item in common],
            [by_entity[item][prior_period] for item in common],
        )
        comparisons.append(
            {
                "period": prior_period,
                "common_entities": len(common),
                "spearman": result.get("spearman"),
            }
        )
    values = [
        float(item["spearman"])
        for item in comparisons
        if item.get("spearman") is not None
    ]
    return {
        "reference_year_end": reference_year_end,
        "periods": comparisons,
        "summary": {
            "count": len(values),
            "median_spearman": float(median(values)) if values else None,
            "min_spearman": min(values) if values else None,
            "max_spearman": max(values) if values else None,
        },
    }


def _capital_ratio(source_entity: dict[str, Any], period: int | None) -> float | None:
    if not period:
        return None
    row = (source_entity.get("capital_history") or {}).get(period) or {}
    return capital_pla_cmr_ratio(row)


def _ilt_value(source_entity: dict[str, Any], period: int | None) -> float | None:
    if not period:
        return None
    from api.v2.liquidity_experiment import calculate_liquidity_observation

    row = calculate_liquidity_observation(
        (source_entity.get("balance_values") or {}).get(period, {}), "ILT"
    )
    return _finite(row.get("value")) if row.get("state") == "derivable" else None


def _external_redundancy(
    rows: list[dict[str, Any]],
    source_entities: dict[str, Any],
    reference_period: int | None,
    kind: str,
) -> dict[str, Any]:
    pairs: list[tuple[float, float]] = []
    for row in rows:
        source_entity = source_entities.get(str(row.get("fip_code") or "").zfill(6), {})
        comparator = (
            _capital_ratio(source_entity, reference_period)
            if kind == "capital_pla_cmr"
            else _ilt_value(source_entity, reference_period)
        )
        if comparator is None:
            continue
        pairs.append((float(row["value"]), comparator))
    return _safe_correlation([x for x, _ in pairs], [y for _, y in pairs])


def _financial_result_effect(rows: list[dict[str, Any]]) -> dict[str, Any]:
    shares: list[float] = []
    positive = negative = zero = 0
    for row in rows:
        base = row.get("operating_base")
        financial = row.get("financial_result")
        if base is None or base <= 0 or financial is None:
            continue
        share = financial / base
        if math.isfinite(share):
            shares.append(float(share))
        if financial > 0:
            positive += 1
        elif financial < 0:
            negative += 1
        else:
            zero += 1
    return {
        "financial_result_sign_counts": {
            "positive": positive,
            "negative": negative,
            "zero": zero,
        },
        "financial_result_to_operating_base": _robust_stats(shares),
    }


def _decomposition_error(rows: list[dict[str, Any]]) -> dict[str, Any]:
    errors: list[float] = []
    for row in rows:
        components = row.get("components") or {}
        if set(components) != set(COMPONENT_FORMULAS):
            continue
        errors.append(
            abs(float(row["value"]) - sum(float(v) for v in components.values()))
        )
    return {
        "count": len(errors),
        "max_absolute_error": max(errors) if errors else None,
        "median_absolute_error": float(median(errors)) if errors else None,
    }


def _operating_base_value(source_entity: dict[str, Any], period: int) -> float | None:
    values = (source_entity.get("balance_values") or {}).get(period, {})
    base, missing = _sum_terms(values, OPERATING_BASE_TERMS)
    if missing or base is None or not math.isfinite(base) or base <= 0:
        return None
    return float(base)


def _cumulative_reporting_diagnostic(
    source_entities: dict[str, Any], reference_period: int | None
) -> dict[str, Any]:
    if not reference_period:
        return {}
    year = reference_period // 100
    month = reference_period % 100
    january = year * 100 + 1
    prior_december = (year - 1) * 100 + 12
    jan_to_reference: list[float] = []
    jan_to_prior_december: list[float] = []
    nondecreasing = total_transitions = 0

    for source_entity in source_entities.values():
        jan = _operating_base_value(source_entity, january)
        current = _operating_base_value(source_entity, reference_period)
        prior_dec = _operating_base_value(source_entity, prior_december)
        if jan is not None and current is not None and jan > 0:
            jan_to_reference.append(current / jan)
        if jan is not None and prior_dec is not None and prior_dec > 0:
            jan_to_prior_december.append(jan / prior_dec)

        previous = None
        for current_month in range(1, month + 1):
            period = year * 100 + current_month
            value = _operating_base_value(source_entity, period)
            if value is None:
                previous = None
                continue
            if previous is not None:
                total_transitions += 1
                if value >= previous:
                    nondecreasing += 1
            previous = value

    return {
        "reference_year": year,
        "reference_month": month,
        "january_to_reference_base_ratio": _robust_stats(jan_to_reference),
        "january_to_prior_december_base_ratio": _robust_stats(jan_to_prior_december),
        "same_year_positive_base_nondecreasing_transition_rate": (
            nondecreasing / total_transitions if total_transitions else None
        ),
        "same_year_transition_count": total_transitions,
        "interpretation": (
            "This is an empirical reporting-pattern diagnostic, not an accounting rule. "
            "A strong within-year accumulation pattern together with a January reset supports "
            "comparing IC/ICA at equivalent horizons such as May/May or December/December, "
            "rather than treating adjacent SES months as independent monthly results."
        ),
    }


def operating_experiment_summary(
    entities: list[dict[str, Any]],
    source_payload: dict[str, Any],
    reference_period: int | None,
) -> dict[str, Any]:
    source_entities = source_payload.get("entities") or {}
    metrics: dict[str, Any] = {}
    rows_by_metric: dict[str, list[dict[str, Any]]] = {}
    for metric in ("IC", "ICA"):
        rows = _current_rows(entities, metric)
        rows_by_metric[metric] = rows
        values = [float(row["value"]) for row in rows]
        states = Counter(
            (
                (
                    ((entity.get("metrics") or {}).get(metric) or {}).get("current")
                    or {}
                ).get("state")
                or "missing_current"
            )
            for entity in entities
        )
        metrics[metric] = {
            "current_state_counts": dict(states),
            "current_distribution": _robust_stats(values),
            "below_1_count": sum(value < 1.0 for value in values),
            "at_or_above_1_count": sum(value >= 1.0 for value in values),
            "same_month_rank_stability": _same_month_rank_stability(
                entities, metric, reference_period
            ),
            "year_end_rank_stability": _year_end_rank_stability(
                entities, metric, reference_period
            ),
            "capital_pla_cmr_correlation": _external_redundancy(
                rows, source_entities, reference_period, "capital_pla_cmr"
            ),
            "ilt_correlation": _external_redundancy(
                rows, source_entities, reference_period, "ilt"
            ),
            "smallest_positive_operating_bases": sorted(
                [row for row in rows if (row.get("operating_base") or 0) > 0],
                key=lambda item: float(item["operating_base"]),
            )[:10],
        }

    ic_rows = {str(row["entity_id"]): row for row in rows_by_metric["IC"]}
    ica_rows = {str(row["entity_id"]): row for row in rows_by_metric["ICA"]}
    common = sorted(set(ic_rows) & set(ica_rows))
    crossings = Counter()
    shifts: list[float] = []
    for entity_id in common:
        ic = float(ic_rows[entity_id]["value"])
        ica = float(ica_rows[entity_id]["value"])
        if ic >= 1.0 and ica < 1.0:
            crossings["ic_ge_1_ica_lt_1"] += 1
        elif ic < 1.0 and ica >= 1.0:
            crossings["ic_lt_1_ica_ge_1"] += 1
        shifts.append(ica - ic)

    summary = {
        "version": OPERATING_EXPERIMENT_VERSION,
        "reference_period": reference_period,
        "entity_count": len(entities),
        "quality_excluded_count": sum(
            bool(entity.get("quality_excluded_from_statistics")) for entity in entities
        ),
        "metrics": metrics,
        "current_ic_ica_correlation": _paired_metric_correlation(entities, "IC", "ICA"),
        "ic_ica_threshold_crossings": dict(crossings),
        "ica_minus_ic_distribution": _robust_stats(shifts),
        "financial_result_effect": _financial_result_effect(rows_by_metric["IC"]),
        "cumulative_reporting_diagnostic": _cumulative_reporting_diagnostic(
            source_entities, reference_period
        ),
        "ic_component_decomposition_identity": _decomposition_error(rows_by_metric["IC"]),
        "interpretation": (
            "The 1.0 point is an arithmetic cost/revenue parity reference, not a SUSEP "
            "regulatory pass/fail threshold. IC and ICA remain experimental diagnostics. "
            "Cost subindices decompose IC and must not be treated as independent pillars."
        ),
    }
    return summary


def validate_operating_experiment(payload: dict[str, Any]) -> None:
    if payload.get("status") != "experimental":
        raise OperatingExperimentInvariantError("operating artifact must remain experimental")
    forbidden = {"score", "rating", "assessment_eligible", "ranking_eligible"}

    def walk(value: Any) -> None:
        if isinstance(value, dict):
            overlap = forbidden & set(value)
            if overlap:
                raise OperatingExperimentInvariantError(
                    f"forbidden decision fields in operating experiment: {sorted(overlap)}"
                )
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(payload)
