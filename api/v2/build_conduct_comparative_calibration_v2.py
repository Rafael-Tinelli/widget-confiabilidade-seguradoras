from __future__ import annotations

import json
import math
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from api.sources.susep_insurance_exposure import (
    DEFAULT_SES_ZIP,
    SES_TABLE_DOCUMENTATION_URL,
    load_susep_insurance_exposure,
)
from api.v2.conduct_comparative import (
    branch_mix,
    branch_mix_distance,
    expected_complaints,
    persistence_diagnostics,
    pressure_ratio,
)
from api.v2.conduct_monthly_pressure import (
    ZERO_MARKET_COMPLAINTS_POLICY,
    ZERO_MARKET_COMPLAINTS_STATE,
    zero_market_complaints_guard,
)

CONDUCT_PATH = Path("data/derived/v2/consumer_gov_conduct_evidence.json")
RECONCILIATION_PATH = Path("data/derived/v2/conduct_coverage_reconciliation.json")
OUTPUT_PATH = Path("data/derived/v2/conduct_comparative_calibration_v2.json")
VERSION = "2.0-draft-conduct-comparative-calibration-5"
CANDIDATE_STATE = "direct_one_to_one_candidate"
ALIGNED_POLICY = "sum_monthly_expected_then_observed_divided_by_expected"


class ConductComparativeCalibrationV2Error(RuntimeError):
    """Raised when comparative Conduct inputs cannot be aligned safely."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _period(month: str) -> int:
    text = str(month or "").strip().replace("-", "")
    if len(text) != 6 or not text.isdigit():
        raise ConductComparativeCalibrationV2Error(f"invalid month: {month!r}")
    year = int(text[:4])
    number = int(text[4:])
    if year < 2000 or not 1 <= number <= 12:
        raise ConductComparativeCalibrationV2Error(f"invalid month: {month!r}")
    return int(text)


def _next_period(period: int) -> int:
    year, month = divmod(period, 100)
    return (year + 1) * 100 + 1 if month == 12 else year * 100 + month + 1


def _validated_periods(months: list[str]) -> list[int]:
    if not months or len(months) != len(set(months)):
        raise ConductComparativeCalibrationV2Error(
            "Conduct comparison months are missing or duplicated"
        )
    periods = [_period(month) for month in months]
    if periods != sorted(periods):
        raise ConductComparativeCalibrationV2Error(
            "Conduct comparison months must be chronological"
        )
    if any(
        periods[index] != _next_period(periods[index - 1])
        for index in range(1, len(periods))
    ):
        raise ConductComparativeCalibrationV2Error(
            "Conduct comparison months must be consecutive"
        )
    return periods


def _finite(value: Any, *, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ConductComparativeCalibrationV2Error(
            f"non-numeric {field}: {value!r}"
        ) from exc
    if not math.isfinite(number):
        raise ConductComparativeCalibrationV2Error(f"non-finite {field}: {value!r}")
    return number


def _nonnegative_int(value: Any, *, field: str) -> int:
    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise ConductComparativeCalibrationV2Error(
            f"non-integer {field}: {value!r}"
        ) from exc
    if not math.isfinite(numeric) or not numeric.is_integer():
        raise ConductComparativeCalibrationV2Error(
            f"non-integer {field}: {value!r}"
        )
    number = int(numeric)
    if number < 0:
        raise ConductComparativeCalibrationV2Error(f"negative {field}: {number}")
    return number


def _quantiles(values: list[float]) -> dict[str, float | None]:
    finite = [float(value) for value in values if math.isfinite(float(value))]
    if not finite:
        return {key: None for key in ("min", "p10", "p25", "p50", "p75", "p90", "max")}
    return {
        "min": min(finite),
        "p10": float(np.percentile(finite, 10)),
        "p25": float(np.percentile(finite, 25)),
        "p50": float(np.percentile(finite, 50)),
        "p75": float(np.percentile(finite, 75)),
        "p90": float(np.percentile(finite, 90)),
        "max": max(finite),
    }


def _candidate_rows(
    reconciliation: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    candidates: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    for row in reconciliation.get("entities") or []:
        pressure = row.get("pressure_comparability") or {}
        state = str(pressure.get("state") or "")
        if state == CANDIDATE_STATE and bool(pressure.get("pressure_eligible_candidate")):
            exposure = row.get("insurance_exposure_12m") or {}
            if _nonnegative_int(
                exposure.get("insurance_premium_direct_missing_rows") or 0,
                field="insurance_premium_direct_missing_rows",
            ) > 0:
                raise ConductComparativeCalibrationV2Error(
                    "direct one-to-one candidate has incomplete direct-premium exposure: "
                    f"{row.get('entity_id')}"
                )
            candidates.append(row)
            continue
        excluded.append(
            {
                "entity_id": row.get("entity_id"),
                "fip_code": row.get("fip_code"),
                "cnpj": row.get("cnpj"),
                "legal_name": row.get("legal_name"),
                "complaints_12m": row.get("complaints_12m"),
                "state": state,
                "reason_code": pressure.get("reason_code"),
            }
        )
    if not candidates:
        raise ConductComparativeCalibrationV2Error(
            "reconciliation has no direct one-to-one candidates"
        )
    return candidates, excluded


def _conduct_index(conduct: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {
        str(row.get("entity_id")): row
        for row in conduct.get("entities") or []
        if row.get("entity_id")
    }


def _monthly_conduct(
    entity: dict[str, Any], months: list[str]
) -> dict[str, dict[str, Any]]:
    rows = list(entity.get("monthly") or [])
    by_month = {
        str(row.get("month") or ""): row
        for row in rows
        if str(row.get("month") or "")
    }
    if len(by_month) != len(rows):
        raise ConductComparativeCalibrationV2Error(
            f"Conduct entity {entity.get('entity_id')} has duplicate/invalid monthly rows"
        )
    missing = [month for month in months if month not in by_month]
    extra = sorted(set(by_month) - set(months))
    if missing:
        raise ConductComparativeCalibrationV2Error(
            f"Conduct entity {entity.get('entity_id')} misses months: {missing}"
        )
    if extra:
        raise ConductComparativeCalibrationV2Error(
            f"Conduct entity {entity.get('entity_id')} has unexpected months: {extra}"
        )
    return by_month


def _ses_month(entity: dict[str, Any], period: int) -> dict[str, Any]:
    months = entity.get("months") or {}
    return months.get(period) or months.get(str(period)) or {}


def _branch_totals(
    ses_entity: dict[str, Any], periods: list[int]
) -> dict[str, float]:
    totals: Counter[str] = Counter()
    for period in periods:
        for branch, values in (
            _ses_month(ses_entity, period).get("insurance_branches") or {}
        ).items():
            totals[str(branch)] += _finite(
                (values or {}).get("premium_direct") or 0.0,
                field="premium_direct",
            )
    return {key: float(value) for key, value in sorted(totals.items())}


def _portfolio_diagnostics(
    branches: dict[str, float], market_branches: dict[str, float]
) -> dict[str, Any]:
    mix = branch_mix(branches)
    return {
        "positive_branch_mix": mix,
        "positive_branch_count": len(mix),
        "hhi": float(sum(value * value for value in mix.values())) if mix else None,
        "top_branch_share": float(max(mix.values())) if mix else None,
        "distance_from_market_mix": branch_mix_distance(branches, market_branches),
        "negative_branch_total_count": sum(value < 0 for value in branches.values()),
        "zero_branch_total_count": sum(value == 0 for value in branches.values()),
    }


def _satisfaction_diagnostics(entity: dict[str, Any]) -> dict[str, Any]:
    totals = entity.get("totals") or {}
    return {
        "sample_count": _nonnegative_int(
            totals.get("satisfaction_count") or 0,
            field="satisfaction_count",
        ),
        "average": totals.get("average_satisfaction"),
        "trend": (entity.get("film") or {}).get("satisfaction_trend"),
        "sample_aware": True,
    }


def _small_sample_bucket(observed: int) -> str:
    if observed < 5:
        return "0_4"
    if observed < 20:
        return "5_19"
    if observed < 100:
        return "20_99"
    return "100_plus"


def _small_sample_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    buckets = {key: [] for key in ("0_4", "5_19", "20_99", "100_plus")}
    for row in rows:
        aligned_observed = int(row["pressure_12m"]["observed_complaints"])
        buckets[_small_sample_bucket(aligned_observed)].append(row)
    return {
        key: {
            "entities": len(items),
            "sample_basis": "pressure_12m.observed_complaints",
            "complaints_total": sum(
                int(item["pressure_12m"]["observed_complaints"])
                for item in items
            ),
            "total_evidence_complaints": sum(
                int(item["complaints_12m"])
                for item in items
            ),
            "expected_complaints_quantiles": _quantiles(
                [float(item["pressure_12m"]["expected_complaints"]) for item in items]
            ),
            "raw_pressure_ratio_quantiles": _quantiles(
                [float(item["pressure_12m"]["ratio"]) for item in items]
            ),
        }
        for key, items in buckets.items()
    }


def _nearest_mix_peers(rows: list[dict[str, Any]], count: int = 5) -> None:
    by_id = {str(row["entity_id"]): row for row in rows}
    for entity_id, row in by_id.items():
        left = row["portfolio_12m"]["branch_premium_direct"]
        distances: list[tuple[float, str]] = []
        for other_id, other in by_id.items():
            if other_id == entity_id:
                continue
            distance = branch_mix_distance(
                left, other["portfolio_12m"]["branch_premium_direct"]
            )
            if distance is not None:
                distances.append((float(distance), other_id))
        distances.sort(key=lambda item: (item[0], item[1]))
        row["portfolio_12m"]["nearest_mix_peers"] = [
            {
                "entity_id": other_id,
                "legal_name": by_id[other_id].get("legal_name"),
                "distance": distance,
            }
            for distance, other_id in distances[:count]
        ]


def _raw_entity(
    candidate: dict[str, Any],
    conduct_entity: dict[str, Any],
    ses_entity: dict[str, Any],
    months: list[str],
    periods: list[int],
) -> dict[str, Any]:
    entity_id = str(candidate.get("entity_id") or "")
    conduct_months = _monthly_conduct(conduct_entity, months)
    premium_direct = 0.0
    earned_sum = 0.0
    earned_complete_months = 0
    complaints_total = 0
    monthly_raw: list[dict[str, Any]] = []

    for month, period in zip(months, periods, strict=True):
        ses_month = _ses_month(ses_entity, period)
        direct_missing = _nonnegative_int(
            ses_month.get("insurance_premium_direct_missing_rows") or 0,
            field="insurance_premium_direct_missing_rows",
        )
        earned_missing = _nonnegative_int(
            ses_month.get("insurance_premium_earned_missing_rows") or 0,
            field="insurance_premium_earned_missing_rows",
        )
        if direct_missing:
            raise ConductComparativeCalibrationV2Error(
                "direct one-to-one candidate has incomplete monthly direct premium: "
                f"{entity_id} {month}"
            )
        direct = _finite(
            ses_month.get("insurance_premium_direct") or 0.0,
            field="premium_direct",
        )
        earned_value = _finite(
            ses_month.get("insurance_premium_earned") or 0.0,
            field="premium_earned",
        )
        earned = earned_value if earned_missing == 0 else None
        complaints = _nonnegative_int(
            conduct_months[month].get("complaints") or 0,
            field="complaints",
        )
        premium_direct += direct
        complaints_total += complaints
        if earned is not None:
            earned_sum += earned
            earned_complete_months += 1
        monthly_raw.append(
            {
                "month": month,
                "period": period,
                "complaints": complaints,
                "premium_direct": direct,
                "premium_direct_missing_rows": direct_missing,
                "premium_earned_diagnostic": earned,
                "premium_earned_missing_rows": earned_missing,
                "premium_earned_diagnostic_complete": earned_missing == 0,
            }
        )

    if premium_direct <= 0:
        raise ConductComparativeCalibrationV2Error(
            f"direct one-to-one candidate has non-positive 12m premium: {entity_id}"
        )
    return {
        "entity_id": entity_id,
        "fip_code": str(candidate.get("fip_code") or "").strip(),
        "cnpj": candidate.get("cnpj"),
        "legal_name": candidate.get("legal_name"),
        "display_name": conduct_entity.get("display_name"),
        "complaints_12m": complaints_total,
        "premium_direct_12m": premium_direct,
        "premium_earned_12m_diagnostic": (
            earned_sum if earned_complete_months == len(months) else None
        ),
        "premium_earned_complete_months": earned_complete_months,
        "premium_earned_diagnostic_complete": earned_complete_months == len(months),
        "monthly_raw": monthly_raw,
        "branch_totals": _branch_totals(ses_entity, periods),
        "satisfaction": _satisfaction_diagnostics(conduct_entity),
    }


def _monthly_baselines(
    raw_rows: list[dict[str, Any]], months: list[str]
) -> tuple[dict[str, tuple[int, float]], list[dict[str, Any]]]:
    baselines: dict[str, tuple[int, float]] = {}
    output: list[dict[str, Any]] = []
    for month in months:
        observations = [
            next(item for item in row["monthly_raw"] if item["month"] == month)
            for row in raw_rows
        ]
        comparable = [item for item in observations if float(item["premium_direct"]) > 0]
        complaints = sum(int(item["complaints"]) for item in comparable)
        premium = sum(float(item["premium_direct"]) for item in comparable)
        baselines[month] = (complaints, premium)
        output.append(
            {
                "month": month,
                "comparable_entities": len(comparable),
                "excluded_non_positive_premium_entities": len(raw_rows) - len(comparable),
                "market_complaints": complaints,
                "market_premium_direct": float(premium),
                "pressure_baseline_state": (
                    "available"
                    if complaints > 0 and premium > 0
                    else "unavailable_zero_market_complaints"
                    if premium > 0
                    else "unavailable_non_positive_market_premium"
                ),
                "population_policy": "complaints_and_exposure_same_entities_only",
            }
        )
    return baselines, output


def _aligned_entity(
    raw: dict[str, Any], baselines: dict[str, tuple[int, float]], market_branches: dict[str, float]
) -> dict[str, Any]:
    monthly: list[dict[str, Any]] = []
    ratios: list[float | None] = []
    aligned_observed = 0
    aligned_expected = 0.0
    aligned_direct = 0.0

    for item in raw["monthly_raw"]:
        month = str(item["month"])
        direct = float(item["premium_direct"])
        complaints = int(item["complaints"])
        market_complaints, market_premium = baselines[month]
        if direct <= 0 or market_premium <= 0:
            monthly.append(
                {
                    **item,
                    "state": "not_comparable_non_positive_monthly_premium",
                    "reason_code": "non_positive_entity_or_market_premium",
                    "expected_complaints": None,
                    "pressure_ratio": None,
                }
            )
            ratios.append(None)
            continue
        zero_guard = zero_market_complaints_guard(market_complaints, market_premium)
        if zero_guard is not None:
            monthly.append({**item, **zero_guard})
            ratios.append(None)
            continue
        expected = expected_complaints(direct, market_complaints, market_premium)
        ratio = pressure_ratio(complaints, direct, market_complaints, market_premium)
        if expected is None or ratio is None:
            raise ConductComparativeCalibrationV2Error(
                f"monthly pressure unavailable: {raw['entity_id']} {month}"
            )
        aligned_observed += complaints
        aligned_expected += expected
        aligned_direct += direct
        monthly.append(
            {
                **item,
                "state": "available",
                "reason_code": None,
                "expected_complaints": expected,
                "pressure_ratio": ratio,
            }
        )
        ratios.append(ratio)

    if aligned_expected <= 0:
        raise ConductComparativeCalibrationV2Error(
            f"aligned annual pressure unavailable: {raw['entity_id']}"
        )
    pressure = {
        "observed_complaints": aligned_observed,
        "total_observed_complaints": raw["complaints_12m"],
        "expected_complaints": float(aligned_expected),
        "ratio": float(aligned_observed / aligned_expected),
        "comparable_months": sum(item.get("state") == "available" for item in monthly),
        "zero_market_complaint_months": sum(
            item.get("state") == ZERO_MARKET_COMPLAINTS_STATE for item in monthly
        ),
        "aligned_premium_direct": float(aligned_direct),
        "aggregation_policy": ALIGNED_POLICY,
        "zero_market_complaints_policy": ZERO_MARKET_COMPLAINTS_POLICY,
    }
    return {
        **{key: raw[key] for key in (
            "entity_id", "fip_code", "cnpj", "legal_name", "display_name",
            "complaints_12m", "premium_direct_12m", "premium_earned_12m_diagnostic",
            "premium_earned_complete_months", "premium_earned_diagnostic_complete",
        )},
        "pressure_12m": pressure,
        "monthly": monthly,
        "persistence": persistence_diagnostics(ratios),
        "satisfaction": raw["satisfaction"],
        "portfolio_12m": {
            "branch_premium_direct": raw["branch_totals"],
            **_portfolio_diagnostics(raw["branch_totals"], market_branches),
        },
        "small_sample_bucket": _small_sample_bucket(aligned_observed),
    }


def build_calibration_v2(
    conduct: dict[str, Any],
    reconciliation: dict[str, Any],
    ses: dict[str, Any],
) -> dict[str, Any]:
    months = [str(value) for value in (conduct.get("source") or {}).get("months") or []]
    periods = _validated_periods(months)
    ses_periods = {int(value) for value in ses.get("periods") or []}
    missing_periods = [period for period in periods if period not in ses_periods]
    if missing_periods:
        raise ConductComparativeCalibrationV2Error(
            f"SES misses Conduct comparison periods: {missing_periods}"
        )

    candidates, excluded = _candidate_rows(reconciliation)
    conduct_by_id = _conduct_index(conduct)
    ses_entities = ses.get("entities") or {}
    raw_rows: list[dict[str, Any]] = []
    for candidate in candidates:
        entity_id = str(candidate.get("entity_id") or "")
        fip = str(candidate.get("fip_code") or "").strip()
        conduct_entity = conduct_by_id.get(entity_id)
        if conduct_entity is None:
            raise ConductComparativeCalibrationV2Error(
                f"candidate lacks Conduct evidence: {entity_id}"
            )
        ses_entity = ses_entities.get(fip) or ses_entities.get(fip.zfill(6))
        if ses_entity is None:
            raise ConductComparativeCalibrationV2Error(
                f"candidate lacks SES exposure: {entity_id}"
            )
        raw_rows.append(_raw_entity(candidate, conduct_entity, ses_entity, months, periods))

    annual_market_complaints = sum(int(row["complaints_12m"]) for row in raw_rows)
    annual_market_premium = sum(float(row["premium_direct_12m"]) for row in raw_rows)
    if annual_market_premium <= 0:
        raise ConductComparativeCalibrationV2Error("aligned annual market premium is non-positive")

    baselines, monthly_market = _monthly_baselines(raw_rows, months)
    market_branches: Counter[str] = Counter()
    for row in raw_rows:
        market_branches.update(row["branch_totals"])
    market_branch_dict = {key: float(value) for key, value in sorted(market_branches.items())}

    rows = [_aligned_entity(row, baselines, market_branch_dict) for row in raw_rows]
    for row in rows:
        row["pressure_12m"]["annual_aggregate_complaint_share_diagnostic"] = (
            row["complaints_12m"] / annual_market_complaints
            if annual_market_complaints > 0
            else None
        )
        row["pressure_12m"]["annual_aggregate_premium_share_diagnostic"] = (
            row["premium_direct_12m"] / annual_market_premium
        )
    _nearest_mix_peers(rows)

    diagnostic_sorted = sorted(
        rows,
        key=lambda row: (float(row["pressure_12m"]["ratio"]), str(row["entity_id"])),
    )
    extreme_count = min(10, len(diagnostic_sorted))
    excluded_counts = Counter(str(row.get("state") or "unknown") for row in excluded)
    taxonomy_state = ((conduct.get("source") or {}).get("taxonomy_evidence") or {}).get("state")
    core_state = ((conduct.get("source") or {}).get("core") or {}).get("state")

    def extreme(row: dict[str, Any]) -> dict[str, Any]:
        return {
            "entity_id": row["entity_id"],
            "legal_name": row.get("legal_name"),
            "complaints_12m": row["complaints_12m"],
            "aligned_observed_complaints": row["pressure_12m"]["observed_complaints"],
            "expected_complaints": row["pressure_12m"]["expected_complaints"],
            "pressure_ratio": row["pressure_12m"]["ratio"],
            "premium_direct_12m": row["premium_direct_12m"],
        }

    return {
        "artifact": "v2_conduct_comparative_calibration_v2",
        "generated_at": _utc_now(),
        "version": VERSION,
        "status": "experimental_diagnostic",
        "assessment_role": "conduct_comparative_calibration_only",
        "scoring": "forbidden_in_this_artifact",
        "ranking": "forbidden_in_this_artifact",
        "denominator": {
            "candidate": "insurance_premium_direct",
            "source_field": "premio_direto",
            "currency": "BRL",
            "source_unit_label": "R$",
            "scale_factor_applied": 1.0,
            "selected_for_scoring": False,
            "final_denominator_approved": False,
            "diagnostic_companion": "insurance_premium_earned",
            "diagnostic_missingness_policy": "incomplete_premium_earned_is_unavailable_not_zero",
            "excluded_domains": ["private_pension", "capitalization"],
        },
        "source": {
            "tier": "P3",
            "consumer_gov_core_state": core_state,
            "taxonomy_state": taxonomy_state,
            "consumer_gov_artifact": str(CONDUCT_PATH),
            "reconciliation_artifact": str(RECONCILIATION_PATH),
            "ses_source": str(DEFAULT_SES_ZIP),
            "ses_component_file": "Ses_seguros.csv",
            "ses_currency": "BRL",
            "ses_source_unit_label": "R$",
            "ses_scale_factor_applied": 1.0,
            "ses_source_documentation_url": SES_TABLE_DOCUMENTATION_URL,
            "months": months,
            "periods": periods,
            "series_policy": "no_cross_source_stitching",
            "period_policy": "chronological_consecutive_months",
        },
        "population": {
            "reconciliation_entities": len(reconciliation.get("entities") or []),
            "direct_one_to_one_candidates": len(rows),
            "excluded_from_pressure_experiment": len(excluded),
            "excluded_by_state": dict(sorted(excluded_counts.items())),
            "population_policy": "complaints_and_exposure_same_entities_only",
        },
        "market_12m": {
            "complaints": int(annual_market_complaints),
            "premium_direct": float(annual_market_premium),
            "premium_currency": "BRL",
            "premium_source_unit_label": "R$",
            "role": "annual_aggregate_context_not_pressure_baseline",
            "branch_premium_direct": market_branch_dict,
            "branch_mix_positive_only": branch_mix(market_branch_dict),
        },
        "monthly_market": monthly_market,
        "diagnostics": {
            "pressure_ratio_quantiles": _quantiles(
                [float(row["pressure_12m"]["ratio"]) for row in rows]
            ),
            "complaint_count_quantiles": _quantiles(
                [float(row["complaints_12m"]) for row in rows]
            ),
            "aligned_complaint_count_quantiles": _quantiles(
                [float(row["pressure_12m"]["observed_complaints"]) for row in rows]
            ),
            "premium_direct_quantiles": _quantiles(
                [float(row["premium_direct_12m"]) for row in rows]
            ),
            "small_sample": _small_sample_summary(rows),
            "shrinkage_applied": False,
            "shrinkage_decision": "pending_distribution_review",
            "peer_groups_selected": False,
            "portfolio_mix_diagnostic_available": True,
            "monthly_population_can_vary": True,
            "pressure_aggregation_policy": ALIGNED_POLICY,
            "zero_market_complaints_policy": "not_comparable_not_neutral",
            "extremes_are_diagnostic_not_ranking": True,
            "highest_pressure_observations": [
                extreme(row) for row in reversed(diagnostic_sorted[-extreme_count:])
            ],
            "lowest_pressure_observations": [
                extreme(row) for row in diagnostic_sorted[:extreme_count]
            ],
        },
        "excluded_entities": sorted(
            excluded,
            key=lambda row: (
                str(row.get("state") or ""),
                str(row.get("legal_name") or ""),
                str(row.get("entity_id") or ""),
            ),
        ),
        "entities": sorted(rows, key=lambda row: str(row.get("entity_id") or "")),
    }


def build_from_files(
    *,
    conduct_path: Path = CONDUCT_PATH,
    reconciliation_path: Path = RECONCILIATION_PATH,
    ses_path: Path = DEFAULT_SES_ZIP,
) -> dict[str, Any]:
    conduct = json.loads(conduct_path.read_text(encoding="utf-8"))
    reconciliation = json.loads(reconciliation_path.read_text(encoding="utf-8"))
    candidates, _ = _candidate_rows(reconciliation)
    fips = [
        str(row.get("fip_code") or "")
        for row in candidates
        if str(row.get("fip_code") or "")
    ]
    ses = load_susep_insurance_exposure(fips, ses_path)
    return build_calibration_v2(conduct, reconciliation, ses)


def main() -> None:
    payload = build_from_files()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "artifact": payload["artifact"],
                "version": payload["version"],
                "population": payload["population"],
                "market_12m": {
                    "complaints": payload["market_12m"]["complaints"],
                    "premium_direct": payload["market_12m"]["premium_direct"],
                },
                "pressure_ratio_quantiles": payload["diagnostics"]["pressure_ratio_quantiles"],
                "small_sample": payload["diagnostics"]["small_sample"],
                "highest_pressure_observations": payload["diagnostics"]["highest_pressure_observations"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    print(f"written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
