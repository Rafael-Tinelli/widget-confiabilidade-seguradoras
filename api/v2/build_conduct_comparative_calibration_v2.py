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
    load_susep_insurance_exposure,
)
from api.v2.conduct_comparative import (
    branch_mix,
    branch_mix_distance,
    expected_complaints,
    persistence_diagnostics,
    pressure_ratio,
)

CONDUCT_PATH = Path("data/derived/v2/consumer_gov_conduct_evidence.json")
RECONCILIATION_PATH = Path("data/derived/v2/conduct_coverage_reconciliation.json")
OUTPUT_PATH = Path("data/derived/v2/conduct_comparative_calibration_v2.json")
VERSION = "2.0-draft-conduct-comparative-calibration-3"
CANDIDATE_STATE = "direct_one_to_one_candidate"


class ConductComparativeCalibrationV2Error(RuntimeError):
    """Raised when comparative Conduct inputs cannot be aligned safely."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _period(month: str) -> int:
    text = str(month or "").strip().replace("-", "")
    if len(text) != 6 or not text.isdigit():
        raise ConductComparativeCalibrationV2Error(f"invalid month: {month!r}")
    year = int(text[:4])
    month_number = int(text[4:])
    if year < 2000 or not 1 <= month_number <= 12:
        raise ConductComparativeCalibrationV2Error(f"invalid month: {month!r}")
    return int(text)


def _next_period(period: int) -> int:
    year = period // 100
    month = period % 100
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
    for index in range(1, len(periods)):
        previous = periods[index - 1]
        current = periods[index]
        if current != _next_period(previous):
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
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise ConductComparativeCalibrationV2Error(
            f"non-integer {field}: {value!r}"
        ) from exc
    if number < 0:
        raise ConductComparativeCalibrationV2Error(f"negative {field}: {number}")
    return number


def _quantiles(values: list[float]) -> dict[str, float | None]:
    finite = [float(value) for value in values if math.isfinite(float(value))]
    if not finite:
        return {
            "min": None,
            "p10": None,
            "p25": None,
            "p50": None,
            "p75": None,
            "p90": None,
            "max": None,
        }
    return {
        "min": float(min(finite)),
        "p10": float(np.percentile(finite, 10)),
        "p25": float(np.percentile(finite, 25)),
        "p50": float(np.percentile(finite, 50)),
        "p75": float(np.percentile(finite, 75)),
        "p90": float(np.percentile(finite, 90)),
        "max": float(max(finite)),
    }


def _candidate_rows(
    reconciliation: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    candidates: list[dict[str, Any]] = []
    excluded: list[dict[str, Any]] = []
    for row in reconciliation.get("entities") or []:
        pressure = row.get("pressure_comparability") or {}
        state = str(pressure.get("state") or "")
        if state == CANDIDATE_STATE and bool(
            pressure.get("pressure_eligible_candidate")
        ):
            exposure = row.get("insurance_exposure_12m") or {}
            if int(exposure.get("insurance_premium_direct_missing_rows") or 0) > 0:
                raise ConductComparativeCalibrationV2Error(
                    "direct one-to-one candidate has incomplete direct-premium exposure: "
                    f"{row.get('entity_id')}"
                )
            candidates.append(row)
        else:
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
        str(row.get("entity_id") or ""): row
        for row in conduct.get("entities") or []
        if str(row.get("entity_id") or "")
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
    if missing:
        raise ConductComparativeCalibrationV2Error(
            f"Conduct entity {entity.get('entity_id')} misses months: {missing}"
        )
    extra = sorted(set(by_month) - set(months))
    if extra:
        raise ConductComparativeCalibrationV2Error(
            f"Conduct entity {entity.get('entity_id')} has unexpected months: {extra}"
        )
    return by_month


def _ses_month(entity: dict[str, Any], period: int) -> dict[str, Any]:
    months = entity.get("months") or {}
    month = months.get(period)
    if month is None:
        month = months.get(str(period))
    return month or {}


def _branch_totals(
    ses_entity: dict[str, Any], periods: list[int]
) -> dict[str, float]:
    totals: Counter[str] = Counter()
    for period in periods:
        month = _ses_month(ses_entity, period)
        for branch, values in (month.get("insurance_branches") or {}).items():
            amount = _finite(
                (values or {}).get("premium_direct") or 0.0,
                field="premium_direct",
            )
            totals[str(branch)] += amount
    return {key: float(value) for key, value in sorted(totals.items())}


def _portfolio_diagnostics(
    branches: dict[str, float], market_branches: dict[str, float]
) -> dict[str, Any]:
    mix = branch_mix(branches)
    concentration = sum(value * value for value in mix.values()) if mix else None
    top_share = max(mix.values()) if mix else None
    return {
        "positive_branch_mix": mix,
        "positive_branch_count": len(mix),
        "hhi": float(concentration) if concentration is not None else None,
        "top_branch_share": float(top_share) if top_share is not None else None,
        "distance_from_market_mix": branch_mix_distance(branches, market_branches),
        "negative_branch_total_count": sum(
            1 for value in branches.values() if value < 0
        ),
        "zero_branch_total_count": sum(1 for value in branches.values() if value == 0),
    }


def _satisfaction_diagnostics(conduct_entity: dict[str, Any]) -> dict[str, Any]:
    totals = conduct_entity.get("totals") or {}
    film = conduct_entity.get("film") or {}
    return {
        "sample_count": int(totals.get("satisfaction_count") or 0),
        "average": totals.get("average_satisfaction"),
        "trend": film.get("satisfaction_trend"),
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
    buckets: dict[str, list[dict[str, Any]]] = {
        key: [] for key in ("0_4", "5_19", "20_99", "100_plus")
    }
    for row in rows:
        buckets[_small_sample_bucket(int(row["complaints_12m"]))].append(row)
    return {
        key: {
            "entities": len(items),
            "complaints_total": sum(
                int(item["complaints_12m"]) for item in items
            ),
            "expected_complaints_quantiles": _quantiles(
                [
                    float(item["pressure_12m"]["expected_complaints"])
                    for item in items
                ]
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
                left,
                other["portfolio_12m"]["branch_premium_direct"],
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


def build_calibration_v2(
    conduct: dict[str, Any],
    reconciliation: dict[str, Any],
    ses: dict[str, Any],
) -> dict[str, Any]:
    months = [
        str(value) for value in (conduct.get("source") or {}).get("months") or []
    ]
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

    rows: list[dict[str, Any]] = []
    market_branches: Counter[str] = Counter()
    annual_market_complaints = 0
    annual_market_premium = 0.0

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

        conduct_months = _monthly_conduct(conduct_entity, months)
        branch_totals = _branch_totals(ses_entity, periods)
        premium_direct = 0.0
        earned_complete_sum = 0.0
        earned_complete_months = 0
        monthly_raw: list[dict[str, Any]] = []
        complaints_total = 0

        for month, period in zip(months, periods, strict=True):
            ses_month = _ses_month(ses_entity, period)
            direct_missing_rows = _nonnegative_int(
                ses_month.get("insurance_premium_direct_missing_rows") or 0,
                field="insurance_premium_direct_missing_rows",
            )
            earned_missing_rows = _nonnegative_int(
                ses_month.get("insurance_premium_earned_missing_rows") or 0,
                field="insurance_premium_earned_missing_rows",
            )
            if direct_missing_rows > 0:
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
            earned = earned_value if earned_missing_rows == 0 else None
            complaints = _nonnegative_int(
                conduct_months[month].get("complaints") or 0,
                field="complaints",
            )
            premium_direct += direct
            if earned is not None:
                earned_complete_sum += earned
                earned_complete_months += 1
            complaints_total += complaints
            monthly_raw.append(
                {
                    "month": month,
                    "period": period,
                    "complaints": complaints,
                    "premium_direct": direct,
                    "premium_direct_missing_rows": direct_missing_rows,
                    "premium_earned_diagnostic": earned,
                    "premium_earned_missing_rows": earned_missing_rows,
                    "premium_earned_diagnostic_complete": earned_missing_rows == 0,
                }
            )

        if premium_direct <= 0:
            raise ConductComparativeCalibrationV2Error(
                f"direct one-to-one candidate has non-positive 12m premium: {entity_id}"
            )
        annual_market_complaints += complaints_total
        annual_market_premium += premium_direct
        for branch, amount in branch_totals.items():
            market_branches[branch] += amount

        raw_rows.append(
            {
                "entity_id": entity_id,
                "fip_code": fip,
                "cnpj": candidate.get("cnpj"),
                "legal_name": candidate.get("legal_name"),
                "display_name": conduct_entity.get("display_name"),
                "complaints_12m": complaints_total,
                "premium_direct_12m": premium_direct,
                "premium_earned_12m_diagnostic": (
                    earned_complete_sum
                    if earned_complete_months == len(months)
                    else None
                ),
                "premium_earned_complete_months": earned_complete_months,
                "premium_earned_diagnostic_complete": (
                    earned_complete_months == len(months)
                ),
                "monthly_raw": monthly_raw,
                "branch_totals": branch_totals,
                "satisfaction": _satisfaction_diagnostics(conduct_entity),
            }
        )

    if annual_market_premium <= 0:
        raise ConductComparativeCalibrationV2Error(
            "aligned annual market premium is non-positive"
        )

    monthly_market: list[dict[str, Any]] = []
    monthly_baselines: dict[str, tuple[int, float]] = {}
    for month in months:
        comparable = [
            row
            for row in raw_rows
            if next(
                item for item in row["monthly_raw"] if item["month"] == month
            )["premium_direct"]
            > 0
        ]
        market_complaints = sum(
            next(item for item in row["monthly_raw"] if item["month"] == month)[
                "complaints"
            ]
            for row in comparable
        )
        market_premium = sum(
            next(item for item in row["monthly_raw"] if item["month"] == month)[
                "premium_direct"
            ]
            for row in comparable
        )
        monthly_baselines[month] = (market_complaints, market_premium)
        monthly_market.append(
            {
                "month": month,
                "comparable_entities": len(comparable),
                "excluded_non_positive_premium_entities": len(raw_rows)
                - len(comparable),
                "market_complaints": market_complaints,
                "market_premium_direct": float(market_premium),
                "population_policy": "complaints_and_exposure_same_entities_only",
            }
        )

    market_branch_dict = {
        key: float(value) for key, value in sorted(market_branches.items())
    }
    for raw in raw_rows:
        monthly: list[dict[str, Any]] = []
        monthly_ratios: list[float | None] = []
        aligned_observed = 0
        aligned_expected = 0.0
        aligned_direct = 0.0
        for item in raw["monthly_raw"]:
            month = str(item["month"])
            direct = float(item["premium_direct"])
            complaints = int(item["complaints"])
            market_complaints, market_premium = monthly_baselines[month]
            if direct <= 0 or market_premium <= 0:
                monthly.append(
                    {
                        **item,
                        "state": "not_comparable_non_positive_monthly_premium",
                        "expected_complaints": None,
                        "pressure_ratio": None,
                    }
                )
                monthly_ratios.append(None)
                continue
            monthly_expected = expected_complaints(
                direct, market_complaints, market_premium
            )
            monthly_ratio = pressure_ratio(
                complaints,
                direct,
                market_complaints,
                market_premium,
            )
            if monthly_expected is None or monthly_ratio is None:
                raise ConductComparativeCalibrationV2Error(
                    f"monthly pressure unavailable: {raw['entity_id']} {month}"
                )
            aligned_observed += complaints
            aligned_expected += monthly_expected
            aligned_direct += direct
            monthly.append(
                {
                    **item,
                    "state": "available",
                    "expected_complaints": monthly_expected,
                    "pressure_ratio": monthly_ratio,
                }
            )
            monthly_ratios.append(monthly_ratio)

        if aligned_expected <= 0:
            raise ConductComparativeCalibrationV2Error(
                f"aligned annual pressure unavailable: {raw['entity_id']}"
            )
        ratio = aligned_observed / aligned_expected

        portfolio = {
            "branch_premium_direct": raw["branch_totals"],
            **_portfolio_diagnostics(raw["branch_totals"], market_branch_dict),
        }
        rows.append(
            {
                "entity_id": raw["entity_id"],
                "fip_code": raw["fip_code"],
                "cnpj": raw["cnpj"],
                "legal_name": raw["legal_name"],
                "display_name": raw["display_name"],
                "complaints_12m": raw["complaints_12m"],
                "premium_direct_12m": raw["premium_direct_12m"],
                "premium_earned_12m_diagnostic": raw[
                    "premium_earned_12m_diagnostic"
                ],
                "premium_earned_complete_months": raw[
                    "premium_earned_complete_months"
                ],
                "premium_earned_diagnostic_complete": raw[
                    "premium_earned_diagnostic_complete"
                ],
                "pressure_12m": {
                    "observed_complaints": aligned_observed,
                    "total_observed_complaints": raw["complaints_12m"],
                    "expected_complaints": float(aligned_expected),
                    "ratio": float(ratio),
                    "comparable_months": sum(
                        item.get("state") == "available" for item in monthly
                    ),
                    "aligned_premium_direct": float(aligned_direct),
                    "aggregation_policy": (
                        "sum_monthly_expected_then_observed_divided_by_expected"
                    ),
                    "annual_aggregate_complaint_share_diagnostic": (
                        raw["complaints_12m"] / annual_market_complaints
                        if annual_market_complaints > 0
                        else None
                    ),
                    "annual_aggregate_premium_share_diagnostic": (
                        raw["premium_direct_12m"] / annual_market_premium
                    ),
                },
                "monthly": monthly,
                "persistence": persistence_diagnostics(monthly_ratios),
                "satisfaction": raw["satisfaction"],
                "portfolio_12m": portfolio,
                "small_sample_bucket": _small_sample_bucket(
                    raw["complaints_12m"]
                ),
            }
        )

    _nearest_mix_peers(rows)
    pressure_values = [float(row["pressure_12m"]["ratio"]) for row in rows]
    complaint_values = [float(row["complaints_12m"]) for row in rows]
    premium_values = [float(row["premium_direct_12m"]) for row in rows]

    diagnostic_sorted = sorted(
        rows,
        key=lambda row: (
            float(row["pressure_12m"]["ratio"]),
            str(row["entity_id"]),
        ),
    )
    extreme_count = min(10, len(diagnostic_sorted))

    excluded_counts = Counter(
        str(row.get("state") or "unknown") for row in excluded
    )
    taxonomy_state = (
        ((conduct.get("source") or {}).get("taxonomy_evidence") or {}).get(
            "state"
        )
    )
    core_state = ((conduct.get("source") or {}).get("core") or {}).get("state")

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
            "selected_for_scoring": False,
            "final_denominator_approved": False,
            "diagnostic_companion": "insurance_premium_earned",
            "diagnostic_missingness_policy": (
                "incomplete_premium_earned_is_unavailable_not_zero"
            ),
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
            "role": "annual_aggregate_context_not_pressure_baseline",
            "branch_premium_direct": market_branch_dict,
            "branch_mix_positive_only": branch_mix(market_branch_dict),
        },
        "monthly_market": monthly_market,
        "diagnostics": {
            "pressure_ratio_quantiles": _quantiles(pressure_values),
            "complaint_count_quantiles": _quantiles(complaint_values),
            "premium_direct_quantiles": _quantiles(premium_values),
            "small_sample": _small_sample_summary(rows),
            "shrinkage_applied": False,
            "shrinkage_decision": "pending_distribution_review",
            "peer_groups_selected": False,
            "portfolio_mix_diagnostic_available": True,
            "monthly_population_can_vary": True,
            "pressure_aggregation_policy": (
                "sum_monthly_expected_then_observed_divided_by_expected"
            ),
            "extremes_are_diagnostic_not_ranking": True,
            "highest_pressure_observations": [
                {
                    "entity_id": row["entity_id"],
                    "legal_name": row.get("legal_name"),
                    "complaints_12m": row["complaints_12m"],
                    "expected_complaints": row["pressure_12m"][
                        "expected_complaints"
                    ],
                    "pressure_ratio": row["pressure_12m"]["ratio"],
                    "premium_direct_12m": row["premium_direct_12m"],
                }
                for row in reversed(diagnostic_sorted[-extreme_count:])
            ],
            "lowest_pressure_observations": [
                {
                    "entity_id": row["entity_id"],
                    "legal_name": row.get("legal_name"),
                    "complaints_12m": row["complaints_12m"],
                    "expected_complaints": row["pressure_12m"][
                        "expected_complaints"
                    ],
                    "pressure_ratio": row["pressure_12m"]["ratio"],
                    "premium_direct_12m": row["premium_direct_12m"],
                }
                for row in diagnostic_sorted[:extreme_count]
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
        "entities": sorted(
            rows,
            key=lambda row: str(row.get("entity_id") or ""),
        ),
    }


def build_from_files(
    *,
    conduct_path: Path = CONDUCT_PATH,
    reconciliation_path: Path = RECONCILIATION_PATH,
    ses_path: Path = DEFAULT_SES_ZIP,
) -> dict[str, Any]:
    conduct = json.loads(conduct_path.read_text(encoding="utf-8"))
    reconciliation = json.loads(
        reconciliation_path.read_text(encoding="utf-8")
    )
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
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
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
                "pressure_ratio_quantiles": payload["diagnostics"][
                    "pressure_ratio_quantiles"
                ],
                "small_sample": payload["diagnostics"]["small_sample"],
                "highest_pressure_observations": payload["diagnostics"][
                    "highest_pressure_observations"
                ],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    print(f"written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
