from __future__ import annotations

import json
import math
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
from scipy.stats import chi2

CALIBRATION_PATH = Path("data/derived/v2/conduct_comparative_calibration_v2.json")
OUTPUT_PATH = Path("data/derived/v2/conduct_credibility_diagnostic.json")
VERSION = "2.0-draft-conduct-credibility-2"
INDIVIDUAL_ALPHA = 0.05
FAMILYWISE_ALPHA = 0.05
MIN_TEMPORAL_MONTHS = 9
ALIGNED_AGGREGATION_POLICY = "sum_monthly_expected_then_observed_divided_by_expected"


class ConductCredibilityDiagnosticError(RuntimeError):
    """Raised when the Conduct credibility diagnostic cannot be built safely."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _finite(value: Any, *, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ConductCredibilityDiagnosticError(
            f"non-numeric {field}: {value!r}"
        ) from exc
    if not math.isfinite(number):
        raise ConductCredibilityDiagnosticError(f"non-finite {field}: {value!r}")
    return number


def _optional_finite(value: Any, *, field: str) -> float | None:
    if value is None:
        return None
    return _finite(value, field=field)


def _nonnegative_int(value: Any, *, field: str) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise ConductCredibilityDiagnosticError(
            f"non-integer {field}: {value!r}"
        ) from exc
    if number < 0:
        raise ConductCredibilityDiagnosticError(f"negative {field}: {number}")
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


def _poisson_ratio_interval(
    observed: int,
    expected: float,
    alpha: float,
) -> dict[str, Any]:
    """Exact Poisson interval for an observed/expected standardized ratio."""
    if observed < 0 or expected <= 0 or not 0 < alpha < 1:
        raise ConductCredibilityDiagnosticError("invalid Poisson ratio interval inputs")
    lower = 0.0 if observed == 0 else float(
        0.5 * chi2.ppf(alpha / 2.0, 2 * observed) / expected
    )
    upper = float(
        0.5 * chi2.ppf(1.0 - alpha / 2.0, 2 * (observed + 1)) / expected
    )
    if lower > 1.0:
        state = "above_size_proportional_reference"
    elif upper < 1.0:
        state = "below_size_proportional_reference"
    else:
        state = "not_distinguishable_from_size_proportional_reference"
    return {
        "method": "exact_poisson_standardized_ratio",
        "alpha": float(alpha),
        "confidence_level": float(1.0 - alpha),
        "lower": lower,
        "upper": upper,
        "reference": 1.0,
        "state": state,
    }


def _month_index(entity: dict[str, Any], months: list[str]) -> dict[str, dict[str, Any]]:
    rows = list(entity.get("monthly") or [])
    by_month = {
        str(row.get("month") or ""): row
        for row in rows
        if str(row.get("month") or "")
    }
    if len(by_month) != len(rows):
        raise ConductCredibilityDiagnosticError(
            f"entity {entity.get('entity_id')} has duplicate/invalid monthly rows"
        )
    if set(by_month) != set(months):
        raise ConductCredibilityDiagnosticError(
            f"entity {entity.get('entity_id')} does not match comparison months"
        )
    return by_month


def _temporal_overlap(entity: dict[str, Any], premium_key: str) -> dict[str, Any]:
    monthly = entity.get("monthly") or []
    complaints_total = _nonnegative_int(
        entity.get("complaints_12m") or 0,
        field="complaints_12m",
    )
    positive_months = 0
    non_positive_months = 0
    missing_months = 0
    complaints_positive = 0
    complaints_non_positive = 0
    complaints_missing = 0

    for row in monthly:
        complaints = _nonnegative_int(row.get("complaints") or 0, field="complaints")
        premium = _optional_finite(row.get(premium_key), field=premium_key)
        if premium is None:
            missing_months += 1
            complaints_missing += complaints
        elif premium > 0:
            positive_months += 1
            complaints_positive += complaints
        else:
            non_positive_months += 1
            complaints_non_positive += complaints

    if complaints_total == 0:
        state = "no_observed_complaints"
    elif complaints_missing > 0:
        state = "observed_complaints_overlap_missing_premium_months"
    elif complaints_non_positive == 0:
        state = "all_observed_complaints_in_positive_premium_months"
    elif complaints_non_positive == complaints_total:
        state = "all_observed_complaints_outside_positive_premium_months"
    else:
        state = "observed_complaints_span_positive_and_non_positive_premium_months"

    return {
        "state": state,
        "positive_premium_months": positive_months,
        "non_positive_premium_months": non_positive_months,
        "missing_premium_months": missing_months,
        "complaints_in_positive_premium_months": complaints_positive,
        "complaints_in_non_positive_premium_months": complaints_non_positive,
        "complaints_in_missing_premium_months": complaints_missing,
        "complaint_share_in_non_positive_premium_months": (
            complaints_non_positive / complaints_total if complaints_total > 0 else None
        ),
        "complaint_share_in_missing_premium_months": (
            complaints_missing / complaints_total if complaints_total > 0 else None
        ),
        "interpretation_guard": (
            "missing_or_non_positive_monthly_premium_is_not_equivalent_to_no_active_policy_or_no_consumer_exposure"
        ),
    }


def _aligned_direct(entity: dict[str, Any]) -> tuple[int, float, float, int]:
    pressure = entity.get("pressure_12m") or {}
    if pressure.get("aggregation_policy") != ALIGNED_AGGREGATION_POLICY:
        raise ConductCredibilityDiagnosticError(
            f"upstream direct pressure is not monthly aligned: {entity.get('entity_id')}"
        )
    observed = _nonnegative_int(
        pressure.get("observed_complaints"),
        field="pressure_12m.observed_complaints",
    )
    expected = _finite(
        pressure.get("expected_complaints"),
        field="pressure_12m.expected_complaints",
    )
    ratio = _finite(pressure.get("ratio"), field="pressure_12m.ratio")
    comparable_months = _nonnegative_int(
        pressure.get("comparable_months"),
        field="pressure_12m.comparable_months",
    )
    if expected <= 0 or comparable_months <= 0:
        raise ConductCredibilityDiagnosticError(
            f"upstream direct pressure is unavailable: {entity.get('entity_id')}"
        )
    expected_ratio = observed / expected
    if not math.isclose(ratio, expected_ratio, rel_tol=1e-10, abs_tol=1e-10):
        raise ConductCredibilityDiagnosticError(
            f"upstream direct pressure ratio mismatch: {entity.get('entity_id')}"
        )
    return observed, expected, ratio, comparable_months


def _earned_baselines(
    entities: list[dict[str, Any]],
    months: list[str],
) -> dict[str, dict[str, float | int]]:
    indexes = {
        str(entity.get("entity_id") or ""): _month_index(entity, months)
        for entity in entities
    }
    result: dict[str, dict[str, float | int]] = {}
    for month in months:
        market_complaints = 0
        market_premium = 0.0
        comparable_entities = 0
        for entity_id, by_month in indexes.items():
            row = by_month[month]
            premium = _optional_finite(
                row.get("premium_earned_diagnostic"),
                field=f"{entity_id}.premium_earned_diagnostic",
            )
            if premium is None or premium <= 0:
                continue
            comparable_entities += 1
            market_complaints += _nonnegative_int(
                row.get("complaints") or 0,
                field=f"{entity_id}.complaints",
            )
            market_premium += premium
        result[month] = {
            "market_complaints": market_complaints,
            "market_premium": market_premium,
            "comparable_entities": comparable_entities,
        }
    return result


def _earned_series(
    entity: dict[str, Any],
    months: list[str],
    baselines: dict[str, dict[str, float | int]],
) -> dict[str, Any]:
    by_month = _month_index(entity, months)
    aligned_observed = 0
    aligned_expected = 0.0
    comparable_months = 0
    missing_months = 0
    non_positive_months = 0

    for month in months:
        row = by_month[month]
        complaints = _nonnegative_int(row.get("complaints") or 0, field="complaints")
        premium = _optional_finite(
            row.get("premium_earned_diagnostic"),
            field="premium_earned_diagnostic",
        )
        if premium is None:
            missing_months += 1
            continue
        if premium <= 0:
            non_positive_months += 1
            continue
        baseline = baselines[month]
        market_premium = float(baseline["market_premium"])
        market_complaints = int(baseline["market_complaints"])
        if market_premium <= 0 or market_complaints <= 0:
            continue
        expected = market_complaints * premium / market_premium
        if expected <= 0:
            continue
        comparable_months += 1
        aligned_observed += complaints
        aligned_expected += expected

    if aligned_expected <= 0:
        return {
            "state": "unavailable_no_positive_aligned_premium_earned",
            "observed_complaints": aligned_observed,
            "expected_complaints": None,
            "ratio": None,
            "comparable_months": comparable_months,
            "missing_months": missing_months,
            "non_positive_months": non_positive_months,
        }
    return {
        "state": "available_diagnostic_only",
        "observed_complaints": aligned_observed,
        "expected_complaints": float(aligned_expected),
        "ratio": float(aligned_observed / aligned_expected),
        "comparable_months": comparable_months,
        "missing_months": missing_months,
        "non_positive_months": non_positive_months,
    }


def _neutral_side(ratio: float) -> str:
    if ratio > 1.0:
        return "above"
    if ratio < 1.0:
        return "below"
    return "equal"


def build_credibility_diagnostic(calibration: dict[str, Any]) -> dict[str, Any]:
    if calibration.get("scoring") != "forbidden_in_this_artifact":
        raise ConductCredibilityDiagnosticError(
            "upstream calibration must explicitly forbid scoring"
        )
    if calibration.get("ranking") != "forbidden_in_this_artifact":
        raise ConductCredibilityDiagnosticError(
            "upstream calibration must explicitly forbid ranking"
        )

    entities = list(calibration.get("entities") or [])
    if not entities:
        raise ConductCredibilityDiagnosticError("calibration contains no entities")
    months = [
        str(value)
        for value in (calibration.get("source") or {}).get("months") or []
    ]
    if not months or len(months) != len(set(months)):
        raise ConductCredibilityDiagnosticError("calibration comparison months are invalid")
    for entity in entities:
        _month_index(entity, months)

    direct_comparisons = len(entities)
    direct_familywise_alpha = FAMILYWISE_ALPHA / direct_comparisons
    earned_baselines = _earned_baselines(entities, months)

    prepared: list[dict[str, Any]] = []
    for entity in entities:
        observed, expected_direct, ratio_direct, direct_months = _aligned_direct(entity)
        earned_series = _earned_series(entity, months, earned_baselines)
        prepared.append(
            {
                "entity": entity,
                "observed": observed,
                "expected_direct": expected_direct,
                "ratio_direct": ratio_direct,
                "direct_months": direct_months,
                "earned_series": earned_series,
            }
        )

    earned_eligible = sum(
        int(item["earned_series"]["comparable_months"]) >= MIN_TEMPORAL_MONTHS
        and item["earned_series"]["ratio"] is not None
        for item in prepared
    )

    rows: list[dict[str, Any]] = []
    for item in prepared:
        entity = item["entity"]
        entity_id = str(entity.get("entity_id") or "")
        observed = int(item["observed"])
        expected_direct = float(item["expected_direct"])
        ratio_direct = float(item["ratio_direct"])
        direct_individual = _poisson_ratio_interval(
            observed,
            expected_direct,
            INDIVIDUAL_ALPHA,
        )
        direct_familywise = _poisson_ratio_interval(
            observed,
            expected_direct,
            direct_familywise_alpha,
        )

        earned_series = item["earned_series"]
        earned_ratio = earned_series.get("ratio")
        earned_expected = earned_series.get("expected_complaints")
        earned_coverage = int(earned_series.get("comparable_months") or 0)
        earned_diagnostic: dict[str, Any] = {
            **earned_series,
            "premium_earned_12m": entity.get("premium_earned_12m_diagnostic"),
            "selected_as_denominator": False,
            "eligible_for_sensitivity_guard": (
                earned_coverage >= MIN_TEMPORAL_MONTHS and earned_ratio is not None
            ),
        }
        if earned_ratio is not None and earned_expected is not None:
            earned_observed = int(earned_series["observed_complaints"])
            earned_diagnostic["individual_exact_interval"] = _poisson_ratio_interval(
                earned_observed,
                float(earned_expected),
                INDIVIDUAL_ALPHA,
            )
            earned_diagnostic["familywise_exact_interval"] = _poisson_ratio_interval(
                earned_observed,
                float(earned_expected),
                direct_familywise_alpha,
            )
        else:
            earned_diagnostic["individual_exact_interval"] = None
            earned_diagnostic["familywise_exact_interval"] = None

        direct_premium = _finite(
            entity.get("premium_direct_12m") or 0.0,
            field="premium_direct_12m",
        )
        earned_annual = _optional_finite(
            entity.get("premium_earned_12m_diagnostic"),
            field="premium_earned_12m_diagnostic",
        )
        annual_premium_ratio = (
            earned_annual / direct_premium
            if earned_annual is not None and direct_premium > 0
            else None
        )

        if earned_diagnostic["eligible_for_sensitivity_guard"]:
            ratio_earned = float(earned_ratio)
            ratio_multiplier = (
                ratio_earned / ratio_direct if ratio_direct > 0 else None
            )
            earned_familywise_state = str(
                earned_diagnostic["familywise_exact_interval"]["state"]
            )
            sensitivity = {
                "annual_premium_earned_to_direct_ratio": annual_premium_ratio,
                "pressure_ratio_earned_to_direct_multiplier": ratio_multiplier,
                "raw_neutral_side_consistency": (
                    "same_side"
                    if _neutral_side(ratio_direct) == _neutral_side(ratio_earned)
                    else "crosses_neutral"
                ),
                "familywise_state_consistency": (
                    "same_state"
                    if direct_familywise["state"] == earned_familywise_state
                    else "changes_state"
                ),
                "earned_temporal_coverage_sufficient": True,
            }
        else:
            sensitivity = {
                "annual_premium_earned_to_direct_ratio": annual_premium_ratio,
                "pressure_ratio_earned_to_direct_multiplier": None,
                "raw_neutral_side_consistency": "earned_insufficient_temporal_coverage",
                "familywise_state_consistency": "earned_insufficient_temporal_coverage",
                "earned_temporal_coverage_sufficient": False,
            }

        rows.append(
            {
                "entity_id": entity_id,
                "fip_code": entity.get("fip_code"),
                "cnpj": entity.get("cnpj"),
                "legal_name": entity.get("legal_name"),
                "display_name": entity.get("display_name"),
                "complaints_12m": _nonnegative_int(
                    entity.get("complaints_12m") or 0,
                    field="complaints_12m",
                ),
                "direct_candidate": {
                    "premium_direct_12m": direct_premium,
                    "premium_share": None,
                    "premium_share_state": "not_used_under_monthly_aligned_pressure",
                    "observed_complaints": observed,
                    "expected_complaints": expected_direct,
                    "ratio": ratio_direct,
                    "comparable_months": int(item["direct_months"]),
                    "aggregation_policy": ALIGNED_AGGREGATION_POLICY,
                    "individual_exact_interval": direct_individual,
                    "familywise_exact_interval": direct_familywise,
                    "selected_as_final_denominator": False,
                },
                "earned_diagnostic": earned_diagnostic,
                "temporal_overlap": {
                    "premium_direct": _temporal_overlap(entity, "premium_direct"),
                    "premium_earned": _temporal_overlap(
                        entity,
                        "premium_earned_diagnostic",
                    ),
                },
                "denominator_sensitivity": sensitivity,
                "human_questions": {
                    "pressure": "Ha reclamacoes demais para o tamanho da operacao?",
                    "credibility": "Temos evidencia suficiente para confiar nessa diferenca?",
                    "denominator": "A conclusao muda muito conforme a medida de tamanho usada?",
                    "time_alignment": "Reclamacoes e medida de exposicao contam uma historia temporal coerente?",
                },
            }
        )

    direct_individual_states = Counter(
        row["direct_candidate"]["individual_exact_interval"]["state"]
        for row in rows
    )
    direct_familywise_states = Counter(
        row["direct_candidate"]["familywise_exact_interval"]["state"]
        for row in rows
    )
    earned_familywise_states = Counter(
        row["earned_diagnostic"]["familywise_exact_interval"]["state"]
        for row in rows
        if row["earned_diagnostic"].get("familywise_exact_interval") is not None
        and row["earned_diagnostic"].get("eligible_for_sensitivity_guard")
    )

    complaints_outside_direct = sum(
        int(
            row["temporal_overlap"]["premium_direct"][
                "complaints_in_non_positive_premium_months"
            ]
        )
        for row in rows
    )
    entities_outside_direct = sum(
        int(
            row["temporal_overlap"]["premium_direct"][
                "complaints_in_non_positive_premium_months"
            ]
        )
        > 0
        for row in rows
    )
    raw_side_changes = sum(
        row["denominator_sensitivity"]["raw_neutral_side_consistency"]
        == "crosses_neutral"
        for row in rows
    )
    familywise_state_changes = sum(
        row["denominator_sensitivity"]["familywise_state_consistency"]
        == "changes_state"
        for row in rows
    )
    ratio_multipliers = [
        float(row["denominator_sensitivity"]["pressure_ratio_earned_to_direct_multiplier"])
        for row in rows
        if row["denominator_sensitivity"]["pressure_ratio_earned_to_direct_multiplier"]
        is not None
        and float(row["direct_candidate"]["ratio"]) > 0
    ]
    sensitivity_sorted = sorted(
        [
            row
            for row in rows
            if row["denominator_sensitivity"][
                "pressure_ratio_earned_to_direct_multiplier"
            ]
            is not None
            and float(row["direct_candidate"]["ratio"]) > 0
        ],
        key=lambda row: abs(
            math.log(
                float(
                    row["denominator_sensitivity"][
                        "pressure_ratio_earned_to_direct_multiplier"
                    ]
                )
            )
        ),
        reverse=True,
    )

    market = calibration.get("market_12m") or {}
    direct_market_complaints = _nonnegative_int(
        market.get("complaints") or 0,
        field="market_12m.complaints",
    )
    direct_market_premium = _finite(
        market.get("premium_direct") or 0.0,
        field="market_12m.premium_direct",
    )
    earned_context_entities = [
        row
        for row in entities
        if _optional_finite(
            row.get("premium_earned_12m_diagnostic"),
            field="premium_earned_12m_diagnostic",
        )
        is not None
    ]
    earned_context_premium = sum(
        float(row["premium_earned_12m_diagnostic"])
        for row in earned_context_entities
        if float(row["premium_earned_12m_diagnostic"]) > 0
    )

    return {
        "artifact": "v2_conduct_credibility_diagnostic",
        "generated_at": _utc_now(),
        "version": VERSION,
        "status": "experimental_diagnostic",
        "assessment_role": "credibility_and_denominator_guard_only",
        "scoring": "forbidden_in_this_artifact",
        "ranking": "forbidden_in_this_artifact",
        "human_model": {
            "primary_question": (
                "Ha reclamacoes demais para o tamanho da operacao, e temos dados suficientes para confiar nessa conclusao?"
            ),
            "principle": (
                "uma razao extrema nao vira verdade apenas por ser numericamente extrema"
            ),
            "frontend_status": "not_approved_for_public_classification",
        },
        "source": {
            "upstream_artifact": str(CALIBRATION_PATH),
            "upstream_version": calibration.get("version"),
            "months": months,
            "direct_candidate": "insurance_premium_direct",
            "earned_companion": "insurance_premium_earned",
        },
        "methodology": {
            "pressure_definition": ALIGNED_AGGREGATION_POLICY,
            "individual_uncertainty": {
                "method": "exact_poisson_standardized_ratio",
                "confidence_level": 1.0 - INDIVIDUAL_ALPHA,
                "role": "diagnostic_only",
            },
            "simultaneous_guard": {
                "method": "bonferroni_familywise_error_control",
                "familywise_alpha": FAMILYWISE_ALPHA,
                "direct_comparisons": direct_comparisons,
                "direct_per_entity_alpha": direct_familywise_alpha,
                "earned_comparisons_with_sufficient_temporal_coverage": earned_eligible,
                "earned_per_entity_alpha": direct_familywise_alpha,
                "role": "reduce_false_extreme_labels_when_many_entities_are_checked_at_once",
            },
            "shrinkage_applied": False,
            "empirical_bayes_applied": False,
            "denominator_selected": False,
            "portfolio_mix_adjusted": False,
            "guardrails": [
                "direct_pressure_must_equal_monthly_aligned_calibration_pressure",
                "annual_aggregate_market_is_context_not_pressure_baseline",
                "familywise_signal_does_not_validate_the_denominator",
                "familywise_signal_does_not_adjust_for_portfolio_mix",
                "below_reference_is_not_a_positive_conduct_grade",
                "missing_premium_is_not_zero_premium",
                "monthly_non_positive_premium_does_not_mean_no_active_policy",
                "premium_earned_remains_diagnostic_only",
                "sparse_premium_earned_cannot_veto_direct_signal",
            ],
        },
        "population": {
            "direct_candidate_entities": direct_comparisons,
            "earned_diagnostic_entities": earned_eligible,
            "earned_unavailable_entities": direct_comparisons - earned_eligible,
        },
        "market": {
            "direct_candidate": {
                "complaints": direct_market_complaints,
                "premium": direct_market_premium,
                "role": "annual_aggregate_context_not_pressure_baseline",
            },
            "earned_diagnostic": {
                "annual_positive_premium_context": float(earned_context_premium),
                "monthly_baselines": earned_baselines,
                "population_policy": "complaints_and_earned_premium_same_entities_same_month_only",
            },
        },
        "diagnostics": {
            "direct_individual_exact_states": dict(
                sorted(direct_individual_states.items())
            ),
            "direct_familywise_exact_states": dict(
                sorted(direct_familywise_states.items())
            ),
            "earned_familywise_exact_states": dict(
                sorted(earned_familywise_states.items())
            ),
            "temporal_overlap": {
                "entities_with_complaints_in_non_positive_direct_premium_months": entities_outside_direct,
                "complaints_in_non_positive_direct_premium_months": complaints_outside_direct,
            },
            "denominator_sensitivity": {
                "raw_neutral_side_changes": raw_side_changes,
                "familywise_state_changes": familywise_state_changes,
                "earned_to_direct_pressure_multiplier_quantiles": _quantiles(
                    ratio_multipliers
                ),
                "most_sensitive_observations": [
                    {
                        "entity_id": row["entity_id"],
                        "legal_name": row.get("legal_name"),
                        "complaints_12m": row["complaints_12m"],
                        "direct_ratio": row["direct_candidate"]["ratio"],
                        "earned_ratio": row["earned_diagnostic"].get("ratio"),
                        "premium_earned_to_direct_ratio": row[
                            "denominator_sensitivity"
                        ]["annual_premium_earned_to_direct_ratio"],
                        "pressure_multiplier": row["denominator_sensitivity"][
                            "pressure_ratio_earned_to_direct_multiplier"
                        ],
                        "familywise_state_consistency": row[
                            "denominator_sensitivity"
                        ]["familywise_state_consistency"],
                    }
                    for row in sensitivity_sorted[:10]
                ],
            },
        },
        "entities": sorted(rows, key=lambda row: str(row.get("entity_id") or "")),
    }


def build_from_file(path: Path = CALIBRATION_PATH) -> dict[str, Any]:
    calibration = json.loads(path.read_text(encoding="utf-8"))
    return build_credibility_diagnostic(calibration)


def main() -> None:
    payload = build_from_file()
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
                "direct_familywise_exact_states": payload["diagnostics"][
                    "direct_familywise_exact_states"
                ],
                "temporal_overlap": payload["diagnostics"]["temporal_overlap"],
                "denominator_sensitivity": payload["diagnostics"][
                    "denominator_sensitivity"
                ],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    print(f"written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
