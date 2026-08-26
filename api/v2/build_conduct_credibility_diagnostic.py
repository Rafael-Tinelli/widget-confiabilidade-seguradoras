from __future__ import annotations

import json
import math
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
from scipy.stats import beta

CALIBRATION_PATH = Path("data/derived/v2/conduct_comparative_calibration_v2.json")
OUTPUT_PATH = Path("data/derived/v2/conduct_credibility_diagnostic.json")
VERSION = "2.0-draft-conduct-credibility-1"
INDIVIDUAL_ALPHA = 0.05
FAMILYWISE_ALPHA = 0.05


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


def _exact_binomial_share_interval(
    observed: int,
    market_complaints: int,
    alpha: float,
) -> tuple[float, float]:
    """Clopper-Pearson interval for one entity's share of market complaints."""
    if observed < 0 or market_complaints <= 0 or observed > market_complaints:
        raise ConductCredibilityDiagnosticError(
            "invalid counts for exact binomial interval"
        )
    if not 0 < alpha < 1:
        raise ConductCredibilityDiagnosticError("alpha must be between 0 and 1")
    lower = 0.0 if observed == 0 else float(
        beta.ppf(alpha / 2.0, observed, market_complaints - observed + 1)
    )
    upper = 1.0 if observed == market_complaints else float(
        beta.ppf(
            1.0 - alpha / 2.0,
            observed + 1,
            market_complaints - observed,
        )
    )
    return lower, upper


def _pressure_interval(
    observed: int,
    market_complaints: int,
    exposure_share: float,
    alpha: float,
) -> dict[str, Any]:
    if exposure_share <= 0 or exposure_share > 1:
        raise ConductCredibilityDiagnosticError(
            f"invalid exposure share: {exposure_share!r}"
        )
    lower_share, upper_share = _exact_binomial_share_interval(
        observed,
        market_complaints,
        alpha,
    )
    lower = lower_share / exposure_share
    upper = upper_share / exposure_share
    if lower > 1.0:
        state = "above_size_proportional_reference"
    elif upper < 1.0:
        state = "below_size_proportional_reference"
    else:
        state = "not_distinguishable_from_size_proportional_reference"
    return {
        "alpha": float(alpha),
        "confidence_level": float(1.0 - alpha),
        "lower": float(lower),
        "upper": float(upper),
        "reference": 1.0,
        "state": state,
    }


def _temporal_overlap(entity: dict[str, Any], premium_key: str) -> dict[str, Any]:
    monthly = entity.get("monthly") or []
    complaints_total = int(entity.get("complaints_12m") or 0)
    positive_months = 0
    non_positive_months = 0
    complaints_positive = 0
    complaints_non_positive = 0

    for row in monthly:
        premium = _finite(row.get(premium_key) or 0.0, field=premium_key)
        complaints = int(row.get("complaints") or 0)
        if premium > 0:
            positive_months += 1
            complaints_positive += complaints
        else:
            non_positive_months += 1
            complaints_non_positive += complaints

    if complaints_total == 0:
        state = "no_observed_complaints"
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
        "complaints_in_positive_premium_months": complaints_positive,
        "complaints_in_non_positive_premium_months": complaints_non_positive,
        "complaint_share_in_non_positive_premium_months": (
            complaints_non_positive / complaints_total if complaints_total > 0 else None
        ),
        "interpretation_guard": (
            "non_positive_monthly_premium_is_not_equivalent_to_no_active_policy_or_no_consumer_exposure"
        ),
    }


def _ratio(
    observed: int,
    exposure: float,
    market_complaints: int,
    market_exposure: float,
) -> tuple[float, float, float]:
    if exposure <= 0 or market_exposure <= 0 or market_complaints <= 0:
        raise ConductCredibilityDiagnosticError("ratio requires positive aligned exposure")
    exposure_share = exposure / market_exposure
    expected = market_complaints * exposure_share
    return float(expected), float(observed / expected), float(exposure_share)


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

    market = calibration.get("market_12m") or {}
    direct_market_complaints = int(market.get("complaints") or 0)
    direct_market_premium = _finite(
        market.get("premium_direct") or 0.0,
        field="market_premium_direct",
    )
    if direct_market_complaints <= 0 or direct_market_premium <= 0:
        raise ConductCredibilityDiagnosticError(
            "direct calibration market must have positive complaints and premium"
        )

    direct_comparisons = len(entities)
    direct_familywise_alpha = FAMILYWISE_ALPHA / direct_comparisons

    earned_entities = [
        entity
        for entity in entities
        if _finite(
            entity.get("premium_earned_12m_diagnostic") or 0.0,
            field="premium_earned_12m_diagnostic",
        )
        > 0
    ]
    earned_market_complaints = sum(
        int(entity.get("complaints_12m") or 0) for entity in earned_entities
    )
    earned_market_premium = sum(
        _finite(
            entity.get("premium_earned_12m_diagnostic") or 0.0,
            field="premium_earned_12m_diagnostic",
        )
        for entity in earned_entities
    )
    earned_comparisons = len(earned_entities)
    earned_familywise_alpha = (
        FAMILYWISE_ALPHA / earned_comparisons if earned_comparisons else None
    )
    earned_ids = {str(entity.get("entity_id") or "") for entity in earned_entities}

    rows: list[dict[str, Any]] = []
    for entity in entities:
        entity_id = str(entity.get("entity_id") or "")
        observed = int(entity.get("complaints_12m") or 0)
        direct = _finite(entity.get("premium_direct_12m") or 0.0, field="premium_direct_12m")
        expected_direct, ratio_direct, direct_share = _ratio(
            observed,
            direct,
            direct_market_complaints,
            direct_market_premium,
        )
        direct_individual = _pressure_interval(
            observed,
            direct_market_complaints,
            direct_share,
            INDIVIDUAL_ALPHA,
        )
        direct_familywise = _pressure_interval(
            observed,
            direct_market_complaints,
            direct_share,
            direct_familywise_alpha,
        )

        earned = _finite(
            entity.get("premium_earned_12m_diagnostic") or 0.0,
            field="premium_earned_12m_diagnostic",
        )
        earned_diagnostic: dict[str, Any]
        if (
            entity_id in earned_ids
            and earned_market_complaints > 0
            and earned_market_premium > 0
            and earned_familywise_alpha is not None
        ):
            expected_earned, ratio_earned, earned_share = _ratio(
                observed,
                earned,
                earned_market_complaints,
                earned_market_premium,
            )
            earned_diagnostic = {
                "state": "available_diagnostic_only",
                "premium_earned_12m": earned,
                "premium_share": earned_share,
                "expected_complaints": expected_earned,
                "ratio": ratio_earned,
                "individual_exact_interval": _pressure_interval(
                    observed,
                    earned_market_complaints,
                    earned_share,
                    INDIVIDUAL_ALPHA,
                ),
                "familywise_exact_interval": _pressure_interval(
                    observed,
                    earned_market_complaints,
                    earned_share,
                    earned_familywise_alpha,
                ),
                "selected_as_denominator": False,
            }
        else:
            earned_diagnostic = {
                "state": "unavailable_non_positive_annual_premium_earned",
                "premium_earned_12m": earned,
                "selected_as_denominator": False,
            }

        if earned_diagnostic["state"] == "available_diagnostic_only":
            ratio_earned = float(earned_diagnostic["ratio"])
            earned_familywise_state = str(
                earned_diagnostic["familywise_exact_interval"]["state"]
            )
            ratio_multiplier = (
                ratio_earned / ratio_direct if ratio_direct > 0 else None
            )
            sensitivity = {
                "annual_premium_earned_to_direct_ratio": earned / direct,
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
            }
        else:
            sensitivity = {
                "annual_premium_earned_to_direct_ratio": earned / direct,
                "pressure_ratio_earned_to_direct_multiplier": None,
                "raw_neutral_side_consistency": "earned_unavailable",
                "familywise_state_consistency": "earned_unavailable",
            }

        rows.append(
            {
                "entity_id": entity_id,
                "fip_code": entity.get("fip_code"),
                "cnpj": entity.get("cnpj"),
                "legal_name": entity.get("legal_name"),
                "display_name": entity.get("display_name"),
                "complaints_12m": observed,
                "direct_candidate": {
                    "premium_direct_12m": direct,
                    "premium_share": direct_share,
                    "expected_complaints": expected_direct,
                    "ratio": ratio_direct,
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
        if row["earned_diagnostic"]["state"] == "available_diagnostic_only"
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
        1
        for row in rows
        if int(
            row["temporal_overlap"]["premium_direct"][
                "complaints_in_non_positive_premium_months"
            ]
        )
        > 0
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
            "months": (calibration.get("source") or {}).get("months"),
            "direct_candidate": "insurance_premium_direct",
            "earned_companion": "insurance_premium_earned",
        },
        "methodology": {
            "pressure_definition": "complaint_share_divided_by_premium_share",
            "individual_uncertainty": {
                "method": "clopper_pearson_exact_binomial",
                "confidence_level": 1.0 - INDIVIDUAL_ALPHA,
                "role": "diagnostic_only",
            },
            "simultaneous_guard": {
                "method": "bonferroni_familywise_error_control",
                "familywise_alpha": FAMILYWISE_ALPHA,
                "direct_comparisons": direct_comparisons,
                "direct_per_entity_alpha": direct_familywise_alpha,
                "earned_comparisons": earned_comparisons,
                "earned_per_entity_alpha": earned_familywise_alpha,
                "role": "reduce_false_extreme_labels_when_many_entities_are_checked_at_once",
            },
            "shrinkage_applied": False,
            "empirical_bayes_applied": False,
            "denominator_selected": False,
            "portfolio_mix_adjusted": False,
            "guardrails": [
                "familywise_signal_does_not_validate_the_denominator",
                "familywise_signal_does_not_adjust_for_portfolio_mix",
                "below_reference_is_not_a_positive_conduct_grade",
                "monthly_non_positive_premium_does_not_mean_no_active_policy",
                "premium_earned_remains_diagnostic_only",
            ],
        },
        "population": {
            "direct_candidate_entities": direct_comparisons,
            "earned_diagnostic_entities": earned_comparisons,
            "earned_unavailable_entities": direct_comparisons - earned_comparisons,
        },
        "market": {
            "direct_candidate": {
                "complaints": direct_market_complaints,
                "premium": direct_market_premium,
            },
            "earned_diagnostic": {
                "complaints": earned_market_complaints,
                "premium": float(earned_market_premium),
                "population_policy": "complaints_and_earned_premium_same_entities_only",
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
