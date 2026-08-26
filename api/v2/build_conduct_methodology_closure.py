from __future__ import annotations

import json
import math
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scipy.stats import beta, chi2

CALIBRATION_PATH = Path("data/derived/v2/conduct_comparative_calibration_v2.json")
RECONCILIATION_PATH = Path("data/derived/v2/conduct_coverage_reconciliation.json")
PORTFOLIO_PATH = Path("data/derived/v2/conduct_portfolio_mix_diagnostic.json")
OUTPUT_PATH = Path("data/derived/v2/conduct_methodology_closure.json")

VERSION = "2.0-draft-conduct-methodology-closure-1"
FAMILYWISE_ALPHA = 0.05
COMMON_MONTHS = 12
MIN_TEMPORAL_MONTHS = 9
PERSISTENCE_SHARE = 0.50
HALF_MIN_COMPARABLE_MONTHS = 3

RECOVERY_ROUTE_BY_STATE = {
    "hybrid_insurance_pension_requires_product_numerator": "recover_product_specific_complaint_numerator",
    "no_current_insurance_activity_observed": "audit_current_insurance_activity_and_exposure",
    "no_current_insurance_activity_observed_pension_activity_present": "audit_insurance_activity_without_using_pension_as_denominator",
    "negative_direct_premium_requires_accounting_review": "review_negative_direct_premium_accounting",
    "shared_consumer_subject_requires_product_split": "recover_product_or_carrier_specific_complaint_numerator",
    "consumer_subject_single_carrier_exposure_not_brand_specific": "recover_brand_specific_exposure",
    "multi_carrier_subject_requires_product_split": "recover_carrier_specific_product_numerator",
    "portfolio_transfer_counterparty_requires_temporal_reconciliation": "reconcile_portfolio_transfer_by_effective_date",
    "portfolio_transfer_requires_temporal_reconciliation": "reconcile_portfolio_transfer_by_effective_date",
    "runoff_pressure_not_applicable": "use_runoff_specific_conduct_context_without_current_pressure",
    "no_positive_insurance_premium_observed": "reconcile_positive_insurance_exposure",
    "shared_exposure_with_external_consumer_subject": "reconcile_external_subject_and_exposure",
}


class ConductMethodologyClosureError(RuntimeError):
    """Raised when the Conduct methodology cannot be closed safely."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _finite(value: Any, *, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ConductMethodologyClosureError(f"non-numeric {field}: {value!r}") from exc
    if not math.isfinite(number):
        raise ConductMethodologyClosureError(f"non-finite {field}: {value!r}")
    return number


def _poisson_ratio_interval(observed: int, expected: float, alpha: float) -> dict[str, Any]:
    """Exact Poisson interval for an observed/expected standardized ratio."""
    if observed < 0 or expected <= 0 or not 0 < alpha < 1:
        raise ConductMethodologyClosureError("invalid Poisson ratio interval inputs")
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


def _rate_ratio_interval(
    early_observed: int,
    early_expected: float,
    recent_observed: int,
    recent_expected: float,
    alpha: float,
) -> dict[str, Any] | None:
    """Exact conditional interval for recent/early standardized pressure."""
    total = early_observed + recent_observed
    if (
        early_observed < 0
        or recent_observed < 0
        or early_expected <= 0
        or recent_expected <= 0
        or total <= 0
        or not 0 < alpha < 1
    ):
        return None

    lower_p = 0.0 if recent_observed == 0 else float(
        beta.ppf(alpha / 2.0, recent_observed, early_observed + 1)
    )
    upper_p = 1.0 if early_observed == 0 else float(
        beta.ppf(1.0 - alpha / 2.0, recent_observed + 1, early_observed)
    )

    def convert(probability: float) -> float | None:
        if probability <= 0:
            return 0.0
        if probability >= 1:
            return None
        return (
            probability
            / (1.0 - probability)
            * early_expected
            / recent_expected
        )

    if early_observed == 0:
        point = None
    else:
        point = (
            (recent_observed / recent_expected)
            / (early_observed / early_expected)
        )

    lower = convert(lower_p)
    upper = convert(upper_p)
    if lower is not None and lower > 1.0:
        state = "deteriorating_pressure"
    elif upper is not None and upper < 1.0:
        state = "improving_pressure"
    else:
        state = "no_clear_change"

    return {
        "method": "exact_conditional_poisson_rate_ratio",
        "recent_to_early_pressure_ratio": point,
        "lower": lower,
        "upper": upper,
        "reference": 1.0,
        "state": state,
        "alpha": float(alpha),
        "confidence_level": float(1.0 - alpha),
    }


def _baselines(
    entities: list[dict[str, Any]],
    premium_key: str,
) -> dict[str, dict[str, Any]]:
    months = sorted(
        {
            str(month.get("month") or "")
            for entity in entities
            for month in entity.get("monthly") or []
            if str(month.get("month") or "")
        }
    )
    if len(months) != COMMON_MONTHS:
        raise ConductMethodologyClosureError(
            f"expected {COMMON_MONTHS} common months, found {len(months)}"
        )

    result: dict[str, dict[str, Any]] = {}
    for month_name in months:
        comparable: list[dict[str, Any]] = []
        for entity in entities:
            month = next(
                (
                    item
                    for item in entity.get("monthly") or []
                    if str(item.get("month") or "") == month_name
                ),
                None,
            )
            if month is None:
                raise ConductMethodologyClosureError(
                    f"entity {entity.get('entity_id')} misses month {month_name}"
                )
            if _finite(month.get(premium_key) or 0.0, field=premium_key) > 0:
                comparable.append(month)

        market_complaints = sum(int(item.get("complaints") or 0) for item in comparable)
        market_premium = sum(
            _finite(item.get(premium_key) or 0.0, field=premium_key)
            for item in comparable
        )
        result[month_name] = {
            "comparable_entities": len(comparable),
            "market_complaints": market_complaints,
            "market_premium": float(market_premium),
        }
    return result


def _longest_run(states: list[str], target: str) -> int:
    best = 0
    current = 0
    for state in states:
        if state == target:
            current += 1
            best = max(best, current)
        else:
            current = 0
    return best


def _series(
    entity: dict[str, Any],
    premium_key: str,
    baselines: dict[str, dict[str, Any]],
    *,
    annual_alpha: float,
    monthly_alpha: float,
) -> dict[str, Any]:
    points: list[dict[str, Any]] = []
    aligned_observed = 0
    aligned_expected = 0.0
    excluded_complaints = 0

    for month in entity.get("monthly") or []:
        month_name = str(month.get("month") or "")
        premium = _finite(month.get(premium_key) or 0.0, field=premium_key)
        complaints = int(month.get("complaints") or 0)
        baseline = baselines[month_name]
        market_premium = float(baseline["market_premium"])
        market_complaints = int(baseline["market_complaints"])

        if premium <= 0 or market_premium <= 0:
            excluded_complaints += complaints
            points.append(
                {
                    "month": month_name,
                    "state": "unavailable_non_positive_comparable_exposure",
                    "complaints": complaints,
                    "premium": premium,
                    "expected_complaints": None,
                    "pressure_ratio": None,
                    "uncertainty": None,
                }
            )
            continue

        expected = market_complaints * premium / market_premium
        interval = _poisson_ratio_interval(complaints, expected, monthly_alpha)
        aligned_observed += complaints
        aligned_expected += expected
        points.append(
            {
                "month": month_name,
                "state": "available",
                "complaints": complaints,
                "premium": premium,
                "expected_complaints": float(expected),
                "pressure_ratio": float(complaints / expected) if expected > 0 else None,
                "uncertainty": interval,
            }
        )

    comparable_points = [point for point in points if point["state"] == "available"]
    if aligned_expected <= 0:
        annual = {
            "state": "unavailable",
            "observed_complaints": aligned_observed,
            "expected_complaints": None,
            "ratio": None,
            "uncertainty": None,
        }
    else:
        interval = _poisson_ratio_interval(
            aligned_observed, aligned_expected, annual_alpha
        )
        annual = {
            "state": "available",
            "observed_complaints": aligned_observed,
            "expected_complaints": float(aligned_expected),
            "ratio": float(aligned_observed / aligned_expected),
            "uncertainty": interval,
        }

    monthly_states = [
        (
            point["uncertainty"]["state"]
            if point.get("uncertainty")
            else "unavailable"
        )
        for point in points
    ]
    above = sum(state == "above_size_proportional_reference" for state in monthly_states)
    below = sum(state == "below_size_proportional_reference" for state in monthly_states)
    indeterminate = sum(
        state == "not_distinguishable_from_size_proportional_reference"
        for state in monthly_states
    )
    available = len(comparable_points)

    annual_state = (
        annual["uncertainty"]["state"] if annual.get("uncertainty") else "unavailable"
    )
    required_persistent_months = (
        math.ceil(available * PERSISTENCE_SHARE) if available else None
    )
    if available < MIN_TEMPORAL_MONTHS:
        persistence_state = "insufficient_temporal_coverage"
    elif annual_state == "above_size_proportional_reference":
        persistence_state = (
            "persistent_above_expected"
            if above >= int(required_persistent_months or 0)
            else "episodic_or_sparse_above_expected"
        )
    elif annual_state == "below_size_proportional_reference":
        persistence_state = (
            "persistent_below_expected"
            if below >= int(required_persistent_months or 0)
            else "episodic_or_sparse_below_expected"
        )
    else:
        persistence_state = "not_distinguishable_from_expected"

    midpoint = COMMON_MONTHS // 2
    early_points = points[:midpoint]
    recent_points = points[midpoint:]

    def half_summary(half: list[dict[str, Any]]) -> tuple[int, float, int]:
        valid = [point for point in half if point["state"] == "available"]
        return (
            sum(int(point["complaints"]) for point in valid),
            sum(float(point["expected_complaints"]) for point in valid),
            len(valid),
        )

    early_observed, early_expected, early_months = half_summary(early_points)
    recent_observed, recent_expected, recent_months = half_summary(recent_points)
    if (
        early_months < HALF_MIN_COMPARABLE_MONTHS
        or recent_months < HALF_MIN_COMPARABLE_MONTHS
    ):
        trend = {
            "state": "insufficient_temporal_coverage",
            "early_comparable_months": early_months,
            "recent_comparable_months": recent_months,
            "interval": None,
        }
    else:
        interval = _rate_ratio_interval(
            early_observed,
            early_expected,
            recent_observed,
            recent_expected,
            annual_alpha,
        )
        trend = {
            "state": interval["state"] if interval else "insufficient_events",
            "early_comparable_months": early_months,
            "recent_comparable_months": recent_months,
            "early_observed": early_observed,
            "early_expected": float(early_expected),
            "recent_observed": recent_observed,
            "recent_expected": float(recent_expected),
            "interval": interval,
        }

    return {
        "premium_field": premium_key,
        "aggregation_policy": "sum_monthly_expected_then_observed_divided_by_expected",
        "annual": annual,
        "monthly": points,
        "temporal_coverage": {
            "comparable_months": available,
            "unavailable_months": COMMON_MONTHS - available,
            "complaints_excluded_from_pressure_in_non_comparable_months": excluded_complaints,
        },
        "persistence": {
            "state": persistence_state,
            "monthly_familywise_within_entity": True,
            "credible_above_months": above,
            "credible_below_months": below,
            "indeterminate_months": indeterminate,
            "unavailable_months": COMMON_MONTHS - available,
            "required_persistent_months": required_persistent_months,
            "longest_credible_above_run": _longest_run(
                monthly_states, "above_size_proportional_reference"
            ),
            "longest_credible_below_run": _longest_run(
                monthly_states, "below_size_proportional_reference"
            ),
        },
        "trend": trend,
    }


def _satisfaction_context(entity: dict[str, Any]) -> dict[str, Any]:
    satisfaction = entity.get("satisfaction") or {}
    trend = satisfaction.get("trend") or {}
    early_sample = int(trend.get("early_half_sample") or 0)
    recent_sample = int(trend.get("recent_half_sample") or 0)
    direction = str(trend.get("direction") or "insufficient")
    if early_sample < 10 or recent_sample < 10:
        usable_direction = "insufficient_sample"
    else:
        usable_direction = direction
    return {
        "role": "context_only_not_pressure_weight",
        "sample_count": int(satisfaction.get("sample_count") or 0),
        "average": satisfaction.get("average"),
        "early_half_average": trend.get("early_half_average"),
        "early_half_sample": early_sample,
        "recent_half_average": trend.get("recent_half_average"),
        "recent_half_sample": recent_sample,
        "direction": usable_direction,
        "guard": "satisfaction_does_not_measure_complaint_incidence",
    }


def _final_pressure_state(
    direct: dict[str, Any],
    earned: dict[str, Any],
) -> tuple[str, str]:
    direct_coverage = int(direct["temporal_coverage"]["comparable_months"])
    direct_interval = (direct.get("annual") or {}).get("uncertainty")
    if direct_coverage < MIN_TEMPORAL_MONTHS or not direct_interval:
        return (
            "pressure_unavailable_insufficient_temporal_coverage",
            "Nao ha meses comparaveis suficientes para uma conclusao anual.",
        )

    direct_state = str(direct_interval["state"])
    earned_interval = (earned.get("annual") or {}).get("uncertainty")
    if earned_interval is not None:
        earned_state = str(earned_interval["state"])
        if earned_state != direct_state:
            return (
                "pressure_inconclusive_denominator_sensitivity",
                "A conclusao muda de estado quando usamos a medida diagnostica de premio ganho.",
            )

    if direct_state == "above_size_proportional_reference":
        return (
            "above_expected_with_sufficient_evidence",
            "Ha mais reclamacoes do que esperariamos para o tamanho da operacao nos meses comparaveis.",
        )
    if direct_state == "below_size_proportional_reference":
        return (
            "below_expected_with_sufficient_evidence",
            "Ha menos reclamacoes do que esperariamos para o tamanho da operacao nos meses comparaveis; isso nao prova melhor atendimento.",
        )
    return (
        "not_distinguishable_from_expected",
        "Os dados nao mostram diferenca suficientemente clara em relacao ao esperado para o tamanho da operacao.",
    )


def build_closure(
    calibration: dict[str, Any],
    reconciliation: dict[str, Any],
    portfolio: dict[str, Any],
) -> dict[str, Any]:
    if calibration.get("scoring") != "forbidden_in_this_artifact":
        raise ConductMethodologyClosureError("upstream calibration must forbid scoring")
    if reconciliation.get("scoring") not in {
        "forbidden_in_this_artifact",
        "forbidden",
        None,
    }:
        raise ConductMethodologyClosureError("unexpected reconciliation scoring state")
    if portfolio.get("scoring") != "forbidden_in_this_artifact":
        raise ConductMethodologyClosureError("portfolio diagnostic must forbid scoring")

    candidates = list(calibration.get("entities") or [])
    if len(candidates) != 103:
        raise ConductMethodologyClosureError(
            f"expected 103 current pressure candidates, found {len(candidates)}"
        )
    reconciliation_entities = list(reconciliation.get("entities") or [])
    if len(reconciliation_entities) != 157:
        raise ConductMethodologyClosureError(
            f"expected 157 reconciled insurers, found {len(reconciliation_entities)}"
        )

    direct_baselines = _baselines(candidates, "premium_direct")
    earned_baselines = _baselines(candidates, "premium_earned_diagnostic")
    annual_alpha = FAMILYWISE_ALPHA / len(candidates)
    monthly_alpha = FAMILYWISE_ALPHA / COMMON_MONTHS

    portfolio_by_id = {
        str(row.get("entity_id") or ""): row
        for row in portfolio.get("entities") or []
    }
    candidate_by_id = {
        str(row.get("entity_id") or ""): row for row in candidates
    }

    candidate_rows: list[dict[str, Any]] = []
    for entity_id, entity in sorted(candidate_by_id.items()):
        direct = _series(
            entity,
            "premium_direct",
            direct_baselines,
            annual_alpha=annual_alpha,
            monthly_alpha=monthly_alpha,
        )
        earned = _series(
            entity,
            "premium_earned_diagnostic",
            earned_baselines,
            annual_alpha=annual_alpha,
            monthly_alpha=monthly_alpha,
        )
        final_state, human_summary = _final_pressure_state(direct, earned)

        portfolio_entity = portfolio_by_id.get(entity_id) or {}
        portfolio_context = portfolio_entity.get("portfolio") or {}

        candidate_rows.append(
            {
                "entity_id": entity_id,
                "fip_code": entity.get("fip_code"),
                "cnpj": entity.get("cnpj"),
                "legal_name": entity.get("legal_name"),
                "display_name": entity.get("display_name"),
                "conduct_observed_complaints_12m": int(
                    entity.get("complaints_12m") or 0
                ),
                "comparability_state": "direct_one_to_one_candidate",
                "pressure_conclusion": {
                    "state": final_state,
                    "human_summary": human_summary,
                    "not_a_customer_incidence_rate": True,
                    "not_a_quality_certification": True,
                },
                "direct_pressure": direct,
                "earned_sensitivity": {
                    **earned,
                    "role": "diagnostic_guard_only",
                    "selected_as_denominator": False,
                    "annual_state_consistent_with_direct": (
                        (
                            (earned.get("annual") or {}).get("uncertainty") or {}
                        ).get("state")
                        == (
                            (direct.get("annual") or {}).get("uncertainty") or {}
                        ).get("state")
                    )
                    if (earned.get("annual") or {}).get("uncertainty")
                    else None,
                },
                "portfolio_context": {
                    "role": "context_and_sensitivity_only",
                    "nearest_distance": portfolio_context.get("nearest_distance"),
                    "fifth_nearest_distance": portfolio_context.get(
                        "fifth_nearest_distance"
                    ),
                    "peer_group_selected": False,
                    "pressure_adjusted_for_portfolio": False,
                },
                "satisfaction": _satisfaction_context(entity),
                "remediation": {
                    "state": "not_established_from_current_p3",
                    "reason": (
                        "O core P3 preservado nao oferece denominador avaliado suficiente "
                        "para inferir resolucao; resposta e finalizacao nao provam remediacao."
                    ),
                },
            }
        )

    reconciliation_by_id = {
        str(row.get("entity_id") or ""): row for row in reconciliation_entities
    }
    noncomparable_rows: list[dict[str, Any]] = []
    for entity_id, row in sorted(reconciliation_by_id.items()):
        pressure = row.get("pressure_comparability") or {}
        if (
            str(pressure.get("state") or "") == "direct_one_to_one_candidate"
            and bool(pressure.get("pressure_eligible_candidate"))
        ):
            continue
        state = str(pressure.get("state") or "unknown")
        noncomparable_rows.append(
            {
                "entity_id": entity_id,
                "fip_code": row.get("fip_code"),
                "cnpj": row.get("cnpj"),
                "legal_name": row.get("legal_name"),
                "display_name": row.get("display_name"),
                "conduct_evidence_state": row.get("conduct_evidence_state"),
                "complaints_12m": row.get("complaints_12m"),
                "pressure_conclusion": {
                    "state": "pressure_unavailable_not_comparable",
                    "comparability_reason": state,
                    "reason_code": pressure.get("reason_code"),
                    "recovery_route": RECOVERY_ROUTE_BY_STATE.get(
                        state, "manual_methodology_review"
                    ),
                    "human_summary": (
                        "Ha dados de Conduta, mas nao ha numerador e denominador "
                        "comparaveis suficientes para calcular pressao sem inventar atribuicoes."
                    ),
                },
            }
        )

    if len(noncomparable_rows) != 54:
        raise ConductMethodologyClosureError(
            f"expected 54 non-comparable entities, found {len(noncomparable_rows)}"
        )

    conclusion_counts = Counter(
        str(row["pressure_conclusion"]["state"]) for row in candidate_rows
    )
    persistence_counts = Counter(
        str(row["direct_pressure"]["persistence"]["state"])
        for row in candidate_rows
    )
    trend_counts = Counter(
        str(row["direct_pressure"]["trend"]["state"]) for row in candidate_rows
    )
    satisfaction_counts = Counter(
        str(row["satisfaction"]["direction"]) for row in candidate_rows
    )

    portfolio_association = (
        (portfolio.get("diagnostics") or {})
        .get("portfolio_distance_vs_pressure_difference", {})
    )
    portfolio_coverage = (
        (portfolio.get("diagnostics") or {}).get("peer_coverage_curve") or []
    )

    return {
        "artifact": "v2_conduct_methodology_closure",
        "generated_at": _utc_now(),
        "version": VERSION,
        "status": "conduct_methodology_closed_for_signal_design",
        "assessment_role": "final_conduct_evidence_and_comparability_contract_before_score_calibration",
        "scoring": "forbidden_in_this_artifact",
        "ranking": "forbidden_in_this_artifact",
        "human_model": {
            "pressure_question": "Reclama muito para o tamanho da operacao?",
            "credibility_question": "Temos dados suficientes para confiar nessa diferenca?",
            "time_question": "O sinal aparece de forma repetida ou parece episodico?",
            "trend_question": "A pressao esta melhorando, piorando ou sem mudanca clara?",
            "portfolio_question": "A carteira e diferente o bastante para exigir cautela na comparacao?",
            "satisfaction_question": "Entre quem avaliou, como foi a experiencia e qual o tamanho da amostra?",
            "silence_rule": "quando a evidencia nao sustenta a frase, a ferramenta nao afirma",
        },
        "methodology_decisions": {
            "exposure_domain": "insurance_only",
            "approved_operational_denominator": {
                "field": "insurance_premium_direct",
                "source": "Ses_seguros.csv:premio_direto",
                "scope": "direct_one_to_one_candidate_only",
                "approved_for_conduct_pressure": True,
                "not_customer_count": True,
                "private_pension_excluded": True,
                "capitalization_excluded": True,
                "monthly_alignment_required": True,
                "annual_aggregation": "sum_monthly_expected_then_observed_divided_by_expected",
            },
            "uncertainty": {
                "annual_method": "exact_poisson_standardized_ratio",
                "annual_familywise_alpha": FAMILYWISE_ALPHA,
                "annual_comparisons": len(candidates),
                "annual_per_entity_alpha": annual_alpha,
                "monthly_method": "exact_poisson_standardized_ratio",
                "monthly_familywise_within_entity": True,
                "monthly_per_month_alpha": monthly_alpha,
                "shrinkage_selected": False,
                "empirical_bayes_selected": False,
                "reason": (
                    "A etapa atual precisa impedir conclusoes frageis, nao estimar uma "
                    "magnitude suavizada para score."
                ),
            },
            "persistence": {
                "minimum_comparable_months": MIN_TEMPORAL_MONTHS,
                "persistent_share_of_comparable_months": PERSISTENCE_SHARE,
                "role": "separate_repeated_signal_from_episodic_or_sparse_signal",
            },
            "trend": {
                "comparison": "recent_6m_vs_early_6m",
                "minimum_comparable_months_per_half": HALF_MIN_COMPARABLE_MONTHS,
                "method": "exact_conditional_poisson_rate_ratio",
                "familywise_across_entities": True,
            },
            "portfolio_mix": {
                "adjustment_selected": False,
                "peer_groups_selected": False,
                "distance_threshold_selected": False,
                "reason": (
                    "A associacao entre distancia de carteira e diferenca de pressao e fraca, "
                    "e limiares estreitos deixam a maior parte do universo sem peers; coramo "
                    "permanece contexto e diagnostico de sensibilidade."
                ),
                "all_pairs_spearman": portfolio_association.get(
                    "all_pairs_spearman"
                ),
                "high_volume_pairs_spearman": portfolio_association.get(
                    "high_volume_100_plus_pairs_spearman"
                ),
                "peer_coverage_curve": portfolio_coverage,
            },
            "satisfaction": {
                "role": "context_only_not_pressure_weight",
                "minimum_sample_per_half_for_direction": 10,
            },
            "remediation": {
                "selected": False,
                "state": "not_established_from_current_p3",
                "reason": (
                    "Responder/finalizar nao prova remediacao e o denominador de resolucao "
                    "nao esta preservado de forma suficiente no P3 atual."
                ),
            },
            "non_comparable_entities": {
                "remain_searchable": True,
                "pressure_is_null": True,
                "no_weight_redistribution": True,
                "recovery_routes_preserved": True,
            },
        },
        "population": {
            "regulatory_universe": len(reconciliation_entities),
            "pressure_candidates": len(candidate_rows),
            "pressure_unavailable_not_comparable": len(noncomparable_rows),
        },
        "diagnostics": {
            "pressure_conclusion_counts": dict(sorted(conclusion_counts.items())),
            "persistence_counts": dict(sorted(persistence_counts.items())),
            "trend_counts": dict(sorted(trend_counts.items())),
            "satisfaction_direction_counts": dict(sorted(satisfaction_counts.items())),
        },
        "candidate_entities": candidate_rows,
        "non_comparable_entities": noncomparable_rows,
        "closure": {
            "conduct_foundation_complete": True,
            "conduct_comparability_contract_complete": True,
            "conduct_signal_contract_complete": True,
            "conduct_numeric_score_defined": False,
            "next_stage": "cross_pillar_score_calibration_after_financial_methodology_closure",
            "important": (
                "Fechar Conduta significa saber quando e como afirmar algo; "
                "nao significa fabricar pressao para os 54 casos sem comparabilidade."
            ),
        },
    }


def build_from_files() -> dict[str, Any]:
    return build_closure(
        json.loads(CALIBRATION_PATH.read_text(encoding="utf-8")),
        json.loads(RECONCILIATION_PATH.read_text(encoding="utf-8")),
        json.loads(PORTFOLIO_PATH.read_text(encoding="utf-8")),
    )


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
                "status": payload["status"],
                "population": payload["population"],
                "diagnostics": payload["diagnostics"],
                "closure": payload["closure"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    print(f"written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
