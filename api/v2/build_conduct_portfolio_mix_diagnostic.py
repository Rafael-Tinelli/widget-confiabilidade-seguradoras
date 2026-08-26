from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
from scipy.stats import spearmanr

CALIBRATION_PATH = Path("data/derived/v2/conduct_comparative_calibration_v2.json")
CREDIBILITY_PATH = Path("data/derived/v2/conduct_credibility_diagnostic.json")
OUTPUT_PATH = Path("data/derived/v2/conduct_portfolio_mix_diagnostic.json")
VERSION = "2.0-draft-conduct-portfolio-mix-1"
DISTANCE_GRID = (0.05, 0.10, 0.15, 0.20, 0.25, 0.30, 0.40, 0.50, 0.60)
CONTINUITY_CORRECTION = 0.5


class ConductPortfolioMixDiagnosticError(RuntimeError):
    """Raised when the portfolio-mix diagnostic cannot be built safely."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _finite(value: Any, *, field: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ConductPortfolioMixDiagnosticError(
            f"non-numeric {field}: {value!r}"
        ) from exc
    if not math.isfinite(number):
        raise ConductPortfolioMixDiagnosticError(f"non-finite {field}: {value!r}")
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


def _tvd(left: dict[str, float], right: dict[str, float]) -> float:
    keys = set(left) | set(right)
    return 0.5 * sum(abs(left.get(key, 0.0) - right.get(key, 0.0)) for key in keys)


def _safe_spearman(x: list[float], y: list[float]) -> float | None:
    if len(x) < 3 or len(y) != len(x):
        return None
    result = spearmanr(x, y)
    statistic = float(result.statistic)
    return statistic if math.isfinite(statistic) else None


def _side(value: float | None) -> str:
    if value is None:
        return "unavailable"
    if value > 1.0:
        return "above"
    if value < 1.0:
        return "below"
    return "equal"


def _stabilized_log_pressure(observed: int, expected: float) -> float:
    if observed < 0 or expected < 0:
        raise ConductPortfolioMixDiagnosticError(
            "stabilized log pressure requires non-negative observed and expected"
        )
    return math.log(
        (observed + CONTINUITY_CORRECTION) / (expected + CONTINUITY_CORRECTION)
    )


def _local_pressure(
    entity: dict[str, Any],
    peers: list[dict[str, Any]],
) -> dict[str, Any]:
    if not peers:
        return {
            "state": "unavailable_no_peers_within_distance",
            "group_entity_count": 1,
            "peer_count": 0,
            "group_complaints": int(entity.get("complaints_12m") or 0),
            "group_premium_direct": _finite(
                entity.get("premium_direct_12m") or 0.0,
                field="premium_direct_12m",
            ),
            "expected_complaints": None,
            "ratio": None,
        }

    group = [entity, *peers]
    complaints = sum(int(row.get("complaints_12m") or 0) for row in group)
    premium = sum(
        _finite(row.get("premium_direct_12m") or 0.0, field="premium_direct_12m")
        for row in group
    )
    entity_premium = _finite(
        entity.get("premium_direct_12m") or 0.0,
        field="premium_direct_12m",
    )
    observed = int(entity.get("complaints_12m") or 0)
    if complaints <= 0 or premium <= 0 or entity_premium <= 0:
        return {
            "state": "unavailable_non_positive_aligned_local_baseline",
            "group_entity_count": len(group),
            "peer_count": len(peers),
            "group_complaints": complaints,
            "group_premium_direct": premium,
            "expected_complaints": None,
            "ratio": None,
        }
    expected = complaints * (entity_premium / premium)
    return {
        "state": "available_diagnostic_only",
        "group_entity_count": len(group),
        "peer_count": len(peers),
        "group_complaints": complaints,
        "group_premium_direct": premium,
        "expected_complaints": float(expected),
        "ratio": float(observed / expected) if expected > 0 else None,
    }


def build_portfolio_mix_diagnostic(
    calibration: dict[str, Any],
    credibility: dict[str, Any],
) -> dict[str, Any]:
    for name, upstream in (
        ("calibration", calibration),
        ("credibility", credibility),
    ):
        if upstream.get("scoring") != "forbidden_in_this_artifact":
            raise ConductPortfolioMixDiagnosticError(
                f"upstream {name} artifact must explicitly forbid scoring"
            )
        if upstream.get("ranking") != "forbidden_in_this_artifact":
            raise ConductPortfolioMixDiagnosticError(
                f"upstream {name} artifact must explicitly forbid ranking"
            )

    entities = list(calibration.get("entities") or [])
    if not entities:
        raise ConductPortfolioMixDiagnosticError("calibration contains no entities")

    credibility_by_id = {
        str(row.get("entity_id") or ""): row
        for row in credibility.get("entities") or []
    }
    by_id = {str(row.get("entity_id") or ""): row for row in entities}
    if set(credibility_by_id) != set(by_id):
        raise ConductPortfolioMixDiagnosticError(
            "calibration and credibility populations are not identical"
        )

    mix_by_id: dict[str, dict[str, float]] = {}
    for entity_id, entity in by_id.items():
        raw_mix = (
            (entity.get("portfolio_12m") or {}).get("positive_branch_mix") or {}
        )
        mix: dict[str, float] = {}
        for branch, raw_value in raw_mix.items():
            value = _finite(raw_value, field="positive_branch_mix")
            if value > 0:
                mix[str(branch)] = value
        total = sum(mix.values())
        if total <= 0:
            raise ConductPortfolioMixDiagnosticError(
                f"entity {entity_id} has no positive portfolio mix"
            )
        mix_by_id[entity_id] = {
            branch: value / total for branch, value in mix.items()
        }

    pair_distance: dict[tuple[str, str], float] = {}
    pair_rows: list[dict[str, Any]] = []
    ids = sorted(by_id)
    for index, left_id in enumerate(ids):
        left = by_id[left_id]
        left_expected = _finite(
            (left.get("pressure_12m") or {}).get("expected_complaints") or 0.0,
            field="expected_complaints",
        )
        left_log = _stabilized_log_pressure(
            int(left.get("complaints_12m") or 0), left_expected
        )
        for right_id in ids[index + 1 :]:
            right = by_id[right_id]
            right_expected = _finite(
                (right.get("pressure_12m") or {}).get("expected_complaints") or 0.0,
                field="expected_complaints",
            )
            right_log = _stabilized_log_pressure(
                int(right.get("complaints_12m") or 0), right_expected
            )
            distance = _tvd(mix_by_id[left_id], mix_by_id[right_id])
            pair_distance[(left_id, right_id)] = distance
            pair_distance[(right_id, left_id)] = distance
            pair_rows.append(
                {
                    "left_id": left_id,
                    "right_id": right_id,
                    "distance": distance,
                    "absolute_stabilized_log_pressure_difference": abs(
                        left_log - right_log
                    ),
                    "both_100_plus_complaints": (
                        int(left.get("complaints_12m") or 0) >= 100
                        and int(right.get("complaints_12m") or 0) >= 100
                    ),
                }
            )

    rows: list[dict[str, Any]] = []
    for entity_id in ids:
        entity = by_id[entity_id]
        credibility_row = credibility_by_id[entity_id]
        distances = sorted(
            (
                (other_id, pair_distance[(entity_id, other_id)])
                for other_id in ids
                if other_id != entity_id
            ),
            key=lambda item: (item[1], item[0]),
        )
        nearest = [
            {
                "entity_id": other_id,
                "legal_name": by_id[other_id].get("legal_name"),
                "distance": float(distance),
                "complaints_12m": int(by_id[other_id].get("complaints_12m") or 0),
                "global_pressure_ratio": (by_id[other_id].get("pressure_12m") or {}).get(
                    "ratio"
                ),
                "credibility_state": (
                    credibility_by_id[other_id]
                    .get("direct_candidate", {})
                    .get("familywise_exact_interval", {})
                    .get("state")
                ),
            }
            for other_id, distance in distances[:5]
        ]
        raw_global_ratio = (entity.get("pressure_12m") or {}).get("ratio")
        global_ratio = (
            _finite(raw_global_ratio, field="pressure_12m.ratio")
            if raw_global_ratio is not None
            else None
        )

        curve = []
        for threshold in DISTANCE_GRID:
            peer_ids = [
                other_id
                for other_id, distance in distances
                if distance <= threshold
            ]
            local = _local_pressure(
                entity, [by_id[peer_id] for peer_id in peer_ids]
            )
            local_ratio = local.get("ratio")
            curve.append(
                {
                    "max_total_variation_distance": threshold,
                    "peer_count": len(peer_ids),
                    "local_aligned_pressure": local,
                    "global_to_local_side_consistency": (
                        "same_side"
                        if _side(global_ratio) == _side(local_ratio)
                        else "changes_side"
                    )
                    if local_ratio is not None and global_ratio is not None
                    else "unavailable",
                    "local_to_global_ratio_multiplier": (
                        float(local_ratio / global_ratio)
                        if local_ratio is not None
                        and global_ratio is not None
                        and global_ratio > 0
                        else None
                    ),
                }
            )

        temporal_direct = (
            credibility_row.get("temporal_overlap", {}).get("premium_direct", {})
        )
        sensitivity = credibility_row.get("denominator_sensitivity", {})
        portfolio = entity.get("portfolio_12m") or {}
        rows.append(
            {
                "entity_id": entity_id,
                "fip_code": entity.get("fip_code"),
                "legal_name": entity.get("legal_name"),
                "display_name": entity.get("display_name"),
                "complaints_12m": int(entity.get("complaints_12m") or 0),
                "premium_direct_12m": _finite(
                    entity.get("premium_direct_12m") or 0.0,
                    field="premium_direct_12m",
                ),
                "global_pressure_ratio": global_ratio,
                "portfolio": {
                    "positive_branch_count": portfolio.get("positive_branch_count"),
                    "hhi": portfolio.get("hhi"),
                    "top_branch_share": portfolio.get("top_branch_share"),
                    "distance_from_market_mix": portfolio.get(
                        "distance_from_market_mix"
                    ),
                    "nearest_observations_not_approved_peers": nearest,
                    "nearest_distance": float(distances[0][1]) if distances else None,
                    "fifth_nearest_distance": (
                        float(distances[4][1]) if len(distances) >= 5 else None
                    ),
                },
                "peer_distance_curve": curve,
                "prior_guard_context": {
                    "statistical_credibility_state": (
                        credibility_row.get("direct_candidate", {})
                        .get("familywise_exact_interval", {})
                        .get("state")
                    ),
                    "complaints_in_non_positive_direct_premium_months": (
                        temporal_direct.get(
                            "complaints_in_non_positive_premium_months"
                        )
                    ),
                    "denominator_familywise_state_consistency": sensitivity.get(
                        "familywise_state_consistency"
                    ),
                },
                "human_questions": {
                    "peer_adequacy": (
                        "Ha empresas suficientemente parecidas para uma comparacao justa de carteira?"
                    ),
                    "mix_robustness": (
                        "O sinal continua quando olhamos apenas para negocios parecidos?"
                    ),
                },
            }
        )

    coverage_curve = []
    side_changes_by_distance: dict[str, int] = {}
    multiplier_quantiles_by_distance: dict[str, dict[str, float | None]] = {}
    for threshold in DISTANCE_GRID:
        peer_counts = []
        available_local = 0
        side_changes = 0
        multipliers: list[float] = []
        for row in rows:
            point = next(
                item
                for item in row["peer_distance_curve"]
                if item["max_total_variation_distance"] == threshold
            )
            peer_counts.append(int(point["peer_count"]))
            if point["local_aligned_pressure"].get("ratio") is not None:
                available_local += 1
            if point["global_to_local_side_consistency"] == "changes_side":
                side_changes += 1
            multiplier = point.get("local_to_global_ratio_multiplier")
            if multiplier is not None and math.isfinite(float(multiplier)):
                multipliers.append(float(multiplier))
        coverage_curve.append(
            {
                "max_total_variation_distance": threshold,
                "entities_with_at_least_1_peer": sum(count >= 1 for count in peer_counts),
                "entities_with_at_least_3_peers": sum(count >= 3 for count in peer_counts),
                "entities_with_at_least_5_peers": sum(count >= 5 for count in peer_counts),
                "median_peer_count": float(np.median(peer_counts)),
                "max_peer_count": max(peer_counts),
                "entities_with_local_aligned_pressure": available_local,
            }
        )
        key = f"{threshold:.2f}"
        side_changes_by_distance[key] = side_changes
        multiplier_quantiles_by_distance[key] = _quantiles(multipliers)

    all_pair_distance = [float(row["distance"]) for row in pair_rows]
    all_pair_pressure_difference = [
        float(row["absolute_stabilized_log_pressure_difference"]) for row in pair_rows
    ]
    high_volume_pairs = [row for row in pair_rows if row["both_100_plus_complaints"]]
    high_volume_distance = [float(row["distance"]) for row in high_volume_pairs]
    high_volume_pressure_difference = [
        float(row["absolute_stabilized_log_pressure_difference"])
        for row in high_volume_pairs
    ]
    nearest_distances = [
        float(row["portfolio"]["nearest_distance"])
        for row in rows
        if row["portfolio"]["nearest_distance"] is not None
    ]
    fifth_nearest_distances = [
        float(row["portfolio"]["fifth_nearest_distance"])
        for row in rows
        if row["portfolio"]["fifth_nearest_distance"] is not None
    ]

    return {
        "artifact": "v2_conduct_portfolio_mix_diagnostic",
        "generated_at": _utc_now(),
        "version": VERSION,
        "status": "experimental_diagnostic",
        "assessment_role": "portfolio_mix_comparability_guard_only",
        "scoring": "forbidden_in_this_artifact",
        "ranking": "forbidden_in_this_artifact",
        "human_model": {
            "primary_question": (
                "Estamos comparando esta seguradora com empresas que vendem coisas suficientemente parecidas?"
            ),
            "secondary_question": (
                "O sinal continua quando olhamos apenas para negocios parecidos?"
            ),
            "principle": (
                "tamanho economico semelhante nao garante risco, produto ou jornada de consumo semelhantes"
            ),
            "frontend_status": "not_approved_for_public_classification",
        },
        "source": {
            "calibration_artifact": str(CALIBRATION_PATH),
            "calibration_version": calibration.get("version"),
            "credibility_artifact": str(CREDIBILITY_PATH),
            "credibility_version": credibility.get("version"),
            "portfolio_field": "coramo_positive_branch_mix",
            "exposure": "insurance_premium_direct",
        },
        "methodology": {
            "portfolio_distance": "total_variation_distance",
            "distance_range": [0.0, 1.0],
            "distance_interpretation": "0_identical_mix_1_no_overlap",
            "distance_grid": list(DISTANCE_GRID),
            "distance_threshold_selected": False,
            "peer_groups_selected": False,
            "portfolio_adjustment_applied": False,
            "local_pressure_role": "diagnostic_sensitivity_curve_only",
            "local_population_alignment": (
                "entity_and_peers_complaints_and_direct_premium_same_entities_only"
            ),
            "pairwise_pressure_similarity": {
                "transform": "log((observed+0.5)/(expected+0.5))",
                "continuity_correction": CONTINUITY_CORRECTION,
                "role": "correlation_diagnostic_only_not_shrinkage_not_score",
                "p_values_reported": False,
                "reason": "pairwise_observations_are_not_independent",
            },
            "guardrails": [
                "no_peer_means_no_local_conclusion_not_neutrality",
                "nearest_entity_is_not_automatically_an_adequate_peer",
                "five_nearest_entities_are_not_automatically_a_cohort",
                "distance_grid_is_exploratory_not_policy",
                "local_ratio_does_not_replace_global_pressure",
                "prior_credibility_denominator_and_temporal_guards_remain_active",
                "portfolio_similarity_does_not_prove_same_customer_risk_or_service_journey",
            ],
        },
        "population": {
            "entities": len(rows),
            "pairwise_comparisons": len(pair_rows),
            "high_volume_100_plus_complaint_entities": sum(
                int(row.get("complaints_12m") or 0) >= 100 for row in entities
            ),
            "high_volume_pairwise_comparisons": len(high_volume_pairs),
        },
        "diagnostics": {
            "distance_distribution_all_pairs": _quantiles(all_pair_distance),
            "nearest_peer_distance_distribution": _quantiles(nearest_distances),
            "fifth_nearest_peer_distance_distribution": _quantiles(
                fifth_nearest_distances
            ),
            "peer_coverage_curve": coverage_curve,
            "portfolio_distance_vs_pressure_difference": {
                "all_pairs_spearman": _safe_spearman(
                    all_pair_distance, all_pair_pressure_difference
                ),
                "high_volume_100_plus_pairs_spearman": _safe_spearman(
                    high_volume_distance, high_volume_pressure_difference
                ),
                "interpretation_guard": (
                    "positive_association_would_support_mix_relevance_but_not_a_causal_or_adjustment_model"
                ),
            },
            "local_pressure_sensitivity": {
                "global_to_local_side_changes_by_distance": side_changes_by_distance,
                "local_to_global_ratio_multiplier_quantiles_by_distance": (
                    multiplier_quantiles_by_distance
                ),
            },
        },
        "entities": rows,
    }


def build_from_files(
    calibration_path: Path = CALIBRATION_PATH,
    credibility_path: Path = CREDIBILITY_PATH,
) -> dict[str, Any]:
    calibration = json.loads(calibration_path.read_text(encoding="utf-8"))
    credibility = json.loads(credibility_path.read_text(encoding="utf-8"))
    return build_portfolio_mix_diagnostic(calibration, credibility)


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
                "distance": {
                    "nearest": payload["diagnostics"][
                        "nearest_peer_distance_distribution"
                    ],
                    "fifth_nearest": payload["diagnostics"][
                        "fifth_nearest_peer_distance_distribution"
                    ],
                },
                "coverage": payload["diagnostics"]["peer_coverage_curve"],
                "mix_pressure_association": payload["diagnostics"][
                    "portfolio_distance_vs_pressure_difference"
                ],
                "local_pressure_sensitivity": payload["diagnostics"][
                    "local_pressure_sensitivity"
                ],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    print(f"written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
