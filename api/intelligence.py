"""Scoring and data-availability layer for the insurers ranking."""

from __future__ import annotations

import math
from typing import Any


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def _safe_div(numerator: float, denominator: float) -> float:
    if not denominator:
        return 0.0
    return numerator / denominator


def _extract_stats(reputation_data: dict[str, Any]) -> dict[str, Any]:
    """Normalize current and legacy Consumidor.gov statistics."""
    if not isinstance(reputation_data, dict) or not reputation_data:
        return {}

    nested = reputation_data.get("statistics")
    source_stats = nested if isinstance(nested, dict) else {}

    def pick(*keys: str) -> Any:
        for key in keys:
            if source_stats.get(key) is not None:
                return source_stats[key]
        for key in keys:
            if reputation_data.get(key) is not None:
                return reputation_data[key]
        return None

    complaints = int(
        pick("complaintsCount", "total_claims", "complaints_count") or 0
    )
    resolved = int(pick("resolvedCount", "resolved_claims", "resolved_count") or 0)
    responded = int(
        pick("respondedCount", "responded_claims", "responded_count") or 0
    )
    finalized = int(
        pick("finalizedCount", "finalized_claims", "finalized_count") or 0
    )
    satisfaction_count = float(
        pick("satisfactionCount", "satisfaction_count") or 0.0
    )
    score_sum = float(
        pick("scoreSum", "score_sum", "satisfactionSum", "satisfaction_sum")
        or 0.0
    )

    overall = pick(
        "overallSatisfaction",
        "averageScore",
        "average_score",
        "overall_satisfaction",
    )
    if overall is None:
        overall = _safe_div(score_sum, satisfaction_count)
    overall = float(overall or 0.0)

    # Consumidor.gov usually publishes satisfaction on a 0..5 scale.
    if satisfaction_count > 0 and 0.0 < overall <= 5.0:
        overall *= 2.0

    return {
        "complaintsCount": complaints,
        "resolvedCount": resolved,
        "respondedCount": responded,
        "finalizedCount": finalized,
        "satisfactionCount": int(satisfaction_count),
        "overallSatisfaction": _clamp(overall, 0.0, 10.0),
    }


def _get_reputation_blob(insurer: dict[str, Any]) -> dict[str, Any]:
    reputation = insurer.get("reputation")
    if isinstance(reputation, dict) and reputation:
        return reputation

    components = insurer.get("components")
    if isinstance(components, dict):
        component_reputation = components.get("reputation")
        if isinstance(component_reputation, dict) and component_reputation:
            return component_reputation
    return {}


def calculate_opin_score(stats: dict[str, Any]) -> float:
    """Legacy reputation blend retained for backward compatibility."""
    complaints = float(stats.get("complaintsCount") or 0.0)
    resolved = float(stats.get("resolvedCount") or 0.0)
    responded = float(stats.get("respondedCount") or 0.0)
    satisfaction = float(stats.get("overallSatisfaction") or 0.0)

    if complaints <= 0:
        return _clamp(50.0 + (satisfaction - 5.0) * 10.0)

    satisfaction_score = _clamp(50.0 + (satisfaction - 5.0) * 10.0)
    resolved_score = _clamp(_safe_div(resolved, complaints) * 100.0)
    responded_score = _clamp(_safe_div(responded, complaints) * 100.0)
    return _clamp(
        satisfaction_score * 0.45
        + resolved_score * 0.35
        + responded_score * 0.20
    )


def calculate_reputation_score(reputation_data: dict[str, Any]) -> float:
    stats = _extract_stats(reputation_data)
    if not stats:
        return 50.0
    return calculate_opin_score(stats)


def calculate_solvency_score(data: dict[str, Any]) -> dict[str, Any]:
    premiums = float(data.get("premiums") or 0.0)
    claims = float(data.get("claims") or 0.0)
    net_worth = float(data.get("net_worth") or data.get("netWorth") or 0.0)

    loss_ratio_raw = claims / premiums if premiums > 0 else None
    loss_ratio: float | None = None
    loss_ratio_status = "ok"

    if premiums <= 0:
        loss_ratio_status = "insufficient_premiums"
        loss_score = 50.0
    elif claims < 0:
        loss_ratio_status = "invalid_claims"
        loss_score = 50.0
    else:
        loss_ratio = claims / premiums
        if loss_ratio <= 0.6:
            loss_score = 90.0
        elif loss_ratio <= 1.0:
            loss_score = 90.0 - (loss_ratio - 0.6) * 100.0
        else:
            loss_score = 50.0 - min((loss_ratio - 1.0) * 50.0, 45.0)

    scale = max(premiums, claims, 1.0)
    net_worth_ratio = _safe_div(net_worth, scale) if net_worth > 0 else 0.0
    loss_score = _clamp(loss_score, 5.0, 98.0)

    if net_worth_ratio <= 0:
        ratio_score = 50.0
    else:
        ratio_score = 50.0 + 20.0 * math.log10(max(net_worth_ratio, 1e-6))
    ratio_score = _clamp(ratio_score)

    solvency_score = _clamp(ratio_score * 0.7 + loss_score * 0.3)
    if premiums > 0:
        scale_confidence = 1.0 - math.exp(-premiums / 200_000_000.0)
        solvency_score = _clamp(
            65.0 * (1.0 - scale_confidence)
            + solvency_score * scale_confidence
        )

    return {
        "score": float(solvency_score),
        "lossRatio": loss_ratio if loss_ratio is None else float(loss_ratio),
        "lossRatioRaw": (
            loss_ratio_raw if loss_ratio_raw is None else float(loss_ratio_raw)
        ),
        "lossRatioStatus": loss_ratio_status,
        "netWorthRatio": float(net_worth_ratio),
    }


def calculate_innovation_score(
    flags: dict[str, Any],
    products: list[dict[str, Any]],
) -> dict[str, Any]:
    """Score the Open Insurance pillar from participation and product breadth."""
    is_participant = bool(flags.get("openInsuranceParticipant"))
    product_count = len(products) if isinstance(products, list) else 0

    score = 60.0
    if is_participant:
        score += 20.0
    score += _clamp(product_count / 50.0, 0.0, 1.0) * 20.0

    return {
        "score": float(_clamp(score)),
        "productsCount": int(product_count),
    }


def calculate_complaint_pressure(
    complaints_count: int,
    premiums_brl: float,
    market_rate: float,
) -> tuple[float, float]:
    if premiums_brl <= 0:
        return 0.0, 1.0

    observed_rate = _safe_div(
        float(complaints_count),
        float(max(premiums_brl, 1.0)),
    )
    safe_market_rate = max(float(market_rate or 0.0), 1e-12)
    scale = max(premiums_brl / 10_000_000.0, 0.2)
    pressure = (observed_rate / safe_market_rate) ** (1.0 / scale)
    return float(observed_rate), float(pressure)


def calculate_reputation_contextual(
    reputation_data: dict[str, Any],
    premiums_brl: float,
    market_rate: float,
) -> dict[str, Any] | None:
    stats = _extract_stats(reputation_data)
    if not stats or premiums_brl <= 0:
        return None

    complaints = int(stats.get("complaintsCount") or 0)
    resolved = int(stats.get("resolvedCount") or 0)
    responded = int(stats.get("respondedCount") or 0)
    satisfaction = float(stats.get("overallSatisfaction") or 0.0)
    satisfaction_count = int(stats.get("satisfactionCount") or 0)

    if complaints <= 0:
        observed_rate, pressure_index = 0.0, 1.0
    else:
        observed_rate, pressure_index = calculate_complaint_pressure(
            complaints,
            premiums_brl,
            market_rate,
        )

    if pressure_index <= 1.0:
        pressure_score = 80.0 + (1.0 - pressure_index) * 20.0
    else:
        pressure_score = 80.0 - (
            math.log(pressure_index + 1.0) / math.log(3.5)
        ) * 50.0
    pressure_score = _clamp(pressure_score, 5.0, 98.0)

    satisfaction_normalized = (_clamp(satisfaction, 0.0, 10.0) - 5.0) / 5.0
    satisfaction_score = _clamp(
        70.0 + satisfaction_normalized * 30.0,
        10.0,
        100.0,
    )

    if complaints > 0:
        resolved_score = _clamp(_safe_div(resolved, complaints) * 100.0)
        responded_score = _clamp(_safe_div(responded, complaints) * 100.0)
    else:
        resolved_score = 0.0
        responded_score = 0.0

    final_score = _clamp(
        pressure_score * 0.70
        + satisfaction_score * 0.15
        + resolved_score * 0.10
        + responded_score * 0.05
    )

    complaint_confidence = 1.0 - math.exp(-float(max(complaints, 0)) / 30.0)
    satisfaction_confidence = 1.0 - math.exp(
        -float(max(satisfaction_count, 0)) / 25.0
    )
    confidence = _clamp(
        max(complaint_confidence, satisfaction_confidence),
        0.0,
        1.0,
    )
    final_score = _clamp(80.0 * (1.0 - confidence) + final_score * confidence)

    return {
        "score": float(final_score),
        "pressure_idx": float(pressure_index),
        "observed_rate_per_brl": float(observed_rate),
        "market_rate_per_brl": float(market_rate),
        "overallSatisfaction": float(_clamp(satisfaction, 0.0, 10.0)),
    }


def compute_market_benchmarks(insurers: list[dict[str, Any]]) -> float:
    total_complaints = 0
    total_premiums = 0.0

    for insurer in insurers:
        reputation = _get_reputation_blob(insurer)
        if not reputation:
            continue
        complaints = int(_extract_stats(reputation).get("complaintsCount") or 0)
        data = insurer.get("data") if isinstance(insurer.get("data"), dict) else {}
        premiums = float(data.get("premiums") or 0.0)
        if complaints <= 0 or premiums <= 0:
            continue
        total_complaints += complaints
        total_premiums += premiums

    if total_premiums <= 0:
        return 0.0
    return float(total_complaints / max(total_premiums, 1.0))


_CONTEXT: dict[str, Any] = {
    "market_avg_complaints_per_premium": 0.0,
    "reputation_dataset_empty": False,
    "reputation_enabled": True,
}


def _reputation_reason(
    *,
    matched: bool,
    dataset_enabled: bool,
    premiums: float,
    applied: bool,
) -> str:
    if applied:
        return "applied"
    if not matched:
        return "missing_reputation"
    if not dataset_enabled:
        return "dataset_disabled"
    if premiums <= 0:
        return "insufficient_premiums"
    return "not_applied"


def calculate_score(insurer: dict[str, Any]) -> dict[str, Any]:
    data = insurer.get("data") if isinstance(insurer.get("data"), dict) else {}
    flags = insurer.get("flags") if isinstance(insurer.get("flags"), dict) else {}
    products = insurer.get("products") if isinstance(insurer.get("products"), list) else []

    solvency = calculate_solvency_score(data)
    innovation = calculate_innovation_score(flags, products)
    premiums = float(data.get("premiums") or 0.0)

    reputation_raw = _get_reputation_blob(insurer)
    reputation_matched = bool(reputation_raw)
    dataset_enabled = bool(_CONTEXT.get("reputation_enabled", True))
    reputation_result: dict[str, Any] | None = None

    if dataset_enabled and reputation_matched:
        market_rate = float(
            _CONTEXT.get("market_avg_complaints_per_premium") or 0.0
        )
        reputation_result = calculate_reputation_contextual(
            reputation_raw,
            premiums,
            market_rate,
        )

    reputation_applied = reputation_result is not None
    reputation_reason = _reputation_reason(
        matched=reputation_matched,
        dataset_enabled=dataset_enabled,
        premiums=premiums,
        applied=reputation_applied,
    )

    reputation_component: dict[str, Any] = {
        "score": 50.0,
        "matched": reputation_matched,
        "applied": reputation_applied,
        "reason": reputation_reason,
    }
    if reputation_result is not None:
        reputation_component.update(reputation_result)
        indexes = reputation_raw.get("indexes")
        if not isinstance(indexes, dict):
            indexes = {}
            reputation_raw["indexes"] = indexes
        indexes.update(
            {
                "pressure_idx": reputation_result.get("pressure_idx"),
                "observed_rate_per_brl": reputation_result.get(
                    "observed_rate_per_brl"
                ),
                "market_rate_per_brl": reputation_result.get(
                    "market_rate_per_brl"
                ),
            }
        )

    if dataset_enabled:
        solvency_weight, reputation_weight, innovation_weight = 0.40, 0.45, 0.15
    else:
        solvency_weight, reputation_weight, innovation_weight = 0.60, 0.0, 0.40

    solvency_score = float(solvency.get("score") or 0.0)
    displayed_reputation_score = float(reputation_component.get("score") or 0.0)
    applied_reputation_score = (
        displayed_reputation_score if reputation_applied else 0.0
    )
    innovation_score = float(innovation.get("score") or 0.0)

    contributions = {
        "solvency": solvency_score * solvency_weight,
        "reputation": applied_reputation_score * reputation_weight,
        "innovation": innovation_score * innovation_weight,
    }
    composite = _clamp(sum(contributions.values()))
    contributions["total"] = composite

    output_data = insurer.get("data")
    if not isinstance(output_data, dict):
        output_data = {}
        insurer["data"] = output_data
    output_data["score"] = float(composite)
    output_data["lossRatio"] = float(solvency.get("lossRatio") or 0.0)
    output_data["components"] = {
        "solvency": solvency_score,
        "reputation": displayed_reputation_score,
        "innovation": innovation_score,
        "financial": solvency_score,
    }
    output_data["componentsDetail"] = {
        "solvency": solvency,
        "reputation": reputation_component,
        "innovation": innovation,
    }
    output_data["solvencyScore"] = solvency_score
    output_data["reputationScore"] = displayed_reputation_score
    output_data["innovationScore"] = innovation_score
    output_data["financialScore"] = solvency_score
    output_data["weights"] = {
        "solvency": solvency_weight,
        "reputation": reputation_weight,
        "innovation": innovation_weight,
    }
    output_data["contributions"] = contributions
    output_data["availability"] = {
        "reputationMatched": reputation_matched,
        "reputationApplied": reputation_applied,
        "reputationReason": reputation_reason,
        "reputationDatasetEnabled": dataset_enabled,
        "openInsuranceParticipant": bool(
            flags.get("openInsuranceParticipant")
        ),
    }
    output_data["context"] = {
        "reputationEnabled": dataset_enabled,
        "reputationDatasetEmpty": bool(
            _CONTEXT.get("reputation_dataset_empty", False)
        ),
        "reputationMatched": reputation_matched,
        "reputationApplied": reputation_applied,
    }

    components = insurer.get("components")
    if not isinstance(components, dict):
        components = {}
        insurer["components"] = components
    financials = components.get("financials")
    if isinstance(financials, dict):
        financials.setdefault("score", solvency_score)
        financials.setdefault("lossRatio", output_data["lossRatio"])

    return insurer


def apply_intelligence_batch(
    insurers: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    _CONTEXT["market_avg_complaints_per_premium"] = compute_market_benchmarks(
        insurers
    )

    matched = 0
    complaint_sum = 0
    for insurer in insurers:
        reputation = _get_reputation_blob(insurer)
        if not reputation:
            continue
        matched += 1
        complaint_sum += int(
            _extract_stats(reputation).get("complaintsCount") or 0
        )

    dataset_empty = bool(matched and complaint_sum == 0)
    _CONTEXT["reputation_dataset_empty"] = dataset_empty
    _CONTEXT["reputation_enabled"] = not dataset_empty

    if dataset_empty:
        print(
            "DEBUG: reputation_dataset_empty=True "
            f"(matched={matched}, complaints=0). Disabling reputation in scoring."
        )

    return [calculate_score(insurer) for insurer in insurers]
