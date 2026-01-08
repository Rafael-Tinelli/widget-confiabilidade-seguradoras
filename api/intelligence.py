# api/intelligence.py
from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Tuple

# --------------------------------------------------------------------------------------
# Intelligence Engine
# --------------------------------------------------------------------------------------
#
# Goal:
# - Produce stable, explainable scores even when some inputs are missing.
# - Be robust to upstream data hiccups (e.g., reputation dataset temporarily empty).
#
# Output contract (mutations on each insurer object):
# - insurer["segment"]
# - insurer["data"]["components"] = {"solvency": float, "reputation": float|None, "innovation": float}
# - insurer["data"]["score"] = float
# - insurer["data"]["lossRatio"] = float
# - if reputation exists: insurer["reputation"]["indexes"] may be enriched
#
# NOTE: Keep changes minimal to avoid regressions in the UI/pipeline.

WEIGHT_PRESSURE = 0.40
WEIGHT_EFFICIENCY = 0.40
WEIGHT_SATISFACTION = 0.20

ALPHA = 5.0

# For expected complaints scaling:
MIN_PREMIUM_FOR_EXPECTED = 100_000.0

# For benchmark market_rate computation:
# Lowering this improves benchmark stability when few companies have reputation matched.
MIN_PREMIUM_FOR_BENCHMARK = 10_000.0

# If we matched "enough" insurers to reputation but total complaints is still zero,
# it is overwhelmingly likely the upstream dataset is empty/incorrect (as per recent logs).
MIN_MATCHED_FOR_SIGNAL_CHECK = 20

_CONTEXT: Dict[str, Any] = {
    "market_rate": None,
    "bench_total_complaints": 0,
    "bench_total_premiums": 0.0,
    "bench_matched": 0,
    "reputation_dataset_empty": False,
}


def _clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, x))


def _safe_div(n: float, d: float, default: float = 0.0) -> float:
    return n / d if d > 0 else default


def _extract_stats(reputation_data: dict) -> dict:
    """
    Supports both:
      - our aggregated format (total_claims, resolved_claims, ...), and
      - a nested { "statistics": { ... } } format.
    """
    if not reputation_data:
        return {}

    if "statistics" in reputation_data and isinstance(reputation_data["statistics"], dict):
        return reputation_data["statistics"]

    # Our normalized aggregated format (from build_consumidor_gov)
    return {
        "complaintsCount": int(reputation_data.get("total_claims") or 0),
        "resolvedCount": int(reputation_data.get("resolved_claims") or 0),
        "respondedCount": int(reputation_data.get("responded_claims") or 0),
        "finalizedCount": int(reputation_data.get("finalized_claims") or 0),
        "overallSatisfaction": _safe_div(
            float(reputation_data.get("score_sum") or 0.0),
            int(reputation_data.get("satisfaction_count") or 0),
        ),
    }


# --------------------------------------------------------------------------------------
# Solvency (financial) score
# --------------------------------------------------------------------------------------
def calculate_solvency_score(data: dict) -> float:
    """
    Combines a (log) net-worth score with a loss-ratio distance-to-ideal score.
    """
    premiums = float(data.get("premiums", 0.0) or 0.0)
    claims = float(data.get("claims", 0.0) or 0.0)
    net_worth = float(data.get("net_worth", 0.0) or data.get("netWorth", 0.0) or 0.0)

    # Net worth score (log scale)
    if net_worth <= 100_000:
        net_worth_score = 0.0
    else:
        log_nw = math.log10(net_worth)
        net_worth_score = _clamp((log_nw - 5) * 20)  # 10^5 -> 0, 10^10 -> 100

    # Loss ratio score (ideal ~0.60)
    lr = claims / premiums if premiums > 0 else 0.0
    if lr <= 0:
        lr_score = 50.0
    else:
        dist = abs(lr - 0.60)
        lr_score = _clamp(100 - (dist * 200))

    final_score = (net_worth_score * 0.6) + (lr_score * 0.4)
    return round(final_score, 1)


# --------------------------------------------------------------------------------------
# Reputation scores
# --------------------------------------------------------------------------------------
def calculate_reputation_score(reputation_data: dict) -> float:
    """
    Legacy fallback reputation score (absolute, not contextual to market).
    Returns neutral 50 if data is missing or has zero complaints.
    """
    stats = _extract_stats(reputation_data)
    if not stats:
        return 50.0

    total = int(stats.get("complaintsCount") or 0)
    if total <= 0:
        return 50.0

    answered = int(stats.get("respondedCount") or 0)
    solved = int(stats.get("resolvedCount") or 0)
    satisfaction = float(stats.get("overallSatisfaction") or 0.0)

    answered_rate = _safe_div(answered, total)
    solved_rate = _safe_div(solved, total)

    # Satisfaction is typically 1..5 -> map to 0..100
    sat_norm = _clamp((satisfaction - 1) * 25) if satisfaction > 0 else 0.0

    # Efficiency focuses on response and resolution.
    efficiency = (answered_rate * 40) + (solved_rate * 40)

    # Mild penalty for very large volumes (avoid overpowering)
    volume_penalty = min(10.0, math.log10(total + 1) * 2)

    final = efficiency + (sat_norm * 0.20) - volume_penalty
    return round(_clamp(final), 1)


def calculate_complaint_pressure(observed: int, premiums: float, market_rate: float) -> Tuple[float, float]:
    """
    Compute:
      - pressure index: observed vs expected (Bayesian smoothing via ALPHA)
      - pressure_score: maps pressure index to 0..100
    """
    safe_premiums = max(float(premiums or 0.0), MIN_PREMIUM_FOR_EXPECTED)

    # Protect against zero/None market_rate (should not happen if contextual path is used correctly)
    safe_market_rate = max(float(market_rate or 0.0), 1e-9)

    expected = safe_premiums * safe_market_rate

    pressure = (float(observed) + ALPHA) / (float(expected) + ALPHA)

    # Pressure=1 => ~75; higher pressure decreases score; lower pressure increases score.
    score = 100.0 / (1.0 + (0.33 * pressure))

    return _clamp(score), max(0.0, pressure)


def calculate_reputation_contextual(
    reputation_data: dict, premiums: float, market_rate: float
) -> Optional[Dict[str, Any]]:
    """
    Contextual reputation score (market-normalized complaint pressure).
    Returns None if stats are unavailable.
    """
    stats = _extract_stats(reputation_data)
    if not stats:
        return None

    complaints_total = int(stats.get("complaintsCount") or 0)
    resolved_count = int(stats.get("resolvedCount") or 0)
    responded_count = int(stats.get("respondedCount") or 0)
    denom_resolution = int(stats.get("finalizedCount") or complaints_total or 0)
    satisfaction = float(stats.get("overallSatisfaction") or 0.0)

    pressure_score, pressure_idx = calculate_complaint_pressure(
        complaints_total, float(premiums or 0.0), float(market_rate or 0.0)
    )

    # If no complaints, efficiency is neutral.
    if complaints_total <= 0:
        efficiency_score = 50.0
    else:
        rate_resp = _safe_div(responded_count, complaints_total)
        rate_sol = _safe_div(resolved_count, denom_resolution)
        efficiency_score = _clamp((rate_resp * 40) + (rate_sol * 60))

    sat_norm = _clamp((satisfaction - 1) * 25) if satisfaction > 0 else 0.0

    if satisfaction > 0:
        final_score = (
            (pressure_score * WEIGHT_PRESSURE)
            + (efficiency_score * WEIGHT_EFFICIENCY)
            + (sat_norm * WEIGHT_SATISFACTION)
        )
    else:
        # If satisfaction is missing, downweight it (do not punish)
        final_score = (pressure_score * 0.90) + (efficiency_score * 0.10)

    return {
        "score": round(_clamp(final_score), 1),
        "pressure_index": round(pressure_idx, 2),
        "metrics": {
            "pressure_score": round(pressure_score, 1),
            "efficiency_score": round(efficiency_score, 1),
            "satisfaction_score": round(sat_norm, 1),
        },
    }


# --------------------------------------------------------------------------------------
# Innovation / Open Insurance score (simple heuristic)
# --------------------------------------------------------------------------------------
def calculate_opin_score(products: list, is_participant: bool) -> float:
    score = 0.0
    if is_participant:
        score += 50.0
    if products:
        score += min(50.0, len(products) * 2)
    return round(_clamp(score), 1)


def determine_segment(data: dict) -> str:
    premiums = float(data.get("premiums", 0.0) or 0.0)
    if premiums > 1_000_000_000:
        return "S1"
    if premiums > 100_000_000:
        return "S2"
    if premiums > 0:
        return "S3"
    return "S4"


# --------------------------------------------------------------------------------------
# Batch context / benchmark
# --------------------------------------------------------------------------------------
def compute_market_benchmarks(insurers: List[dict]) -> float:
    total_complaints = 0
    total_premiums = 0.0
    matched = 0

    for ins in insurers:
        data = (ins or {}).get("data", {}) or {}
        rep = (ins or {}).get("reputation")
        prem = float(data.get("premiums", 0.0) or 0.0)

        if not rep or prem < MIN_PREMIUM_FOR_BENCHMARK:
            continue

        stats = _extract_stats(rep)
        comp = int(stats.get("complaintsCount") or 0)

        matched += 1
        total_complaints += max(0, comp)
        total_premiums += prem

    # Persist benchmark diagnostics in context (useful for CI debugging)
    _CONTEXT["bench_total_complaints"] = int(total_complaints)
    _CONTEXT["bench_total_premiums"] = float(total_premiums)
    _CONTEXT["bench_matched"] = int(matched)

    # Detect "empty dataset" signature (many matches, but zero complaints everywhere)
    _CONTEXT["reputation_dataset_empty"] = bool(
        matched >= MIN_MATCHED_FOR_SIGNAL_CHECK and total_complaints == 0
    )

    if total_premiums <= 0:
        return 0.0

    return total_complaints / total_premiums


def apply_intelligence_batch(insurers: List[dict]) -> List[dict]:
    market_rate = compute_market_benchmarks(insurers)

    # If dataset looks empty, we should not use reputation in scoring at all,
    # otherwise we risk producing misleadingly high/neutral scores.
    if _CONTEXT.get("reputation_dataset_empty"):
        _CONTEXT["market_rate"] = None
        # Keep a CI-visible breadcrumb without breaking logs in local runs.
        print(
            "DEBUG: reputation_dataset_empty=True "
            f"(matched={_CONTEXT.get('bench_matched')}, "
            f"complaints={_CONTEXT.get('bench_total_complaints')}). "
            "Disabling reputation in scoring."
        )
    else:
        _CONTEXT["market_rate"] = market_rate if market_rate > 0 else None
        print(
            "DEBUG: market_rate="
            f"{market_rate} (matched={_CONTEXT.get('bench_matched')}, "
            f"complaints={_CONTEXT.get('bench_total_complaints')}, "
            f"premiums={_CONTEXT.get('bench_total_premiums')})"
        )

    for ins in insurers:
        calculate_score(ins)

    return insurers


# --------------------------------------------------------------------------------------
# Per-insurer scoring
# --------------------------------------------------------------------------------------
def calculate_score(insurer_obj: dict) -> dict:
    data = insurer_obj.get("data", {}) or {}
    products = insurer_obj.get("products", []) or []
    flags = insurer_obj.get("flags", {}) or {}
    premiums = float(data.get("premiums", 0.0) or 0.0)

    solvency_score = calculate_solvency_score(data)
    opin_score = calculate_opin_score(products, bool(flags.get("openInsuranceParticipant")))

    reputation_raw = insurer_obj.get("reputation")
    reputation_score: Optional[float] = None

    # If the reputation dataset seems empty, treat as unavailable.
    if reputation_raw and _CONTEXT.get("reputation_dataset_empty"):
        # Preserve existing structure but add a diagnostic hint.
        try:
            insurer_obj["reputation"].setdefault("indexes", {})
            insurer_obj["reputation"]["indexes"]["data_quality"] = "empty_dataset"
        except Exception:
            pass
        reputation_score = None
    elif reputation_raw:
        market_rate = _CONTEXT.get("market_rate")

        # Contextual path (only if benchmark exists)
        if isinstance(market_rate, (int, float)) and float(market_rate) > 0:
            rep_result = calculate_reputation_contextual(reputation_raw, premiums, float(market_rate))
            if rep_result:
                reputation_score = float(rep_result["score"])
                insurer_obj["reputation"].setdefault("indexes", {})
                insurer_obj["reputation"]["indexes"].update(
                    {
                        "complaint_pressure": rep_result["pressure_index"],
                        "components": rep_result["metrics"],
                    }
                )

        # If contextual was not possible or returned None, fallback to absolute
        if reputation_score is None:
            reputation_score = calculate_reputation_score(reputation_raw)

    # Final score weighting
    if reputation_score is not None:
        final_score = (solvency_score * 0.50) + (reputation_score * 0.40) + (opin_score * 0.10)
    else:
        final_score = (solvency_score * 0.80) + (opin_score * 0.20)

    # Persist
    insurer_obj["segment"] = determine_segment(data)
    insurer_obj.setdefault("data", {})
    insurer_obj["data"].setdefault("components", {})
    insurer_obj["data"]["components"] = {
        "solvency": round(float(solvency_score), 1),
        "reputation": round(float(reputation_score), 1) if reputation_score is not None else None,
        "innovation": round(float(opin_score), 1),
    }
    insurer_obj["data"]["score"] = round(float(final_score), 1)

    # Loss ratio
    if premiums > 0:
        insurer_obj["data"]["lossRatio"] = float(data.get("claims", 0.0) or 0.0) / premiums
    else:
        insurer_obj["data"]["lossRatio"] = 0.0

    return insurer_obj
