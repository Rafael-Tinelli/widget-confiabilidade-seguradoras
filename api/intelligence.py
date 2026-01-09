# api/intelligence.py
from __future__ import annotations

import math
from typing import Any, Dict, List


# --------------------------------------------------------------------------------------
# Intelligence layer for insurers.
#
# Goals:
# - Resilient scoring (avoid the “everything stuck at 50” trap)
# - Contextual reputation scoring using market benchmarks
# - Batch-level dataset diagnostics (disable reputation if dataset is effectively empty)
# --------------------------------------------------------------------------------------


def _clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, x))


def _safe_div(a: float, b: float) -> float:
    if not b:
        return 0.0
    return a / b


def _extract_stats(reputation_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Robustly normalize reputation statistics.

    Internal standard:
      - complaintsCount
      - resolvedCount
      - respondedCount
      - finalizedCount
      - overallSatisfaction (0..10)

    Supports both:
      - nested `statistics` dict
      - flat dict
      - legacy keys (total_claims, resolved_claims, score_sum, satisfaction_count, etc.)
    """
    if not reputation_data or not isinstance(reputation_data, dict):
        return {}

    stats = reputation_data.get("statistics")
    src_stats = stats if isinstance(stats, dict) else {}
    src_root = reputation_data

    def _pick(*keys: str) -> Any:
        for k in keys:
            if k in src_stats and src_stats.get(k) is not None:
                return src_stats.get(k)
        for k in keys:
            if k in src_root and src_root.get(k) is not None:
                return src_root.get(k)
        return None

    complaints = int(_pick("complaintsCount", "total_claims", "complaints_count") or 0)
    resolved = int(_pick("resolvedCount", "resolved_claims", "resolved_count") or 0)
    responded = int(_pick("respondedCount", "responded_claims", "responded_count") or 0)
    finalized = int(_pick("finalizedCount", "finalized_claims", "finalized_count") or 0)

    overall = _pick("overallSatisfaction", "overall_satisfaction")

    if overall is None:
        score_sum = _pick("scoreSum", "score_sum", "satisfactionSum", "satisfaction_sum")
        sat_count = _pick("satisfactionCount", "satisfaction_count")
        overall = _safe_div(float(score_sum or 0.0), float(sat_count or 0.0))
    else:
        overall = float(overall or 0.0)

    return {
        "complaintsCount": complaints,
        "resolvedCount": resolved,
        "respondedCount": responded,
        "finalizedCount": finalized,
        "overallSatisfaction": float(overall or 0.0),
    }


def _get_reputation_blob(insurer_obj: Dict[str, Any]) -> Dict[str, Any]:
    """Backward-compatible accessor for reputation payload."""
    rep = insurer_obj.get("reputation")
    if isinstance(rep, dict) and rep:
        return rep
    comp = insurer_obj.get("components")
    if isinstance(comp, dict):
        rep2 = comp.get("reputation")
        if isinstance(rep2, dict) and rep2:
            return rep2
    return {}


def calculate_opin_score(stats: Dict[str, Any]) -> float:
    """
    Opinion score proxy derived from:
      - satisfaction (0..10)
      - resolution ratio (resolved/complaints)
      - response ratio (responded/complaints)
    """
    complaints = float(stats.get("complaintsCount") or 0.0)
    resolved = float(stats.get("resolvedCount") or 0.0)
    responded = float(stats.get("respondedCount") or 0.0)
    satisfaction = float(stats.get("overallSatisfaction") or 0.0)

    if complaints <= 0:
        # Without complaint volume, satisfaction is the only meaningful signal.
        return _clamp(50.0 + (satisfaction - 5.0) * 10.0, 0.0, 100.0)

    resolved_ratio = _safe_div(resolved, complaints)
    responded_ratio = _safe_div(responded, complaints)

    sat_score = _clamp(50.0 + (satisfaction - 5.0) * 10.0, 0.0, 100.0)
    res_score = _clamp(resolved_ratio * 100.0, 0.0, 100.0)
    rsp_score = _clamp(responded_ratio * 100.0, 0.0, 100.0)

    # Weighted blend
    return _clamp(sat_score * 0.45 + res_score * 0.35 + rsp_score * 0.20, 0.0, 100.0)


def calculate_reputation_score(reputation_data: Dict[str, Any]) -> float:
    stats = _extract_stats(reputation_data)
    if not stats:
        return 50.0

    return calculate_opin_score(stats)


def calculate_solvency_score(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Solvency proxy based on:
      - loss ratio (claims/premiums)
      - net worth relative to scale (net_worth/max(premiums, claims))
    """
    premiums = float(data.get("premiums") or 0.0)
    claims = float(data.get("claims") or 0.0)
    net_worth = float(
        data.get("net_worth")
        or data.get("netWorth")
        or 0.0
    )

    loss_ratio = _safe_div(claims, premiums) if premiums > 0 else 0.0
    scale = max(premiums, claims, 1.0)
    net_worth_ratio = _safe_div(net_worth, scale) if net_worth > 0 else 0.0

    # Loss ratio score: 0.6->90, 1.0->50, >1.0 decays.
    if premiums <= 0:
        loss_score = 50.0
    elif loss_ratio <= 0.6:
        loss_score = 90.0
    elif loss_ratio <= 1.0:
        loss_score = 90.0 - (loss_ratio - 0.6) * 100.0
    else:
        loss_score = 50.0 - min((loss_ratio - 1.0) * 50.0, 45.0)
    loss_score = _clamp(loss_score, 5.0, 98.0)

    # Net worth ratio score in log space.
    if net_worth_ratio <= 0:
        ratio_score = 50.0
    else:
        ratio_score = 50.0 + 20.0 * math.log10(max(net_worth_ratio, 1e-6))
    ratio_score = _clamp(ratio_score, 0.0, 100.0)

    solvency_score = _clamp(ratio_score * 0.7 + loss_score * 0.3, 0.0, 100.0)

    return {
        "score": float(solvency_score),
        "lossRatio": float(loss_ratio),
        "netWorthRatio": float(net_worth_ratio),
    }


def calculate_innovation_score(flags: Dict[str, Any], products: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Innovation proxy:
      - Open Insurance participant => +20
      - Product breadth => up to +20
      - Base 60
    """
    open_ins = bool(flags.get("openInsuranceParticipant"))
    products_count = len(products) if isinstance(products, list) else 0

    score = 60.0
    if open_ins:
        score += 20.0

    score += _clamp(products_count / 50.0, 0.0, 1.0) * 20.0

    return {"score": float(_clamp(score, 0.0, 100.0)), "productsCount": int(products_count)}


def calculate_complaint_pressure(
    complaints_count: int,
    premiums_brl: float,
    market_rate: float,
) -> tuple[float, float]:
    """
    Observed vs expected complaints (normalized by market average).

    Returns:
      observed_rate_per_brl, pressure_index
    Where:
      pressure_index ~= 1.0 means "market average",
      >1 means "worse than market", <1 means "better than market".
    """
    if premiums_brl <= 0:
        return 0.0, 1.0

    observed_rate = _safe_div(float(complaints_count), float(max(premiums_brl, 1.0)))

    market = max(float(market_rate or 0.0), 1e-12)

    # Smoothing for tiny insurers: soften pressure.
    scale = max(premiums_brl / 10_000_000.0, 0.2)
    pressure = (observed_rate / market) ** (1.0 / scale)

    return float(observed_rate), float(pressure)


def calculate_reputation_contextual(
    reputation_data: Dict[str, Any],
    premiums_brl: float,
    market_rate: float,
) -> Dict[str, Any] | None:
    stats = _extract_stats(reputation_data)
    if not stats or premiums_brl <= 0:
        return None

    complaints = int(stats.get("complaintsCount") or 0)
    satisfaction = float(stats.get("overallSatisfaction") or 0.0)

    observed_rate, pressure_idx = calculate_complaint_pressure(complaints, premiums_brl, market_rate)

    # Pressure -> score
    if pressure_idx <= 1.0:
        pressure_score = 80.0 + (1.0 - pressure_idx) * 20.0
    else:
        pressure_score = 80.0 - (math.log(pressure_idx + 1.0) / math.log(3.5)) * 50.0
    pressure_score = _clamp(pressure_score, 5.0, 98.0)

    sat = _clamp(satisfaction, 0.0, 10.0)
    sat_norm = (sat - 5.0) / 5.0
    satisfaction_score = _clamp(70.0 + sat_norm * 30.0, 10.0, 100.0)

    final_score = pressure_score
    if sat > 0:
        final_score = _clamp(pressure_score * 0.8 + satisfaction_score * 0.2, 0.0, 100.0)

    return {
        "score": float(final_score),
        "pressure_idx": float(pressure_idx),
        "observed_rate_per_brl": float(observed_rate),
        "market_rate_per_brl": float(market_rate),
        "overallSatisfaction": float(sat),
    }


def compute_market_benchmarks(insurers: List[Dict[str, Any]]) -> float:
    """
    Market benchmark: complaints per BRL premium.

    Returns a float to preserve your current pipeline contract.
    """
    total_complaints = 0
    total_premiums = 0.0

    for ins in insurers:
        rep = _get_reputation_blob(ins)
        if rep:
            stats = _extract_stats(rep)
            total_complaints += int(stats.get("complaintsCount") or 0)

        data = ins.get("data") if isinstance(ins.get("data"), dict) else {}
        total_premiums += float(data.get("premiums") or 0.0)

    if total_premiums <= 0:
        return 0.0
    return float(total_complaints / max(total_premiums, 1.0))


_CONTEXT: Dict[str, Any] = {
    "market_avg_complaints_per_premium": 0.0,
    "reputation_dataset_empty": False,
    "reputation_enabled": True,
}


def calculate_score(insurer_obj: Dict[str, Any]) -> Dict[str, Any]:
    data = insurer_obj.get("data") if isinstance(insurer_obj.get("data"), dict) else {}
    flags = insurer_obj.get("flags") if isinstance(insurer_obj.get("flags"), dict) else {}
    products = insurer_obj.get("products") if isinstance(insurer_obj.get("products"), list) else []

    segment = str(insurer_obj.get("segment") or "S4").strip().upper()
    if segment not in {"S1", "S2", "S3", "S4"}:
        segment = "S4"

    solvency = calculate_solvency_score(data)
    innovation = calculate_innovation_score(flags, products)

    premiums = float(data.get("premiums") or 0.0)

    reputation_raw = _get_reputation_blob(insurer_obj)
    reputation_score: float | None = None
    reputation_component: Dict[str, Any] = {"score": 50.0}

    if _CONTEXT.get("reputation_enabled", True) and reputation_raw:
        market_rate = float(_CONTEXT.get("market_avg_complaints_per_premium") or 0.0)
        rep_result = calculate_reputation_contextual(reputation_raw, premiums, float(market_rate))
        if rep_result is not None:
            reputation_score = float(rep_result.get("score") or 0.0)
            reputation_component = rep_result

                # Keep indexes inside the reputation blob (same behavior, but safe for fallback blob)
            if isinstance(reputation_raw, dict) and reputation_raw:
                reputation_raw.setdefault("indexes", {})
                reputation_raw["indexes"].update(
                    {
                        "pressure_idx": reputation_component.get("pressure_idx"),
                        "observed_rate_per_brl": reputation_component.get("observed_rate_per_brl"),
                        "market_rate_per_brl": reputation_component.get("market_rate_per_brl"),
                    }
                )

    # Weights by segment
    if not _CONTEXT.get("reputation_enabled", True):
        w_sol, w_rep, w_inn = 0.6, 0.0, 0.4
    else:
        if segment == "S1":
            w_sol, w_rep, w_inn = 0.45, 0.40, 0.15
        elif segment == "S2":
            w_sol, w_rep, w_inn = 0.40, 0.45, 0.15
        elif segment == "S3":
            w_sol, w_rep, w_inn = 0.35, 0.50, 0.15
        else:
            w_sol, w_rep, w_inn = 0.35, 0.45, 0.20

    composite = (
        float(solvency.get("score") or 0.0) * w_sol
        + float(reputation_score or 0.0) * w_rep
        + float(innovation.get("score") or 0.0) * w_inn
    )
    composite = _clamp(composite, 0.0, 100.0)

    insurer_obj.setdefault("data", {})
    insurer_obj["data"]["score"] = float(composite)
    insurer_obj["data"]["lossRatio"] = float(solvency.get("lossRatio") or 0.0)
    insurer_obj["data"]["components"] = {
        "solvency": solvency,
        "reputation": reputation_component,
        "innovation": innovation,
    }
    insurer_obj["data"]["weights"] = {"solvency": w_sol, "reputation": w_rep, "innovation": w_inn}
    insurer_obj["data"]["segment"] = segment
    insurer_obj["data"]["context"] = {
        "reputationEnabled": bool(_CONTEXT.get("reputation_enabled", True)),
        "reputationDatasetEmpty": bool(_CONTEXT.get("reputation_dataset_empty", False)),
    }

    return insurer_obj


def apply_intelligence_batch(insurers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    market_rate = compute_market_benchmarks(insurers)
    _CONTEXT["market_avg_complaints_per_premium"] = float(market_rate)

    matched = 0
    complaints_sum = 0

    for ins in insurers:
        rep = _get_reputation_blob(ins)
        if isinstance(rep, dict) and rep:
            matched += 1
            stats = _extract_stats(rep)
            complaints_sum += int(stats.get("complaintsCount") or 0)

    reputation_dataset_empty = bool(matched and complaints_sum == 0)
    _CONTEXT["reputation_dataset_empty"] = reputation_dataset_empty
    _CONTEXT["reputation_enabled"] = not reputation_dataset_empty

    if reputation_dataset_empty:
        print(
            "DEBUG: reputation_dataset_empty=True "
            f"(matched={matched}, complaints=0). Disabling reputation in scoring."
        )

    return [calculate_score(ins) for ins in insurers]
