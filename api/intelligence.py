# api/intelligence.py
from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Tuple

# --------------------------------------------------------------------------------------
# Intelligence Engine (v3.1)
#
# Goals:
# - Preserve existing public interface: calculate_score(insurer_obj) -> insurer_obj (in-place update)
# - Preserve aggregation logic (final score composition):
#     - With reputation: 50% Solvency + 40% Reputation + 10% Innovation
#     - Without reputation: 80% Solvency + 20% Innovation
# - Add batch processing (apply_intelligence_batch) to enable contextual "Observed vs Expected"
#   complaint pressure scoring for reputation.
# - Keep a legacy reputation scorer as a fallback when contextual benchmarks are unavailable.
# --------------------------------------------------------------------------------------

# --- Calibration constants (Observed vs Expected reputation) ---
ALPHA = 5.0  # Laplacian-style smoothing in "complaints count" space

# [CORREÇÃO] Ajuste de pesos: Equilibra Volume (40%) com Qualidade (60%)
WEIGHT_PRESSURE = 0.40
WEIGHT_EFFICIENCY = 0.40
WEIGHT_SATISFACTION = 0.20

# Minimum premium used for expected complaints estimate (avoid division by zero / micro denominators)
MIN_PREMIUM_FOR_EXPECTED = 100_000.0

# Premium threshold for market benchmark calculation (exclude micro/noise)
MIN_PREMIUM_FOR_BENCHMARK = 100_000.0

# Module-global context set by apply_intelligence_batch (keeps calculate_score signature unchanged)
_CONTEXT: Dict[str, Any] = {"market_rate": None}


def _clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, x))


def _safe_div(n: float, d: float, default: float = 0.0) -> float:
    return n / d if d > 0 else default


def _extract_stats(reputation_data: dict) -> dict:
    """
    [CORREÇÃO CRÍTICA] Normaliza dados de reputação.
    Aceita tanto o formato aninhado 'statistics' (v2) quanto o formato plano (v3/dump).
    Isso impede que a nota zere se a estrutura mudar.
    """
    if not reputation_data:
        return {}

    # Tenta formato v2 (aninhado - legacy)
    if "statistics" in reputation_data:
        return reputation_data["statistics"]

    # Tenta formato v3 (plano / dump do consumidor.gov)
    # Mapeia chaves do dump para o padrão interno esperado pelas fórmulas
    return {
        "complaintsCount": reputation_data.get("total_claims", 0),
        "resolvedCount": reputation_data.get("resolved_claims", 0),
        "respondedCount": reputation_data.get("responded_claims", 0),
        "finalizedCount": reputation_data.get("finalized_claims", 0),
        "overallSatisfaction": _safe_div(
            float(reputation_data.get("score_sum", 0)),
            int(reputation_data.get("satisfaction_count", 0))
        )
    }


# -----------------------------
# Solvency (unchanged from v2)
# -----------------------------
def calculate_solvency_score(data: dict) -> float:
    """
    Calculates solvency score (0-100).
    60% Net worth (log-scaled) + 40% Loss ratio stability.
    """
    premiums = float(data.get("premiums", 0.0) or 0.0)
    claims = float(data.get("claims", 0.0) or 0.0)
    
    # [CORREÇÃO] Lê net_worth explicitamente (snake_case do SES ou camelCase legado)
    net_worth = float(data.get("net_worth", 0.0) or data.get("netWorth", 0.0) or 0.0)

    # 1) Net worth score (log-scaled, softly clipped)
    if net_worth <= 100_000:
        net_worth_score = 0.0
    else:
        # log10(100k)=5 => 0; log10(10mi)=7 => 40; log10(1bi)=9 => 80; log10(10bi)=10 => 100
        log_nw = math.log10(net_worth)
        net_worth_score = _clamp((log_nw - 5) * 20)

    # 2) Loss ratio score (ideal band ~40%-70%, center 60%)
    lr = claims / premiums if premiums > 0 else 0.0
    if lr <= 0:
        lr_score = 50.0  # unknown or extremely low, neutral
    else:
        dist = abs(lr - 0.60)
        lr_score = _clamp(100 - (dist * 200))

    final_score = (net_worth_score * 0.6) + (lr_score * 0.4)
    return round(final_score, 1)


# --------------------------------------------------------
# Reputation (legacy / absolute) - preserved as fallback
# --------------------------------------------------------
def calculate_reputation_score(reputation_data: dict) -> float:
    """
    Legacy reputation score (0-100), used when market benchmark is unavailable.
    """
    # [CORREÇÃO] Usa o extrator robusto
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
    satisfaction_norm = _clamp((satisfaction - 1) * 25) if satisfaction > 0 else 0.0

    efficiency = (answered_rate * 40) + (solved_rate * 40)
    satisfaction_score = (satisfaction_norm / 100) * 20
    volume_penalty = min(20.0, math.log10(total + 1) * 5)

    final = efficiency + satisfaction_score - volume_penalty
    return round(_clamp(final), 1)


# --------------------------------------------------------
# Reputation (contextual) - Observed vs Expected (v3)
# --------------------------------------------------------
def calculate_complaint_pressure(
    observed_complaints: int,
    premiums_brl: float,
    market_rate: float,
) -> Tuple[float, float]:
    """
    Observed vs Expected complaint pressure.
    """
    safe_premiums = max(float(premiums_brl or 0.0), MIN_PREMIUM_FOR_EXPECTED)
    expected = safe_premiums * float(market_rate or 0.0)

    pressure = (float(observed_complaints) + ALPHA) / (float(expected) + ALPHA)
    
    # [CORREÇÃO] Suavização da curva de pressão (fator 0.33)
    # Isso evita que empresas na média (pressure=1) caiam para nota 50. Agora ficam em ~75.
    score = 100.0 / (1.0 + (0.33 * pressure))

    return _clamp(score), max(0.0, pressure)


def calculate_reputation_contextual(
    reputation_data: dict,
    premiums_brl: float,
    market_rate: float,
) -> Optional[Dict[str, Any]]:
    """
    Contextual reputation scorer (Observed vs Expected + operational + satisfaction).
    """
    # [CORREÇÃO] Usa o extrator robusto
    stats = _extract_stats(reputation_data)
    if not stats:
        return None

    complaints_total = int(stats.get("complaintsCount") or 0)
    resolved_count = int(stats.get("resolvedCount") or 0)
    responded_count = int(stats.get("respondedCount") or 0)
    denom_resolution = int(stats.get("finalizedCount") or complaints_total or 0)
    satisfaction = float(stats.get("overallSatisfaction") or 0.0)  # expected 1.0-5.0

    # Pillar 1: Complaint pressure (contextual)
    pressure_score, pressure_idx = calculate_complaint_pressure(
        observed_complaints=complaints_total,
        premiums_brl=float(premiums_brl or 0.0),
        market_rate=float(market_rate or 0.0),
    )

    # Pillar 2: Operational efficiency
    if complaints_total <= 0:
        # No complaints: operational KPIs are not informative; keep neutral.
        efficiency_score = 50.0
    else:
        rate_resp = _safe_div(responded_count, complaints_total)
        rate_sol = _safe_div(resolved_count, denom_resolution)
        efficiency_score = _clamp((rate_resp * 40) + (rate_sol * 60))

    # Pillar 3: Satisfaction (subjective)
    sat_norm = _clamp((satisfaction - 1) * 25) if satisfaction > 0 else 0.0

    # Final weighted reputation score
    if satisfaction > 0:
        final_score = (
            (pressure_score * WEIGHT_PRESSURE)
            + (efficiency_score * WEIGHT_EFFICIENCY)
            + (sat_norm * WEIGHT_SATISFACTION)
        )
    else:
        # No satisfaction signal: reweight towards pressure, penalize opacity lightly.
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


# -----------------------------
# Innovation (Open Insurance)
# -----------------------------
def calculate_opin_score(products: list, is_participant: bool) -> float:
    """
    Open Insurance innovation score.
      - +50 if participant
      - +2 points per product up to +50
    """
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


# --------------------------------------------------------
# Market benchmark + batch processing (new in v3)
# --------------------------------------------------------
def compute_market_benchmarks(insurers: List[dict]) -> float:
    """
    Computes the global market complaint rate (complaints per BRL of premiums).
    """
    total_complaints = 0
    total_premiums = 0.0

    for ins in insurers:
        data = (ins or {}).get("data", {}) or {}
        rep = (ins or {}).get("reputation")

        prem = float(data.get("premiums", 0.0) or 0.0)
        if not rep or prem < MIN_PREMIUM_FOR_BENCHMARK:
            continue

        stats = _extract_stats(rep) # [CORREÇÃO] Usa o extrator robusto
        comp = int(stats.get("complaintsCount") or 0)

        # Allow zeros; they still inform the denominator.
        total_complaints += max(0, comp)
        total_premiums += prem

    if total_premiums <= 0:
        return 0.0

    return total_complaints / total_premiums


def apply_intelligence_batch(insurers: List[dict]) -> List[dict]:
    """
    Batch processor:
      1) Computes market benchmark (complaints per BRL).
      2) Stores it in module context.
      3) Applies calculate_score() in-place for each insurer.
    """
    market_rate = compute_market_benchmarks(insurers)
    _CONTEXT["market_rate"] = market_rate if market_rate > 0 else None

    for ins in insurers:
        calculate_score(ins)

    return insurers


# -----------------------------
# Public interface (preserved)
# -----------------------------
def calculate_score(insurer_obj: dict) -> dict:
    """
    Computes and injects scores.
    """
    data = insurer_obj.get("data", {}) or {}
    products = insurer_obj.get("products", []) or []
    flags = insurer_obj.get("flags", {}) or {}

    premiums = float(data.get("premiums", 0.0) or 0.0)

    # Individual scores
    solvency_score = calculate_solvency_score(data)
    opin_score = calculate_opin_score(products, bool(flags.get("openInsuranceParticipant")))

    reputation_raw = insurer_obj.get("reputation")
    reputation_score: Optional[float] = None

    market_rate = _CONTEXT.get("market_rate")
    if reputation_raw:
        if isinstance(market_rate, (int, float)) and float(market_rate) > 0:
            rep_result = calculate_reputation_contextual(reputation_raw, premiums, float(market_rate))
            if rep_result:
                reputation_score = float(rep_result["score"])
                if insurer_obj.get("reputation") is not None:
                    insurer_obj["reputation"].setdefault("indexes", {})
                    insurer_obj["reputation"]["indexes"].update(
                        {
                            "complaint_pressure": rep_result["pressure_index"],
                            "components": rep_result["metrics"],
                        }
                    )
        else:
            reputation_score = calculate_reputation_score(reputation_raw)

    # Aggregation (preserved)
    if reputation_score is not None:
        final_score = (solvency_score * 0.50) + (reputation_score * 0.40) + (opin_score * 0.10)
    else:
        final_score = (solvency_score * 0.80) + (opin_score * 0.20)

    insurer_obj["segment"] = determine_segment(data)

    insurer_obj.setdefault("data", {})
    insurer_obj["data"].setdefault("components", {})
    insurer_obj["data"]["components"] = {
        "solvency": round(float(solvency_score), 1),
        "reputation": round(float(reputation_score), 1) if reputation_score is not None else None,
        "innovation": round(float(opin_score), 1),
    }
    insurer_obj["data"]["score"] = round(float(final_score), 1)

    # Ensure loss ratio
    if premiums > 0:
        insurer_obj["data"]["lossRatio"] = float(data.get("claims", 0.0) or 0.0) / premiums
    else:
        insurer_obj["data"]["lossRatio"] = 0.0

    return insurer_obj
