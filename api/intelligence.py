# api/intelligence.py
from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


# ----------------------------
# Helpers
# ----------------------------

def _safe_div(a: float, b: float) -> float:
    if not b:
        return 0.0
    return a / b


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _to_float(x: Any) -> float:
    try:
        if x is None:
            return 0.0
        if isinstance(x, bool):
            return 0.0
        if isinstance(x, (int, float)):
            return float(x)
        if isinstance(x, str):
            t = x.strip()
            if not t:
                return 0.0
            # pt-BR: "1.234,56" -> "1234.56"
            if "," in t and "." in t:
                t = t.replace(".", "").replace(",", ".")
            elif "," in t and "." not in t:
                t = t.replace(",", ".")
            t = re.sub(r"[^0-9eE\-+.]", "", t)
            if not t or t in {"-", "+", "."}:
                return 0.0
            return float(t)
        return float(x)
    except Exception:
        return 0.0


def _to_int(x: Any) -> int:
    try:
        if x is None:
            return 0
        if isinstance(x, bool):
            return 0
        if isinstance(x, int):
            return int(x)
        if isinstance(x, float):
            return int(x)
        if isinstance(x, str):
            t = x.strip()
            if not t:
                return 0
            t = re.sub(r"[^\d\-+]", "", t)
            if not t or t in {"-", "+"}:
                return 0
            return int(t)
        return int(x)
    except Exception:
        return 0


# ----------------------------
# Reputation extraction (ROBUST)
# ----------------------------

def _extract_stats(reputation_data: dict) -> dict:
    """
    Extrai estatísticas de reputação de forma robusta.

    Aceita (e normaliza) variações de schema:
    - Formato "flat": total_claims, resolved_claims, responded_claims, finalized_claims, score_sum, satisfaction_count
    - Formato "nested": reputation_data["statistics"] com chaves em camelCase e/ou snake_case
    - Mistos (schema drift): "statistics" existe, mas com chaves legado; NÃO faz "pass-through".
    """
    if not reputation_data or not isinstance(reputation_data, dict):
        return {}

    stats = reputation_data.get("statistics")
    src_stats = stats if isinstance(stats, dict) else {}
    src_root = reputation_data

    def _pick(*keys: str) -> Any:
        # Preferência: statistics -> root (mas sempre normalizando)
        for k in keys:
            if k in src_stats and src_stats.get(k) is not None:
                return src_stats.get(k)
        for k in keys:
            if k in src_root and src_root.get(k) is not None:
                return src_root.get(k)
        return None

    complaints = _to_int(_pick("complaintsCount", "total_claims", "complaints_count"))
    resolved = _to_int(_pick("resolvedCount", "resolved_claims", "resolved_count"))
    responded = _to_int(_pick("respondedCount", "responded_claims", "responded_count"))
    finalized = _to_int(_pick("finalizedCount", "finalized_claims", "finalized_count"))

    overall = _pick("overallSatisfaction", "overall_satisfaction")
    if overall is None:
        score_sum = _pick("score_sum", "scoreSum", "satisfaction_sum", "satisfactionSum")
        sat_count = _pick("satisfaction_count", "satisfactionCount")
        overall = _safe_div(_to_float(score_sum), float(_to_int(sat_count)))
    else:
        overall = _to_float(overall)

    return {
        "complaintsCount": complaints,
        "resolvedCount": resolved,
        "respondedCount": responded,
        "finalizedCount": finalized,
        "overallSatisfaction": float(overall or 0.0),
    }


# ----------------------------
# Scoring (mantém seu modelo contextual)
# ----------------------------

def calculate_complaint_pressure(complaints: int, premiums: float, market_avg_complaints_per_premium: float) -> float:
    # Pressão = (reclamações por prêmio) / média mercado
    # Se não há prêmio, assume pressão neutra (1.0) para não punir/beneficiar arbitrariamente.
    if premiums <= 0:
        return 1.0

    rate = complaints / max(premiums, 1.0)
    avg = max(market_avg_complaints_per_premium, 1e-12)
    return rate / avg


def calculate_reputation_contextual(pressure: float, satisfaction: float) -> float:
    """
    Mantém seu modelo: contextual por pressão (benchmark mercado) + satisfação.
    Calibração importante: pressão média (~1.0) NÃO pode virar nota 50 “cara de erro”.

    Regra atual:
    - Base 75
    - Penaliza pressão acima da média e premia abaixo, de forma suave
    - Ajusta por satisfação (0..10)
    """
    base = 75.0

    # Ajuste de pressão (calibrado): média (1.0) -> impacto ~0
    # Quanto maior a pressão, mais perde; quanto menor, mais ganha.
    # O fator 0.33 evita colapsar tudo em 50.
    pressure_impact = (1.0 - pressure) * 30.0 * 0.33

    # Satisfação: normaliza 0..10 para -10..+10 em torno de 5.0
    sat = _clamp(satisfaction, 0.0, 10.0)
    sat_impact = (sat - 5.0) * 2.0

    score = base + pressure_impact + sat_impact
    return _clamp(score, 0.0, 100.0)


def calculate_solvency_score(premiums: float, claims: float, net_worth: float) -> float:
    """
    Modelo simples e estável: quanto maior o patrimônio líquido em relação ao risco operacional,
    melhor. Mantém comportamento defensivo quando dados faltam.
    """
    if net_worth <= 0:
        return 50.0

    # Exposição aproximada: max(premiums, claims) para evitar divisão “boa” quando claims é 0.
    exposure = max(premiums, claims, 1.0)
    ratio = net_worth / exposure

    # Compressão logarítmica para não explodir acima de 100
    # ratio ~1 => ok, ratio >> 1 => melhor, ratio << 1 => pior
    score = 50.0 + 20.0 * math.log10(max(ratio, 1e-6))
    return _clamp(score, 0.0, 100.0)


def calculate_innovation_score(is_open_insurance_participant: bool, products_count: int) -> float:
    """
    Score de inovação: participação Open Insurance + amplitude (produtos).
    Mantém simples para não gerar ruído.
    """
    score = 60.0
    if is_open_insurance_participant:
        score += 20.0
    score += _clamp(products_count / 50.0, 0.0, 1.0) * 20.0
    return _clamp(score, 0.0, 100.0)


def _market_avg_complaints_per_premium(insurers: List[Dict[str, Any]]) -> float:
    total_complaints = 0
    total_premiums = 0.0

    for ins in insurers:
        rep = ins.get("reputation") or {}
        stats = _extract_stats(rep) if isinstance(rep, dict) else {}
        total_complaints += _to_int(stats.get("complaintsCount"))
        data = ins.get("data") or {}
        total_premiums += _to_float(data.get("premiums"))

    if total_premiums <= 0:
        return 0.0
    return total_complaints / max(total_premiums, 1.0)


def _determine_segment(insurer_obj: Dict[str, Any]) -> str:
    seg = (insurer_obj.get("segment") or "").strip().upper()
    if seg in {"S1", "S2", "S3", "S4"}:
        return seg
    return "S4"


def calculate_composite_score(
    insurer_obj: Dict[str, Any],
    market_avg_complaints_per_premium: float,
    reputation_enabled: bool,
) -> Dict[str, Any]:
    data = insurer_obj.get("data") or {}

    premiums = _to_float(data.get("premiums"))
    claims = _to_float(data.get("claims"))

    # net worth (compat snake_case / camelCase)
    net_worth = _to_float(
        data.get("net_worth")
        or data.get("netWorth")
        or insurer_obj.get("net_worth")
        or insurer_obj.get("netWorth")
    )

    flags = insurer_obj.get("flags") or {}
    is_open_insurance = bool(flags.get("openInsuranceParticipant"))
    products = insurer_obj.get("products") or []
    products_count = len(products) if isinstance(products, list) else 0

    solvency = calculate_solvency_score(premiums, claims, net_worth)
    innovation = calculate_innovation_score(is_open_insurance, products_count)

    rep_score = 0.0
    rep_components: Dict[str, Any] = {}

    rep = insurer_obj.get("reputation") or {}
    stats = _extract_stats(rep) if isinstance(rep, dict) else {}

    complaints = _to_int(stats.get("complaintsCount"))
    satisfaction = _to_float(stats.get("overallSatisfaction"))

    if reputation_enabled:
        pressure = calculate_complaint_pressure(complaints, premiums, market_avg_complaints_per_premium)
        rep_score = calculate_reputation_contextual(pressure, satisfaction)
        rep_components = {
            "complaintsCount": complaints,
            "pressure": pressure,
            "overallSatisfaction": satisfaction,
        }
    else:
        rep_score = 0.0
        rep_components = {
            "disabled": True,
        }

    # Segment weights (mantém seu modelo)
    segment = _determine_segment(insurer_obj)
    if segment == "S1":
        w_solv, w_rep, w_inn = 0.45, 0.40, 0.15
    elif segment == "S2":
        w_solv, w_rep, w_inn = 0.40, 0.45, 0.15
    elif segment == "S3":
        w_solv, w_rep, w_inn = 0.35, 0.50, 0.15
    else:
        w_solv, w_rep, w_inn = 0.35, 0.45, 0.20

    # Se reputação desabilitada, redistribui peso
    if not reputation_enabled:
        w_rep = 0.0
        w_solv = 0.60
        w_inn = 0.40

    score = (solvency * w_solv) + (rep_score * w_rep) + (innovation * w_inn)

    return {
        "score": _clamp(score, 0.0, 100.0),
        "components": {
            "solvency": solvency,
            "reputation": rep_score,
            "innovation": innovation,
        },
        "details": {
            "reputation": rep_components,
            "weights": {"solvency": w_solv, "reputation": w_rep, "innovation": w_inn},
            "segment": segment,
        },
    }


def apply_intelligence_batch(insurers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    # Benchmarks de mercado (contextual)
    avg_complaints_per_premium = _market_avg_complaints_per_premium(insurers)

    # Se dataset está “vazio”, desabilita reputação para não gerar pontuação enganosa
    matched = 0
    complaints_sum = 0
    for ins in insurers:
        rep = ins.get("reputation")
        if isinstance(rep, dict) and rep:
            matched += 1
            stats = _extract_stats(rep)
            complaints_sum += _to_int(stats.get("complaintsCount"))

    reputation_dataset_empty = bool(matched and complaints_sum == 0)
    reputation_enabled = not reputation_dataset_empty

    if reputation_dataset_empty:
        print(f"DEBUG: reputation_dataset_empty=True (matched={matched}, complaints=0). Disabling reputation in scoring.")

    for ins in insurers:
        score_obj = calculate_composite_score(
            ins,
            market_avg_complaints_per_premium=avg_complaints_per_premium,
            reputation_enabled=reputation_enabled,
        )
        ins["intelligence"] = score_obj

    return insurers
