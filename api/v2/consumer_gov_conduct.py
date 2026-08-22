from __future__ import annotations

from collections import Counter
from typing import Any

TAXONOMY_COLUMNS: dict[str, tuple[str, ...]] = {
    "area": ("Área", "Area"),
    "assunto": ("Assunto",),
    "grupo_problema": ("Grupo Problema",),
    "problema": ("Problema",),
    "como_comprou_contratou": (
        "Como Comprou Contratou",
        "Como Comprou/Contratou",
    ),
    "canal_origem": ("Canal de Origem",),
}

FILM_POLICY_VERSION = "consumer-gov-film-1-experimental"
MIN_ESTABLISHED_MONTHS = 9
MIN_ESTABLISHED_COMPLAINTS = 30
MIN_OUTCOME_WINDOW_SAMPLE = 10
SHARE_FALL_RATIO = 0.75
SHARE_RISE_RATIO = 4 / 3
RESOLUTION_MATERIAL_CHANGE = 0.10
SATISFACTION_MATERIAL_CHANGE = 0.50


def normalize_text(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().split())


def row_value(row: dict[str, Any], aliases: tuple[str, ...]) -> str:
    for name in aliases:
        value = str(row.get(name) or "").strip()
        if value:
            return value
    return ""


def safe_float(value: Any) -> float | None:
    text = str(value or "").strip().replace(",", ".")
    if not text:
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def new_month_evidence(month: str) -> dict[str, Any]:
    return {
        "month": month,
        "complaints": 0,
        "responded": 0,
        "finalized": 0,
        "evaluated": 0,
        "consumer_resolved": 0,
        "consumer_not_resolved": 0,
        "satisfaction_count": 0,
        "satisfaction_sum": 0.0,
        "response_time_count": 0,
        "response_time_sum_days": 0.0,
        "consumer_contacted_company_yes": 0,
        "consumer_contacted_company_no": 0,
        "taxonomy": {key: Counter() for key in TAXONOMY_COLUMNS},
        "situacao": Counter(),
        "avaliacao_reclamacao": Counter(),
        "analise_recusa": Counter(),
    }


def accumulate_row(evidence: dict[str, Any], row: dict[str, Any]) -> None:
    evidence["complaints"] += 1

    if normalize_text(row.get("Respondida")) == "s":
        evidence["responded"] += 1

    situacao = str(row.get("Situação") or "").strip()
    situacao_norm = normalize_text(situacao)
    if situacao:
        evidence["situacao"][situacao] += 1
    if "finalizada" in situacao_norm or "encerrada" in situacao_norm:
        evidence["finalized"] += 1

    avaliacao = str(row.get("Avaliação Reclamação") or "").strip()
    avaliacao_norm = normalize_text(avaliacao)
    if avaliacao:
        evidence["avaliacao_reclamacao"][avaliacao] += 1
    if avaliacao_norm == "resolvida":
        evidence["evaluated"] += 1
        evidence["consumer_resolved"] += 1
    elif avaliacao_norm in {"nao resolvida", "não resolvida"}:
        evidence["evaluated"] += 1
        evidence["consumer_not_resolved"] += 1

    score = safe_float(row.get("Nota do Consumidor"))
    if score is not None and 1 <= score <= 5:
        evidence["satisfaction_count"] += 1
        evidence["satisfaction_sum"] += score

    response_time = safe_float(row.get("Tempo Resposta"))
    if response_time is not None and response_time >= 0:
        evidence["response_time_count"] += 1
        evidence["response_time_sum_days"] += response_time

    procurou = normalize_text(row.get("Procurou Empresa"))
    if procurou == "s":
        evidence["consumer_contacted_company_yes"] += 1
    elif procurou == "n":
        evidence["consumer_contacted_company_no"] += 1

    analise_recusa = str(row.get("Análise da Recusa") or "").strip()
    if analise_recusa:
        evidence["analise_recusa"][analise_recusa] += 1

    for key, aliases in TAXONOMY_COLUMNS.items():
        value = row_value(row, aliases)
        if value:
            evidence["taxonomy"][key][value] += 1


def _sorted_counter(value: Counter[str]) -> dict[str, int]:
    return {
        key: int(count)
        for key, count in sorted(
            value.items(),
            key=lambda item: (-int(item[1]), str(item[0])),
        )
    }


def finalize_month_evidence(
    evidence: dict[str, Any],
    *,
    matched_current_insurer_market_complaints: int,
) -> dict[str, Any]:
    complaints = int(evidence["complaints"])
    evaluated = int(evidence["evaluated"])
    satisfaction_count = int(evidence["satisfaction_count"])
    response_time_count = int(evidence["response_time_count"])
    contacted_total = (
        int(evidence["consumer_contacted_company_yes"])
        + int(evidence["consumer_contacted_company_no"])
    )

    return {
        "month": evidence["month"],
        "complaints": complaints,
        "matched_current_insurer_market_complaints": int(
            matched_current_insurer_market_complaints
        ),
        "complaint_share_among_matched_current_insurers": (
            complaints / matched_current_insurer_market_complaints
            if matched_current_insurer_market_complaints > 0
            else None
        ),
        "responded": int(evidence["responded"]),
        "response_rate": evidence["responded"] / complaints if complaints else None,
        "finalized": int(evidence["finalized"]),
        "finalized_rate": evidence["finalized"] / complaints if complaints else None,
        "evaluated": evaluated,
        "consumer_resolved": int(evidence["consumer_resolved"]),
        "consumer_not_resolved": int(evidence["consumer_not_resolved"]),
        "consumer_resolved_rate_among_evaluated": (
            evidence["consumer_resolved"] / evaluated if evaluated else None
        ),
        "satisfaction_count": satisfaction_count,
        "average_satisfaction": (
            evidence["satisfaction_sum"] / satisfaction_count
            if satisfaction_count
            else None
        ),
        "response_time_count": response_time_count,
        "average_response_time_days": (
            evidence["response_time_sum_days"] / response_time_count
            if response_time_count
            else None
        ),
        "consumer_contacted_company_yes": int(
            evidence["consumer_contacted_company_yes"]
        ),
        "consumer_contacted_company_no": int(
            evidence["consumer_contacted_company_no"]
        ),
        "consumer_contacted_company_yes_rate": (
            evidence["consumer_contacted_company_yes"] / contacted_total
            if contacted_total
            else None
        ),
        "taxonomy": {
            key: _sorted_counter(counter)
            for key, counter in evidence["taxonomy"].items()
        },
        "situacao": _sorted_counter(evidence["situacao"]),
        "avaliacao_reclamacao": _sorted_counter(
            evidence["avaliacao_reclamacao"]
        ),
        "analise_recusa": _sorted_counter(evidence["analise_recusa"]),
    }


def _weighted_rate(
    months: list[dict[str, Any]],
    numerator: str,
    denominator: str,
) -> tuple[float | None, int]:
    den = sum(int(item.get(denominator) or 0) for item in months)
    if den <= 0:
        return None, 0
    num = sum(int(item.get(numerator) or 0) for item in months)
    return num / den, den


def _weighted_average(
    months: list[dict[str, Any]],
    average_field: str,
    count_field: str,
) -> tuple[float | None, int]:
    total_count = 0
    total_value = 0.0
    for item in months:
        count = int(item.get(count_field) or 0)
        average = item.get(average_field)
        if count <= 0 or average is None:
            continue
        total_count += count
        total_value += float(average) * count
    if total_count <= 0:
        return None, 0
    return total_value / total_count, total_count


def _mean_share(months: list[dict[str, Any]]) -> float | None:
    values = [
        float(item["complaint_share_among_matched_current_insurers"])
        for item in months
        if item.get("complaint_share_among_matched_current_insurers") is not None
    ]
    return sum(values) / len(values) if values else None


def _direction_from_ratio(
    early: float | None,
    recent: float | None,
) -> tuple[str, float | None]:
    if early is None or recent is None:
        return "insufficient", None
    if early == 0:
        if recent == 0:
            return "stable", 1.0
        return "emerging", None
    ratio = recent / early
    if ratio <= SHARE_FALL_RATIO:
        return "falling", ratio
    if ratio >= SHARE_RISE_RATIO:
        return "rising", ratio
    return "stable", ratio


def _direction_from_difference(
    early: float | None,
    recent: float | None,
    *,
    material_change: float,
    early_sample: int,
    recent_sample: int,
) -> tuple[str, float | None]:
    if (
        early is None
        or recent is None
        or early_sample < MIN_OUTCOME_WINDOW_SAMPLE
        or recent_sample < MIN_OUTCOME_WINDOW_SAMPLE
    ):
        return "insufficient", None
    difference = recent - early
    if difference >= material_change:
        return "improving", difference
    if difference <= -material_change:
        return "worsening", difference
    return "stable", difference


def _top_problem_group(months: list[dict[str, Any]]) -> dict[str, Any] | None:
    counter: Counter[str] = Counter()
    for item in months:
        taxonomy = item.get("taxonomy") or {}
        for key, count in (taxonomy.get("grupo_problema") or {}).items():
            counter[str(key)] += int(count or 0)
    total = sum(counter.values())
    if total <= 0:
        return None
    label, count = max(counter.items(), key=lambda item: (item[1], item[0]))
    return {
        "label": label,
        "complaints": int(count),
        "share": count / total,
        "taxonomy_complaints": total,
    }


def build_conduct_film(months: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted(months, key=lambda item: str(item.get("month") or ""))
    if not ordered:
        return {
            "policy_version": FILM_POLICY_VERSION,
            "history_state": "unobserved",
            "conduct_signal": "indeterminate",
        }

    total_complaints = sum(int(item.get("complaints") or 0) for item in ordered)
    months_with_complaints = sum(int(item.get("complaints") or 0) > 0 for item in ordered)
    if total_complaints == 0:
        history_state = "unobserved"
    elif (
        months_with_complaints >= MIN_ESTABLISHED_MONTHS
        and total_complaints >= MIN_ESTABLISHED_COMPLAINTS
    ):
        history_state = "established"
    else:
        history_state = "limited"

    early_three = ordered[:3]
    recent_three = ordered[-3:]
    early_half = ordered[: len(ordered) // 2]
    recent_half = ordered[len(ordered) // 2 :]

    early_share = _mean_share(early_three)
    recent_share = _mean_share(recent_three)
    share_direction, share_ratio = _direction_from_ratio(early_share, recent_share)

    early_resolution, early_resolution_n = _weighted_rate(
        early_half,
        "consumer_resolved",
        "evaluated",
    )
    recent_resolution, recent_resolution_n = _weighted_rate(
        recent_half,
        "consumer_resolved",
        "evaluated",
    )
    resolution_direction, resolution_change = _direction_from_difference(
        early_resolution,
        recent_resolution,
        material_change=RESOLUTION_MATERIAL_CHANGE,
        early_sample=early_resolution_n,
        recent_sample=recent_resolution_n,
    )

    early_satisfaction, early_satisfaction_n = _weighted_average(
        early_half,
        "average_satisfaction",
        "satisfaction_count",
    )
    recent_satisfaction, recent_satisfaction_n = _weighted_average(
        recent_half,
        "average_satisfaction",
        "satisfaction_count",
    )
    satisfaction_direction, satisfaction_change = _direction_from_difference(
        early_satisfaction,
        recent_satisfaction,
        material_change=SATISFACTION_MATERIAL_CHANGE,
        early_sample=early_satisfaction_n,
        recent_sample=recent_satisfaction_n,
    )

    early_problem = _top_problem_group(early_half)
    recent_problem = _top_problem_group(recent_half)
    dominant_problem_group_changed = bool(
        early_problem
        and recent_problem
        and early_problem["label"] != recent_problem["label"]
    )

    if history_state != "established":
        conduct_signal = "indeterminate"
    elif (
        share_direction == "falling"
        and "worsening" not in {resolution_direction, satisfaction_direction}
        and "improving" in {resolution_direction, satisfaction_direction}
    ):
        conduct_signal = "improving_observed_pattern"
    elif (
        share_direction in {"rising", "emerging"}
        and "improving" not in {resolution_direction, satisfaction_direction}
        and "worsening" in {resolution_direction, satisfaction_direction}
    ):
        conduct_signal = "persistent_observed_pressure"
    elif (
        share_direction == "stable"
        and resolution_direction in {"stable", "insufficient"}
        and satisfaction_direction in {"stable", "insufficient"}
    ):
        conduct_signal = "stable_observed_pattern"
    else:
        conduct_signal = "mixed_or_indeterminate"

    return {
        "policy_version": FILM_POLICY_VERSION,
        "assessment_role": "experimental_longitudinal_diagnostic_only",
        "history_state": history_state,
        "conduct_signal": conduct_signal,
        "months_observed": len(ordered),
        "months_with_complaints": months_with_complaints,
        "complaints": total_complaints,
        "complaint_share_trend": {
            "early_3m_mean": early_share,
            "recent_3m_mean": recent_share,
            "recent_to_early_ratio": share_ratio,
            "direction": share_direction,
            "limitation": (
                "Relative share inside matched Consumer.gov current-insurer complaints; "
                "it is not an exposure-normalized complaint incidence rate."
            ),
        },
        "consumer_resolution_trend": {
            "early_half_rate": early_resolution,
            "early_half_sample": early_resolution_n,
            "recent_half_rate": recent_resolution,
            "recent_half_sample": recent_resolution_n,
            "change": resolution_change,
            "direction": resolution_direction,
        },
        "satisfaction_trend": {
            "early_half_average": early_satisfaction,
            "early_half_sample": early_satisfaction_n,
            "recent_half_average": recent_satisfaction,
            "recent_half_sample": recent_satisfaction_n,
            "change": satisfaction_change,
            "direction": satisfaction_direction,
        },
        "problem_mix": {
            "early_half_dominant_group": early_problem,
            "recent_half_dominant_group": recent_problem,
            "dominant_group_changed": dominant_problem_group_changed,
        },
        "policy_thresholds": {
            "established_min_months_with_complaints": MIN_ESTABLISHED_MONTHS,
            "established_min_complaints": MIN_ESTABLISHED_COMPLAINTS,
            "outcome_window_min_sample": MIN_OUTCOME_WINDOW_SAMPLE,
            "complaint_share_falling_ratio_max": SHARE_FALL_RATIO,
            "complaint_share_rising_ratio_min": SHARE_RISE_RATIO,
            "resolution_material_change": RESOLUTION_MATERIAL_CHANGE,
            "satisfaction_material_change": SATISFACTION_MATERIAL_CHANGE,
        },
    }
