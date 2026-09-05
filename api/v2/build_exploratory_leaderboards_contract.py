from __future__ import annotations

import json
import math
import re
import unicodedata
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ASSESSMENT_PATH = Path("data/derived/v2/assessment_eligibility_contract.json")
SEMANTIC_PATH = Path(
    "data/derived/v2/cross_pillar_assessment_semantic_contract.json"
)
FINANCIAL_PATH = Path("data/derived/v2/financial_methodology_closure.json")
CONDUCT_PATH = Path("data/derived/v2/conduct_methodology_closure.json")
RECONCILIATION_PATH = Path("data/derived/v2/conduct_coverage_reconciliation.json")
RANKING_PREFLIGHT_PATH = Path("data/derived/v2/ranking_eligibility_preflight.json")
OUTPUT_PATH = Path("data/derived/v2/exploratory_leaderboards_contract.json")
PUBLIC_DIR = Path("data/derived/v2/public")

VERSION = "2.0-draft-exploratory-leaderboards-contract-1"
TOP_POSITIONS = 10


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _finite(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _map_rows(
    rows: list[dict[str, Any]], label: str
) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        entity_id = str(row.get("entity_id") or "")
        if not entity_id:
            raise ValueError(f"{label} row without entity_id")
        if entity_id in result:
            raise ValueError(f"duplicate {label} entity_id: {entity_id}")
        result[entity_id] = row
    return result


def _normalize_search(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = "".join(
        char
        for char in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(char)
    )
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def _competition_ranked(
    rows: list[dict[str, Any]],
    *,
    metric_key: str,
    descending: bool,
    top_positions: int = TOP_POSITIONS,
) -> list[dict[str, Any]]:
    prepared = [row for row in rows if _finite(row.get(metric_key)) is not None]
    prepared.sort(
        key=lambda row: (
            -float(row[metric_key]) if descending else float(row[metric_key]),
            str(row.get("legal_name") or ""),
            str(row.get("entity_id") or ""),
        )
    )
    output: list[dict[str, Any]] = []
    previous_value: float | None = None
    previous_rank = 0
    for index, row in enumerate(prepared, start=1):
        value = float(row[metric_key])
        if previous_value is None or value != previous_value:
            rank = index
            previous_rank = rank
            previous_value = value
        else:
            rank = previous_rank
        if rank > top_positions:
            break
        output.append({**row, "leaderboard_rank": rank})
    return output


def _validate_sources(
    assessment: dict[str, Any],
    semantic: dict[str, Any],
    financial: dict[str, Any],
    conduct: dict[str, Any],
    reconciliation: dict[str, Any],
    ranking_preflight: dict[str, Any],
) -> tuple[
    int,
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, dict[str, Any]],
]:
    if assessment.get("status") != "assessment_eligibility_contract_closed":
        raise ValueError("assessment eligibility contract is not closed")
    if semantic.get("status") != "cross_pillar_assessment_semantic_contract_closed":
        raise ValueError("semantic contract is not closed")
    if financial.get("status") != "financial_methodology_closed_for_signal_design":
        raise ValueError("financial methodology is not closed")
    if conduct.get("status") != "conduct_methodology_closed_for_signal_design":
        raise ValueError("conduct methodology is not closed")
    if reconciliation.get("artifact") != "v2_conduct_coverage_reconciliation":
        raise ValueError("unexpected reconciliation artifact")
    if (
        ranking_preflight.get("status")
        != "ranking_eligibility_preflight_closed_gate_remains_blocked"
    ):
        raise ValueError("ranking eligibility preflight is not closed")
    if (
        (ranking_preflight.get("closure_decision") or {}).get(
            "ranking_eligibility_gate_opened"
        )
        is not False
    ):
        raise ValueError("general ranking gate must remain closed")
    if int((ranking_preflight.get("population") or {}).get("ranking_eligible") or 0):
        raise ValueError("ranking_eligible must remain zero")
    if (
        (ranking_preflight.get("closure_decision") or {}).get(
            "semantic_comparison_of_assessment_eligible_subset_supported"
        )
        is not True
    ):
        raise ValueError(
            "semantic comparison must be supported before explorer publication"
        )

    assessment_by = _map_rows(list(assessment.get("entities") or []), "assessment")
    semantic_by = _map_rows(list(semantic.get("entities") or []), "semantic")
    financial_by = _map_rows(list(financial.get("entities") or []), "financial")
    reconciliation_by = _map_rows(
        list(reconciliation.get("entities") or []), "reconciliation"
    )
    conduct_by = _map_rows(
        list(conduct.get("candidate_entities") or [])
        + list(conduct.get("non_comparable_entities") or []),
        "conduct",
    )

    regulatory = int((assessment.get("population") or {}).get("regulatory_universe") or 0)
    if not regulatory:
        raise ValueError("missing regulatory universe")
    populations = {
        "assessment": set(assessment_by),
        "semantic": set(semantic_by),
        "financial": set(financial_by),
        "reconciliation": set(reconciliation_by),
        "conduct": set(conduct_by),
    }
    for label, ids in populations.items():
        if len(ids) != regulatory:
            raise ValueError(f"{label} population mismatch: {len(ids)} != {regulatory}")
    reference = populations["assessment"]
    for label, ids in populations.items():
        if ids != reference:
            raise ValueError(f"{label} entity set differs from assessment population")

    return (
        regulatory,
        assessment_by,
        semantic_by,
        financial_by,
        conduct_by,
        reconciliation_by,
    )


def _base_public_row(
    entity_id: str,
    assessment: dict[str, Any],
    semantic: dict[str, Any],
    financial: dict[str, Any],
    conduct: dict[str, Any],
    reconciliation: dict[str, Any],
) -> dict[str, Any]:
    exposure = reconciliation.get("insurance_exposure_12m") or {}
    public = semantic.get("public_assessment") or {}
    pressure = conduct.get("pressure_conclusion") or {}
    direct = conduct.get("direct_pressure") or {}
    annual = direct.get("annual") or {}
    temporal = direct.get("temporal_coverage") or {}
    persistence = direct.get("persistence") or {}
    trend = direct.get("trend") or {}
    comparability = reconciliation.get("pressure_comparability") or {}
    display_name = reconciliation.get("display_name") or conduct.get("display_name")
    legal_name = (
        assessment.get("legal_name")
        or financial.get("legal_name")
        or reconciliation.get("legal_name")
    )

    if pressure.get("state") == "pressure_unavailable_not_comparable":
        conduct_public = {
            "state": pressure.get("state"),
            "summary": pressure.get("human_summary"),
            "comparability_state": pressure.get("comparability_reason")
            or comparability.get("state"),
            "reason_code": pressure.get("reason_code")
            or comparability.get("reason_code"),
            "observed_complaints_12m": reconciliation.get("complaints_12m"),
            "expected_complaints_12m": None,
            "pressure_ratio": None,
            "comparable_months": None,
            "persistence": None,
            "trend": None,
        }
    else:
        conduct_public = {
            "state": pressure.get("state"),
            "summary": pressure.get("human_summary"),
            "comparability_state": conduct.get("comparability_state")
            or comparability.get("state"),
            "reason_code": comparability.get("reason_code"),
            "observed_complaints_12m": annual.get("observed_complaints"),
            "expected_complaints_12m": annual.get("expected_complaints"),
            "pressure_ratio": annual.get("ratio"),
            "comparable_months": temporal.get("comparable_months"),
            "persistence": persistence.get("state"),
            "trend": trend.get("state"),
        }

    return {
        "entity_id": entity_id,
        "fip_code": assessment.get("fip_code") or financial.get("fip_code"),
        "legal_name": legal_name,
        "display_name": display_name,
        "search_text": _normalize_search(
            " ".join(filter(None, [str(legal_name or ""), str(display_name or "")]))
        ),
        "assessment": {
            "eligible": bool(assessment.get("assessment_eligible")),
            "state": assessment.get("assessment_state"),
            "completeness": semantic.get("assessment_completeness"),
            "matrix_state": semantic.get("matrix_state"),
            "public_class": public.get("public_class"),
            "title": public.get("title"),
            "summary": public.get("summary"),
            "why_it_matters": public.get("why_it_matters"),
            "mandatory_limit": public.get("mandatory_limit"),
            "evidence_readiness": semantic.get("evidence_readiness"),
        },
        "financial": {
            "reference_period": financial.get("reference_period"),
            "core_signal": financial.get("core_financial_signal"),
            "capital": financial.get("capital"),
            "liquidity": financial.get("liquidity"),
            "operating_context": financial.get("operating_context"),
            "evidence_confidence": financial.get("evidence_confidence"),
            "public_interpretation": financial.get("public_interpretation"),
        },
        "conduct": conduct_public,
        "market_context": {
            "insurance_premium_direct_12m": exposure.get("insurance_premium_direct"),
            "insurance_premium_earned_12m": exposure.get(
                "insurance_premium_earned_diagnostic"
            ),
            "complaints_12m": reconciliation.get("complaints_12m"),
            "premium_is_quality_metric": False,
            "complaints_are_customer_count": False,
        },
        "explore_memberships": {
            "leaderboards": [],
            "collections": [],
        },
    }


def _leaderboard(
    *,
    leaderboard_id: str,
    title: str,
    question: str,
    metric: str,
    unit: str,
    direction: str,
    scope: str,
    rows: list[dict[str, Any]],
    caveats: list[str],
) -> dict[str, Any]:
    return {
        "id": leaderboard_id,
        "type": "public_numeric_leaderboard",
        "title": title,
        "question": question,
        "metric": metric,
        "unit": unit,
        "direction": direction,
        "scope": scope,
        "top_positions": TOP_POSITIONS,
        "tie_policy": (
            "competition_rank_same_metric_value_same_rank_no_secondary_merit_tiebreaker"
        ),
        "is_general_ranking": False,
        "caveats": caveats,
        "entries": rows,
    }


def _collection(
    *,
    collection_id: str,
    title: str,
    rule: str,
    rows: list[dict[str, Any]],
    caveats: list[str],
) -> dict[str, Any]:
    return {
        "id": collection_id,
        "type": "public_semantic_collection",
        "title": title,
        "rule": rule,
        "ordered": False,
        "is_general_ranking": False,
        "caveats": caveats,
        "entity_count": len(rows),
        "entries": rows,
    }


def build_exploratory_leaderboards_contract(
    assessment: dict[str, Any],
    semantic: dict[str, Any],
    financial: dict[str, Any],
    conduct: dict[str, Any],
    reconciliation: dict[str, Any],
    ranking_preflight: dict[str, Any],
) -> dict[str, Any]:
    (
        regulatory,
        assessment_by,
        semantic_by,
        financial_by,
        conduct_by,
        reconciliation_by,
    ) = _validate_sources(
        assessment,
        semantic,
        financial,
        conduct,
        reconciliation,
        ranking_preflight,
    )

    explorer_rows = [
        _base_public_row(
            entity_id,
            assessment_by[entity_id],
            semantic_by[entity_id],
            financial_by[entity_id],
            conduct_by[entity_id],
            reconciliation_by[entity_id],
        )
        for entity_id in sorted(assessment_by)
    ]
    explorer_by = {row["entity_id"]: row for row in explorer_rows}

    factual_source: list[dict[str, Any]] = []
    for row in explorer_rows:
        factual_source.append(
            {
                "entity_id": row["entity_id"],
                "legal_name": row["legal_name"],
                "display_name": row["display_name"],
                "assessment_eligible": row["assessment"]["eligible"],
                "public_class": row["assessment"]["public_class"],
                "premium_direct_12m": _finite(
                    row["market_context"]["insurance_premium_direct_12m"]
                ),
                "pla_cmr_ratio": _finite(
                    (row["financial"]["capital"] or {}).get("pla_cmr_ratio")
                ),
                "ilt": _finite((row["financial"]["liquidity"] or {}).get("value")),
                "conduct_pressure_state": row["conduct"]["state"],
                "conduct_pressure_ratio": _finite(row["conduct"]["pressure_ratio"]),
                "conduct_observed": row["conduct"]["observed_complaints_12m"],
                "conduct_expected": row["conduct"]["expected_complaints_12m"],
                "conduct_comparable_months": row["conduct"]["comparable_months"],
            }
        )

    premium_candidates = [
        row
        for row in factual_source
        if row["premium_direct_12m"] is not None and row["premium_direct_12m"] > 0
    ]
    capital_candidates = [
        row for row in factual_source if row["pla_cmr_ratio"] is not None
    ]
    liquidity_candidates = [row for row in factual_source if row["ilt"] is not None]
    conduct_low_candidates = [
        row
        for row in factual_source
        if row["conduct_pressure_state"] == "below_expected_with_sufficient_evidence"
        and row["conduct_pressure_ratio"] is not None
    ]
    conduct_high_candidates = [
        row
        for row in factual_source
        if row["conduct_pressure_state"] == "above_expected_with_sufficient_evidence"
        and row["conduct_pressure_ratio"] is not None
    ]

    leaderboards = [
        _leaderboard(
            leaderboard_id="largest_by_direct_premium",
            title="Maiores seguradoras por prêmio direto de seguros",
            question=(
                "Quais seguradoras apresentam maior volume de prêmio direto de seguros "
                "na janela de 12 meses?"
            ),
            metric="insurance_premium_direct_12m",
            unit="BRL",
            direction="descending",
            scope="regulatory_universe_with_positive_direct_insurance_premium",
            rows=_competition_ranked(
                premium_candidates,
                metric_key="premium_direct_12m",
                descending=True,
            ),
            caveats=[
                "Prêmio mede volume econômico da operação, não qualidade, popularidade ou confiabilidade.",
                "Prêmio zero ou negativo não recebe posição nesta lista e não é tratado como desempenho ruim.",
            ],
        ),
        _leaderboard(
            leaderboard_id="highest_pla_cmr_ratio",
            title="Maiores relações PLA/CMR observadas",
            question=(
                "Quais seguradoras apresentam maior relação entre Patrimônio Líquido "
                "Ajustado e Capital Mínimo Requerido na competência de referência?"
            ),
            metric="pla_cmr_ratio",
            unit="ratio",
            direction="descending",
            scope="regulatory_universe_with_derivable_pla_cmr",
            rows=_competition_ranked(
                capital_candidates,
                metric_key="pla_cmr_ratio",
                descending=True,
            ),
            caveats=[
                "A lista ordena uma métrica de capital; não é ranking financeiro geral.",
                "PLA/CMR acima de 1 indica requisito observado atendido, mas magnitude maior não recebe mérito ilimitado na avaliação semântica.",
            ],
        ),
        _leaderboard(
            leaderboard_id="highest_ilt",
            title="Maiores ILTs observados",
            question=(
                "Quais seguradoras apresentam maior ILT na competência financeira de "
                "referência?"
            ),
            metric="ilt",
            unit="ratio",
            direction="descending",
            scope="regulatory_universe_with_derivable_ilt",
            rows=_competition_ranked(
                liquidity_candidates,
                metric_key="ilt",
                descending=True,
            ),
            caveats=[
                "A lista ordena o ILT observado; não é ranking financeiro geral nem selo de liquidez da SUSEP.",
                "A referência 1,0 usada pela metodologia é paridade aritmética, não limite prudencial oficial.",
            ],
        ),
        _leaderboard(
            leaderboard_id="lowest_conduct_pressure_ratio",
            title=(
                "Menor pressão relativa de reclamações entre conclusões abaixo do esperado"
            ),
            question=(
                "Entre as seguradoras com evidência suficiente de reclamações abaixo do "
                "esperado para o tamanho da operação, quais apresentam menor razão "
                "observadas/esperadas?"
            ),
            metric="conduct_observed_expected_ratio",
            unit="ratio",
            direction="ascending",
            scope="below_expected_with_sufficient_evidence_only",
            rows=_competition_ranked(
                conduct_low_candidates,
                metric_key="conduct_pressure_ratio",
                descending=False,
            ),
            caveats=[
                "A lista só inclui conclusões anuais abaixo do esperado com evidência suficiente e denominador estável.",
                "Razão menor não prova melhor atendimento, não mede percentual de clientes insatisfeitos e não vira bônus no ranking geral.",
                "Empates usam a mesma posição; nenhum critério secundário de mérito é inventado.",
            ],
        ),
        _leaderboard(
            leaderboard_id="highest_conduct_pressure_ratio",
            title=(
                "Maior pressão relativa de reclamações entre conclusões acima do esperado"
            ),
            question=(
                "Entre as seguradoras com evidência suficiente de reclamações acima do "
                "esperado para o tamanho da operação, quais apresentam maior razão "
                "observadas/esperadas?"
            ),
            metric="conduct_observed_expected_ratio",
            unit="ratio",
            direction="descending",
            scope="above_expected_with_sufficient_evidence_only",
            rows=_competition_ranked(
                conduct_high_candidates,
                metric_key="conduct_pressure_ratio",
                descending=True,
            ),
            caveats=[
                "A lista só inclui conclusões anuais acima do esperado com evidência suficiente e denominador estável.",
                "Razão maior indica pressão relativa no indicador; não significa que todos os clientes terão problemas nem define sozinha a qualidade geral da seguradora.",
            ],
        ),
    ]

    favorable_ids = {
        row["entity_id"]
        for row in explorer_rows
        if row["assessment"]["eligible"]
        and row["assessment"]["public_class"] == "favorable_reading"
    }
    financial_clear_ids = {
        row["entity_id"]
        for row in explorer_rows
        if row["financial"]["core_signal"]
        == "core_indicators_without_current_shortfall"
    }
    favorable_below_ids = {
        row["entity_id"]
        for row in explorer_rows
        if row["entity_id"] in favorable_ids
        and row["conduct"]["state"] == "below_expected_with_sufficient_evidence"
    }
    improving_adverse_ids = {
        row["entity_id"]
        for row in explorer_rows
        if row["conduct"]["state"] == "above_expected_with_sufficient_evidence"
        and row["conduct"]["trend"] == "improving_pressure"
    }
    persistent_adverse_ids = {
        row["entity_id"]
        for row in explorer_rows
        if row["conduct"]["state"] == "above_expected_with_sufficient_evidence"
        and row["conduct"]["persistence"] == "persistent_above_expected"
    }

    def collection_rows(ids: set[str]) -> list[dict[str, Any]]:
        return [
            {
                "entity_id": explorer_by[entity_id]["entity_id"],
                "legal_name": explorer_by[entity_id]["legal_name"],
                "display_name": explorer_by[entity_id]["display_name"],
                "assessment_eligible": explorer_by[entity_id]["assessment"]["eligible"],
                "public_class": explorer_by[entity_id]["assessment"]["public_class"],
            }
            for entity_id in sorted(
                ids,
                key=lambda eid: (str(explorer_by[eid]["legal_name"] or ""), eid),
            )
        ]

    collections = [
        _collection(
            collection_id="financial_core_without_current_adverse_signal",
            title="Sem sinal financeiro central adverso na competência observada",
            rule=(
                "core_financial_signal == core_indicators_without_current_shortfall"
            ),
            rows=collection_rows(financial_clear_ids),
            caveats=[
                "É uma coleção sem ordem interna.",
                "Ausência de sinal adverso central não é garantia de solvência nem superioridade.",
            ],
        ),
        _collection(
            collection_id="favorable_joint_assessment",
            title="Leitura central favorável na avaliação conjunta",
            rule=(
                "assessment_eligible == true AND public_class == favorable_reading"
            ),
            rows=collection_rows(favorable_ids),
            caveats=[
                "É uma coleção sem ordem interna; não existe 1º ou último lugar.",
                "A leitura favorável vale somente para o escopo e os períodos avaliados.",
            ],
        ),
        _collection(
            collection_id="favorable_with_below_expected_conduct",
            title="Leitura central favorável com Conduta abaixo do esperado",
            rule=(
                "favorable_joint_assessment AND conduct == "
                "below_expected_with_sufficient_evidence"
            ),
            rows=collection_rows(favorable_below_ids),
            caveats=[
                "A coincidência de sinais favoráveis não constitui selo de excelência.",
                "Reclamações abaixo do esperado não provam melhor atendimento.",
            ],
        ),
        _collection(
            collection_id="conduct_improving_but_still_adverse",
            title="Pressão de Conduta ainda adversa, com sinal recente de melhora",
            rule=(
                "conduct == above_expected_with_sufficient_evidence AND trend == "
                "improving_pressure"
            ),
            rows=collection_rows(improving_adverse_ids),
            caveats=[
                "A melhora recente não apaga a pressão anual ainda acima do esperado.",
            ],
        ),
        _collection(
            collection_id="conduct_persistent_above_expected",
            title="Pressão de Conduta recorrente acima do esperado",
            rule=(
                "conduct == above_expected_with_sufficient_evidence AND persistence == "
                "persistent_above_expected"
            ),
            rows=collection_rows(persistent_adverse_ids),
            caveats=[
                "Persistência qualifica o sinal adverso; não transforma a coleção em ranking de piores seguradoras.",
            ],
        ),
    ]

    for board in leaderboards:
        for entry in board["entries"]:
            explorer_by[entry["entity_id"]]["explore_memberships"][
                "leaderboards"
            ].append({"id": board["id"], "rank": entry["leaderboard_rank"]})
    for group in collections:
        for entry in group["entries"]:
            explorer_by[entry["entity_id"]]["explore_memberships"][
                "collections"
            ].append(group["id"])

    concepts = {
        "maiores_por_premio": {
            "classification": "public_numeric_leaderboard",
            "output_id": "largest_by_direct_premium",
        },
        "maior_folga_de_capital": {
            "classification": "public_numeric_leaderboard",
            "output_id": "highest_pla_cmr_ratio",
        },
        "maior_ilt": {
            "classification": "public_numeric_leaderboard",
            "output_id": "highest_ilt",
        },
        "menor_pressao_de_reclamacoes": {
            "classification": "public_numeric_leaderboard",
            "output_id": "lowest_conduct_pressure_ratio",
        },
        "maior_pressao_de_reclamacoes": {
            "classification": "public_numeric_leaderboard",
            "output_id": "highest_conduct_pressure_ratio",
        },
        "financeiro_mais_em_dia": {
            "classification": "public_semantic_collection",
            "output_id": "financial_core_without_current_adverse_signal",
            "reason": (
                "o contrato suporta identificar ausência de sinal financeiro central adverso, "
                "mas não ordenar internamente as empresas sem inventar mérito adicional"
            ),
        },
        "mais_popular": {
            "classification": "not_supported",
            "reason": (
                "prêmio mede tamanho econômico e reclamações medem atrito observado; "
                "nenhuma fonte atual mede popularidade"
            ),
            "safe_alternative": "largest_by_direct_premium",
        },
        "emergente_promissora": {
            "classification": "not_supported",
            "reason": (
                "exige contrato próprio de crescimento, maturidade mínima, porte e "
                "significado público de promissora"
            ),
            "safe_alternative": None,
        },
        "consagrada_exemplar": {
            "classification": "not_supported",
            "reason": (
                "consagrada exige evidência de tenure/legado e exemplar implicaria mérito "
                "geral não definido"
            ),
            "safe_alternative": "favorable_with_below_expected_conduct",
        },
        "mais_reclamadas_em_volume_absoluto": {
            "classification": "context_only",
            "reason": (
                "volume absoluto é fortemente influenciado por tamanho; permanece disponível "
                "no explorer, sem leaderboard de qualidade"
            ),
        },
        "crescimento_de_premio": {
            "classification": "not_supported",
            "reason": (
                "a v2 ainda não fechou contrato longitudinal de crescimento de produção "
                "para uso público"
            ),
            "safe_alternative": None,
        },
        "ranking_geral": {
            "classification": "not_supported",
            "reason": (
                "Ranking Eligibility Preflight mantém ranking_eligible = 0 por cobertura e "
                "ausência de ordem total defensável"
            ),
            "safe_alternative": "semantic_comparator",
        },
    }

    leaderboard_candidate_counts = {
        "largest_by_direct_premium": len(premium_candidates),
        "highest_pla_cmr_ratio": len(capital_candidates),
        "highest_ilt": len(liquidity_candidates),
        "lowest_conduct_pressure_ratio": len(conduct_low_candidates),
        "highest_conduct_pressure_ratio": len(conduct_high_candidates),
    }
    collection_counts = {group["id"]: group["entity_count"] for group in collections}

    return {
        "artifact": "v2_exploratory_leaderboards_contract",
        "generated_at": _utc_now(),
        "version": VERSION,
        "status": "exploratory_leaderboards_contract_closed",
        "product_direction": (
            "semantic_assessment_and_comparator_primary_exploratory_leaderboards_secondary"
        ),
        "general_scoring": "forbidden",
        "general_ranking": "blocked",
        "ranking_eligible": 0,
        "human_model": {
            "primary_question": (
                "Quais formas de explorar e ordenar aspectos específicos do mercado são "
                "úteis e honestas sem transformar uma métrica isolada em ranking geral?"
            ),
            "rule": (
                "leaderboard numérico só existe quando a própria métrica define a ordem; "
                "conceitos compostos permanecem coleções semânticas ou não suportados"
            ),
        },
        "source_contracts": {
            "assessment": assessment.get("artifact"),
            "semantic": semantic.get("artifact"),
            "financial": financial.get("artifact"),
            "conduct": conduct.get("artifact"),
            "reconciliation": reconciliation.get("artifact"),
            "ranking_preflight": ranking_preflight.get("artifact"),
        },
        "population": {
            "regulatory_universe": regulatory,
            "assessment_eligible": sum(
                bool(row["assessment"]["eligible"]) for row in explorer_rows
            ),
            "assessment_not_eligible": sum(
                not bool(row["assessment"]["eligible"]) for row in explorer_rows
            ),
            "ranking_eligible": 0,
        },
        "publication_policy": {
            "semantic_assessment_is_primary_product": True,
            "side_by_side_comparison_supported": True,
            "recommended_max_side_by_side_cards": 4,
            "automatic_winner_label_supported": False,
            "metric_specific_leaderboards_supported": True,
            "semantic_collections_supported": True,
            "general_composite_ranking_supported": False,
            "leaderboard_metric_may_be_relabelled_best_insurer": False,
            "missing_data_may_receive_bottom_position": False,
            "secondary_merit_tiebreaker_allowed": False,
            "php_may_recompute_methodology": False,
            "php_role": (
                "render_precomputed_semantics_metrics_collections_and_leaderboards"
            ),
        },
        "concept_registry": concepts,
        "diagnostics": {
            "leaderboard_candidate_counts": leaderboard_candidate_counts,
            "leaderboard_output_row_counts": {
                board["id"]: len(board["entries"]) for board in leaderboards
            },
            "collection_counts": collection_counts,
            "concept_classification_counts": dict(
                sorted(
                    Counter(
                        item["classification"] for item in concepts.values()
                    ).items()
                )
            ),
        },
        "leaderboards": leaderboards,
        "collections": collections,
        "public_output_contract": {
            "explorer_file": "public/insurer_explorer.json",
            "index_file": "public/explore_index.json",
            "leaderboard_directory": "public/leaderboards",
            "collection_directory": "public/collections",
            "host_role": (
                "PHP reads JSON and renders; it must not recreate methodological decisions"
            ),
        },
        "closure_decision": {
            "exploratory_leaderboards_gate_opened": True,
            "general_ranking_gate_opened": False,
            "ranking_eligible": 0,
            "semantic_comparator_ready_for_data_contract": True,
            "metric_specific_public_leaderboards_ready": True,
            "general_ranking_remains_blocked": True,
            "unsupported_labels_must_not_be_synthesized": True,
            "next_product_stage": "public_api_json_packaging_and_frontend_php_integration",
        },
        "explorer_entities": sorted(
            explorer_rows,
            key=lambda row: (str(row.get("legal_name") or ""), row["entity_id"]),
        ),
    }


def write_public_outputs(
    payload: dict[str, Any], root: Path = PUBLIC_DIR
) -> list[Path]:
    root.mkdir(parents=True, exist_ok=True)
    leaderboard_dir = root / "leaderboards"
    collection_dir = root / "collections"
    leaderboard_dir.mkdir(parents=True, exist_ok=True)
    collection_dir.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []
    explorer = {
        "artifact": "v2_public_insurer_explorer",
        "generated_at": payload["generated_at"],
        "version": payload["version"],
        "status": "public_explorer_payload_ready",
        "publication_policy": payload["publication_policy"],
        "population": payload["population"],
        "entities": payload["explorer_entities"],
    }
    explorer_path = root / "insurer_explorer.json"
    explorer_path.write_text(
        json.dumps(explorer, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    written.append(explorer_path)

    index = {
        "artifact": "v2_public_explore_index",
        "generated_at": payload["generated_at"],
        "version": payload["version"],
        "status": "public_explore_index_ready",
        "product_direction": payload["product_direction"],
        "population": payload["population"],
        "publication_policy": payload["publication_policy"],
        "concept_registry": payload["concept_registry"],
        "leaderboards": [
            {
                key: board[key]
                for key in (
                    "id",
                    "type",
                    "title",
                    "question",
                    "metric",
                    "unit",
                    "direction",
                    "scope",
                    "top_positions",
                    "tie_policy",
                    "is_general_ranking",
                    "caveats",
                )
            }
            for board in payload["leaderboards"]
        ],
        "collections": [
            {
                key: group[key]
                for key in (
                    "id",
                    "type",
                    "title",
                    "rule",
                    "ordered",
                    "is_general_ranking",
                    "caveats",
                    "entity_count",
                )
            }
            for group in payload["collections"]
        ],
    }
    index_path = root / "explore_index.json"
    index_path.write_text(
        json.dumps(index, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    written.append(index_path)

    for board in payload["leaderboards"]:
        path = leaderboard_dir / f"{board['id']}.json"
        body = {
            "artifact": "v2_public_metric_leaderboard",
            "generated_at": payload["generated_at"],
            "version": payload["version"],
            **board,
        }
        path.write_text(
            json.dumps(body, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        written.append(path)

    for group in payload["collections"]:
        path = collection_dir / f"{group['id']}.json"
        body = {
            "artifact": "v2_public_semantic_collection",
            "generated_at": payload["generated_at"],
            "version": payload["version"],
            **group,
        }
        path.write_text(
            json.dumps(body, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        written.append(path)
    return written


def main() -> None:
    assessment = json.loads(ASSESSMENT_PATH.read_text(encoding="utf-8"))
    semantic = json.loads(SEMANTIC_PATH.read_text(encoding="utf-8"))
    financial = json.loads(FINANCIAL_PATH.read_text(encoding="utf-8"))
    conduct = json.loads(CONDUCT_PATH.read_text(encoding="utf-8"))
    reconciliation = json.loads(RECONCILIATION_PATH.read_text(encoding="utf-8"))
    ranking_preflight = json.loads(
        RANKING_PREFLIGHT_PATH.read_text(encoding="utf-8")
    )
    payload = build_exploratory_leaderboards_contract(
        assessment,
        semantic,
        financial,
        conduct,
        reconciliation,
        ranking_preflight,
    )
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    written = write_public_outputs(payload)
    print(
        json.dumps(
            {
                "artifact": payload["artifact"],
                "status": payload["status"],
                "population": payload["population"],
                "diagnostics": payload["diagnostics"],
                "closure_decision": payload["closure_decision"],
                "public_files": [str(path) for path in written],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
