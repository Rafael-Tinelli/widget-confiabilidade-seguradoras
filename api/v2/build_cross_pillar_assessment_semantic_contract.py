from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

STAGE1_PATH = Path("data/derived/v2/cross_pillar_calibration_diagnostic.json")
STAGE2_PATH = Path("data/derived/v2/cross_pillar_architecture_experiment.json")
OUTPUT_PATH = Path("data/derived/v2/cross_pillar_assessment_semantic_contract.json")

VERSION = "2.0-draft-cross-pillar-assessment-semantic-contract-2"
MINIMUM_UNIVERSE_SANITY = 100

STATE_BY_SIGNATURE = {
    "F0|C0": "no_current_core_adverse_signal",
    "F0|C1": "conduct_pressure_only",
    "F1|C0": "liquidity_pressure_only",
    "F1|C1": "liquidity_and_conduct_pressure",
    "F2|C0": "capital_shortfall_without_conduct_pressure",
    "F2|C1": "capital_shortfall_and_conduct_pressure",
}

STATE_CONTRACT = {
    "no_current_core_adverse_signal": {
        "public_class": "favorable_reading",
        "title": "Leitura central favorável",
        "summary": (
            "Nos indicadores centrais analisados, não identificamos insuficiência de capital, "
            "pressão de liquidez pelo ILT nem pressão de reclamações acima do esperado para o "
            "tamanho da operação."
        ),
        "why_it_matters": (
            "É um resultado favorável dentro do escopo da metodologia: os três sinais centrais "
            "não apontam fragilidade atual na competência e na janela analisadas."
        ),
        "mandatory_limit": (
            "Isso não garante solvência futura, qualidade de cobertura, preço, atendimento "
            "individual ou superioridade sobre outra seguradora."
        ),
        "materiality": "no_current_core_adverse_signal",
    },
    "conduct_pressure_only": {
        "public_class": "attention",
        "title": "Atenção à Conduta",
        "summary": (
            "Os indicadores financeiros centrais não mostram insuficiência atual, mas há mais "
            "reclamações do que esperaríamos para o tamanho da operação nos meses comparáveis."
        ),
        "why_it_matters": (
            "A diferença é sustentada pela evidência disponível e indica pressão de problemas "
            "reportados pelos consumidores acima da referência proporcional do mercado comparável."
        ),
        "mandatory_limit": (
            "Isso não significa que todo cliente terá problema, não identifica sozinho a causa "
            "das reclamações e não prova má qualidade em todos os produtos ou canais."
        ),
        "materiality": "conduct_attention",
    },
    "liquidity_pressure_only": {
        "public_class": "attention",
        "title": "Atenção à liquidez",
        "summary": (
            "O patrimônio ajustado cobre o capital mínimo requerido, mas o ILT está abaixo da "
            "paridade aritmética usada como referência. A Conduta não mostra pressão de "
            "reclamações acima do esperado."
        ),
        "why_it_matters": (
            "A leitura pede cautela no eixo de liquidez, mesmo sem insuficiência de capital "
            "identificada na competência de referência."
        ),
        "mandatory_limit": (
            "ILT abaixo de 1 não é, por si só, uma reprovação prudencial oficial da SUSEP e não "
            "permite concluir insolvência."
        ),
        "materiality": "liquidity_attention",
    },
    "liquidity_and_conduct_pressure": {
        "public_class": "attention",
        "title": "Atenção em liquidez e Conduta",
        "summary": (
            "O capital mínimo requerido está atendido, mas há pressão de liquidez pelo ILT e "
            "pressão de reclamações acima do esperado para o tamanho da operação."
        ),
        "why_it_matters": (
            "Dois eixos centrais independentes apresentam cautela ao mesmo tempo. A metodologia "
            "preserva ambos em vez de permitir que um apague o outro."
        ),
        "mandatory_limit": (
            "A combinação não equivale automaticamente a insolvência nem prova que todos os "
            "produtos ou atendimentos da seguradora sejam inadequados."
        ),
        "materiality": "two_attention_signals",
    },
    "capital_shortfall_without_conduct_pressure": {
        "public_class": "prudential_warning",
        "title": "Alerta prudencial de capital",
        "summary": (
            "O patrimônio líquido ajustado está abaixo do capital mínimo requerido na competência "
            "de referência. A Conduta não mostra pressão de reclamações acima do esperado."
        ),
        "why_it_matters": (
            "O CMR é uma exigência prudencial de capital. Por isso, a insuficiência observada é "
            "um alerta material e não pode ser compensada por uma leitura favorável de reclamações."
        ),
        "mandatory_limit": (
            "O dado não autoriza afirmar que a seguradora esteja insolvente ou que não consiga "
            "pagar sinistros. Ele registra insuficiência de capital em relação ao CMR na "
            "competência analisada."
        ),
        "materiality": "mandatory_prudential_capital_warning",
    },
    "capital_shortfall_and_conduct_pressure": {
        "public_class": "prudential_warning",
        "title": "Alerta de capital e Conduta",
        "summary": (
            "O patrimônio líquido ajustado está abaixo do capital mínimo requerido e também há "
            "pressão de reclamações acima do esperado para o tamanho da operação."
        ),
        "why_it_matters": (
            "Há simultaneamente um alerta prudencial material de capital e um sinal adverso de "
            "Conduta. Nenhum dos dois é reduzido ou compensado pelo outro."
        ),
        "mandatory_limit": (
            "A combinação não permite afirmar insolvência automática nem generalizar a experiência "
            "de reclamação para todos os clientes ou produtos."
        ),
        "materiality": "mandatory_prudential_capital_warning_plus_conduct_attention",
    },
}

FINANCIAL_PUBLIC = {
    "core_indicators_without_current_shortfall": "sem_insuficiencia_financeira_central_atual",
    "capital_requirement_met_with_liquidity_pressure": "atencao_liquidez",
    "capital_requirement_shortfall_observed": "alerta_prudencial_capital",
    "core_financial_signal_unavailable": "financeiro_central_indisponivel",
}

CONDUCT_PUBLIC = {
    "above_expected_with_sufficient_evidence": "acima_do_esperado",
    "below_expected_with_sufficient_evidence": "abaixo_do_esperado",
    "not_distinguishable_from_expected": "sem_diferenca_clara",
    "pressure_inconclusive_denominator_sensitivity": "conclusao_sensivel_ao_denominador",
    "pressure_unavailable_insufficient_temporal_coverage": "cobertura_temporal_insuficiente",
    "pressure_unavailable_not_comparable": "nao_comparavel_com_seguranca",
}

READINESS_PUBLIC = {
    "joint_core_conclusive": "avaliacao_conjunta_central_completa",
    "conduct_not_comparable": "conduta_nao_comparavel_com_seguranca",
    "conduct_insufficient_temporal_coverage": "conduta_com_cobertura_temporal_insuficiente",
    "conduct_denominator_sensitive": "conduta_sensivel_ao_denominador",
    "financial_core_incomplete": "financeiro_central_incompleto",
    "conduct_inconclusive_or_unavailable": "conduta_inconclusiva_ou_indisponivel",
}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _by_id(rows: list[dict[str, Any]], source: str) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        entity_id = str(row.get("entity_id") or "")
        if not entity_id:
            raise ValueError(f"{source}: entity without entity_id")
        if entity_id in result:
            raise ValueError(f"{source}: duplicate entity {entity_id}")
        result[entity_id] = row
    return result


def _conduct_qualifiers(row: dict[str, Any]) -> dict[str, str | None]:
    conduct = row.get("conduct") or {}
    if conduct.get("pressure_state") != "above_expected_with_sufficient_evidence":
        return {"persistence": None, "trend": None}

    persistence_map = {
        "persistent_above_expected": "pressao_recorrente",
        "episodic_or_sparse_above_expected": "pressao_episodica_ou_esparsa",
    }
    trend_map = {
        "deteriorating_pressure": "sinal_recente_de_piora",
        "improving_pressure": "sinal_recente_de_melhora_mas_pressao_anual_ainda_adversa",
        "no_clear_change": "sem_mudanca_recente_clara",
    }
    return {
        "persistence": persistence_map.get(str(conduct.get("persistence") or "")),
        "trend": trend_map.get(str(conduct.get("trend") or "")),
    }


def _operating_qualifier(row: dict[str, Any]) -> str | None:
    mapping = {
        "balanced_persistent": "trajetoria_operacional_equilibrada",
        "improved": "trajetoria_operacional_em_melhora",
        "recent_pressure": "pressao_operacional_recente",
        "persistent_pressure": "pressao_operacional_persistente",
        "indeterminate": "trajetoria_operacional_inconclusiva",
    }
    return mapping.get(str((row.get("financial") or {}).get("operating_context") or ""))


def _confidence_label(row: dict[str, Any]) -> str:
    mapping = {
        "established_core_history": "historico_estabelecido",
        "limited_core_history": "historico_limitado",
        "insufficient_core_evidence": "evidencia_central_insuficiente",
    }
    value = str((row.get("financial") or {}).get("evidence_confidence") or "")
    return mapping.get(value, "confianca_nao_classificada")


def _available_alerts(row: dict[str, Any]) -> list[dict[str, str]]:
    signature = row.get("adverse_signature") or {}
    alerts: list[dict[str, str]] = []
    if bool(signature.get("capital_shortfall")):
        alerts.append(
            {
                "kind": "prudential_capital_warning",
                "title": "Alerta prudencial de capital",
                "text": (
                    "O patrimônio líquido ajustado está abaixo do capital mínimo requerido na "
                    "competência de referência. A avaliação conjunta pode estar incompleta, mas "
                    "este alerta financeiro disponível não deve ser ocultado."
                ),
            }
        )
    if bool(signature.get("liquidity_pressure")):
        alerts.append(
            {
                "kind": "liquidity_attention",
                "title": "Atenção à liquidez",
                "text": (
                    "O ILT está abaixo da paridade aritmética usada como referência. A avaliação "
                    "conjunta pode estar incompleta, mas o sinal de liquidez disponível permanece visível."
                ),
            }
        )
    if bool(signature.get("conduct_above_expected")):
        alerts.append(
            {
                "kind": "conduct_attention",
                "title": "Atenção à Conduta",
                "text": (
                    "Há pressão de reclamações acima do esperado para o tamanho da operação nos "
                    "meses comparáveis."
                ),
            }
        )
    return alerts


def _incomplete_public_block(readiness: str) -> dict[str, str]:
    explanations = {
        "conduct_not_comparable": (
            "Há dados de Conduta, mas não há numerador e denominador comparáveis suficientes para "
            "calcular a pressão sem inventar atribuições."
        ),
        "conduct_insufficient_temporal_coverage": (
            "Ainda não há meses comparáveis suficientes para uma conclusão anual segura de "
            "pressão de reclamações."
        ),
        "conduct_denominator_sensitive": (
            "A conclusão de Conduta muda conforme a medida econômica usada para representar o "
            "tamanho da operação; por isso, não apresentamos direção conjunta."
        ),
        "financial_core_incomplete": (
            "O núcleo Financeiro não possui evidência suficiente para formar uma conclusão conjunta segura."
        ),
        "conduct_inconclusive_or_unavailable": (
            "A evidência de Conduta não sustenta uma conclusão conjunta segura nesta janela."
        ),
    }
    return {
        "public_class": "evidence_incomplete",
        "title": "Avaliação conjunta incompleta",
        "summary": (
            "Ainda não há evidência comparável suficiente para formar uma conclusão conjunta segura "
            "entre Financeiro e Conduta."
        ),
        "why_it_matters": explanations.get(
            readiness,
            "A ferramenta preserva os dados disponíveis e não transforma ausência de evidência em desempenho.",
        ),
        "mandatory_limit": (
            "Este estado não é uma avaliação negativa nem neutra da seguradora. Os sinais disponíveis "
            "continuam visíveis separadamente."
        ),
        "materiality": "evidence_constraint_not_performance",
    }


def _validate_stage2_alignment(
    stage1_row: dict[str, Any], stage2_row: dict[str, Any]
) -> str | None:
    signature = stage1_row.get("core_signature")
    expected = STATE_BY_SIGNATURE.get(str(signature or ""))
    actual = stage2_row.get("matrix_state")
    if actual != expected:
        raise ValueError(
            f"matrix mismatch for {stage1_row.get('entity_id')}: expected={expected} actual={actual}"
        )
    return expected


def build_semantic_contract(
    stage1: dict[str, Any], stage2: dict[str, Any]
) -> dict[str, Any]:
    if stage1.get("status") != "cross_pillar_calibration_stage_1_diagnostic":
        raise ValueError("unexpected Stage 1 status")
    if stage2.get("status") != "cross_pillar_architecture_stage_2_experiment":
        raise ValueError("unexpected Stage 2 status")
    if stage1.get("scoring") != "forbidden_in_this_artifact":
        raise ValueError("Stage 1 scoring must remain forbidden")
    if stage2.get("scoring") != "forbidden_in_this_artifact":
        raise ValueError("Stage 2 scoring must remain forbidden")
    if stage2.get("ranking") != "forbidden_in_this_artifact":
        raise ValueError("Stage 2 ranking must remain forbidden")

    stage1_population = stage1.get("population") or {}
    regulatory_universe = int(stage1_population.get("regulatory_universe") or 0)
    if regulatory_universe < MINIMUM_UNIVERSE_SANITY:
        raise ValueError(
            f"unexpectedly small semantic regulatory universe: {regulatory_universe}"
        )

    stage1_by_id = _by_id(stage1.get("entities") or [], "stage1")
    stage2_by_id = _by_id(stage2.get("entities") or [], "stage2")
    if len(stage1_by_id) != regulatory_universe:
        raise ValueError("Stage 1 entity population differs from its regulatory universe")
    if len(stage2_by_id) != regulatory_universe:
        raise ValueError("Stage 2 entity population differs from Stage 1 regulatory universe")
    if set(stage1_by_id) != set(stage2_by_id):
        raise ValueError("Stage 1 and Stage 2 entity populations differ")

    stage2_population = stage2.get("population") or {}
    if int(stage2_population.get("regulatory_universe") or 0) != regulatory_universe:
        raise ValueError("Stage 2 population metadata differs from Stage 1")

    rows = []
    state_counts: Counter[str] = Counter()
    class_counts: Counter[str] = Counter()
    readiness_counts: Counter[str] = Counter()
    available_alert_counts: Counter[str] = Counter()
    conduct_detail_counts: Counter[str] = Counter()
    qualifier_counts: Counter[str] = Counter()
    complete_count = 0
    incomplete_available_alert_counts: Counter[str] = Counter()

    for entity_id in sorted(stage1_by_id):
        source = stage1_by_id[entity_id]
        architecture = stage2_by_id[entity_id]
        matrix_state = _validate_stage2_alignment(source, architecture)
        readiness = str(source.get("joint_evidence_readiness") or "")
        is_complete = readiness == "joint_core_conclusive" and matrix_state is not None
        if readiness == "joint_core_conclusive" and matrix_state is None:
            raise ValueError(f"joint conclusive entity without matrix state: {entity_id}")
        if readiness != "joint_core_conclusive" and matrix_state is not None:
            raise ValueError(f"incomplete entity with matrix state: {entity_id}")

        if is_complete:
            public = dict(STATE_CONTRACT[matrix_state])
            complete_count += 1
            state_counts[matrix_state] += 1
        else:
            public = _incomplete_public_block(readiness)
            state_counts["evidence_incomplete_for_joint_assessment"] += 1

        class_counts[public["public_class"]] += 1
        readiness_counts[readiness] += 1

        conduct_state = str((source.get("conduct") or {}).get("pressure_state") or "")
        conduct_detail = CONDUCT_PUBLIC.get(conduct_state, "conduta_nao_classificada")
        conduct_detail_counts[conduct_detail] += 1

        conduct_qualifiers = _conduct_qualifiers(source)
        for value in conduct_qualifiers.values():
            if value:
                qualifier_counts[value] += 1

        alerts = _available_alerts(source)
        for alert in alerts:
            available_alert_counts[alert["kind"]] += 1
            if not is_complete:
                incomplete_available_alert_counts[alert["kind"]] += 1

        if conduct_state != "above_expected_with_sufficient_evidence" and any(
            conduct_qualifiers.values()
        ):
            raise ValueError(f"adverse Conduct qualifier leaked to non-adverse state: {entity_id}")

        has_capital_shortfall = bool(
            (source.get("adverse_signature") or {}).get("capital_shortfall")
        )
        if has_capital_shortfall and not any(
            alert["kind"] == "prudential_capital_warning" for alert in alerts
        ):
            raise ValueError(f"capital shortfall hidden from public alerts: {entity_id}")

        if (
            is_complete
            and matrix_state.startswith("capital_shortfall")
            and public["public_class"] != "prudential_warning"
        ):
            raise ValueError(f"capital state without prudential warning class: {entity_id}")
        if (
            is_complete
            and matrix_state == "no_current_core_adverse_signal"
            and public["public_class"] != "favorable_reading"
        ):
            raise ValueError(f"favorable matrix state mislabeled: {entity_id}")

        rows.append(
            {
                "entity_id": entity_id,
                "fip_code": source.get("fip_code"),
                "legal_name": source.get("legal_name"),
                "assessment_completeness": (
                    "joint_core_complete" if is_complete else "joint_core_incomplete"
                ),
                "semantic_public_assessment_supported": is_complete,
                "formal_assessment_gate_effect": "none_contract_only",
                "formal_ranking_gate_effect": "none_contract_only",
                "matrix_state": matrix_state,
                "public_assessment": public,
                "available_pillar_reading": {
                    "financial": FINANCIAL_PUBLIC.get(
                        str((source.get("financial") or {}).get("core_state") or ""),
                        "financeiro_nao_classificado",
                    ),
                    "financial_confidence": _confidence_label(source),
                    "operating_context": _operating_qualifier(source),
                    "conduct": conduct_detail,
                },
                "qualifiers": {
                    "conduct_persistence": conduct_qualifiers["persistence"],
                    "conduct_trend": conduct_qualifiers["trend"],
                    "operating_context": _operating_qualifier(source),
                    "financial_confidence": _confidence_label(source),
                },
                "mandatory_available_alerts": alerts,
                "evidence_readiness": READINESS_PUBLIC.get(
                    readiness, "evidencia_nao_classificada"
                ),
                "conduct_detail_preserved_below_expected_vs_no_clear": True,
            }
        )

    architecture_counts = (stage2.get("matrix_architecture") or {}).get("state_counts") or {}
    if dict(state_counts) != architecture_counts:
        raise ValueError(
            "semantic state counts diverge from Stage 2 architecture: "
            f"semantic={dict(state_counts)} stage2={architecture_counts}"
        )

    coverage = stage2.get("coverage_constraint") or {}
    incomplete_count = regulatory_universe - complete_count
    if complete_count != int(coverage.get("joint_conclusive_entities") or 0):
        raise ValueError("semantic complete count diverges from Stage 2 coverage")
    if incomplete_count != int(coverage.get("joint_incomplete_entities") or 0):
        raise ValueError("semantic incomplete count diverges from Stage 2 coverage")
    if complete_count != int(stage1_population.get("joint_core_conclusive") or 0):
        raise ValueError("semantic complete count diverges from Stage 1 population")
    if incomplete_count != int(stage1_population.get("joint_core_not_conclusive") or 0):
        raise ValueError("semantic incomplete count diverges from Stage 1 population")

    return {
        "artifact": "v2_cross_pillar_assessment_semantic_contract",
        "generated_at": _utc_now(),
        "version": VERSION,
        "status": "cross_pillar_assessment_semantic_contract_closed",
        "scoring": "forbidden_in_this_artifact",
        "ranking": "forbidden_in_this_artifact",
        "formal_assessment_eligibility": "not_opened_by_this_contract",
        "formal_ranking_eligibility": "not_opened_by_this_contract",
        "human_model": {
            "primary_question": (
                "O que os sinais disponíveis permitem dizer ao consumidor sobre esta seguradora, "
                "em linguagem útil, sem transformar evidência parcial em garantia?"
            ),
            "communication_order": [
                "leitura_geral",
                "sinais_encontrados",
                "por_que_importa",
                "qualificadores",
                "numeros_e_metodologia",
                "limites_da_conclusao",
                "confianca_e_cobertura",
            ],
            "rule": (
                "resultado favoravel pode ser reconhecido dentro do escopo avaliado; alerta material "
                "nao pode ser compensado; ausencia de evidencia nunca vira desempenho"
            ),
        },
        "semantic_contract": {
            "state_contract": STATE_CONTRACT,
            "incomplete_state_title": "Avaliação conjunta incompleta",
            "capital_shortfall_mandatory_warning": True,
            "incomplete_joint_assessment_does_not_hide_available_alerts": True,
            "conduct_adverse_qualifiers_only_when_annual_pressure_above_expected": True,
            "below_expected_detail_preserved_without_quality_bonus": True,
            "missingness_separate_from_performance": True,
            "no_cross_pillar_weights": True,
            "no_total_order": True,
        },
        "population": {
            "regulatory_universe": regulatory_universe,
            "semantic_public_assessment_supported": complete_count,
            "joint_core_incomplete": incomplete_count,
        },
        "diagnostics": {
            "state_counts": dict(state_counts),
            "public_class_counts": dict(sorted(class_counts.items())),
            "readiness_counts": dict(sorted(readiness_counts.items())),
            "conduct_detail_counts": dict(sorted(conduct_detail_counts.items())),
            "qualifier_counts": dict(sorted(qualifier_counts.items())),
            "mandatory_available_alert_counts": dict(
                sorted(available_alert_counts.items())
            ),
            "incomplete_available_alert_counts": dict(
                sorted(incomplete_available_alert_counts.items())
            ),
        },
        "closure_decision": {
            "semantic_contract_closed": True,
            "individual_joint_assessment_semantically_supported_for_complete_subset": True,
            "formal_assessment_eligibility_gate_opened": False,
            "formal_ranking_eligibility_gate_opened": False,
            "full_market_ranking_supported": False,
            "next_gate": "assessment_eligibility_contract",
        },
        "entities": rows,
    }


def main() -> None:
    stage1 = json.loads(STAGE1_PATH.read_text(encoding="utf-8"))
    stage2 = json.loads(STAGE2_PATH.read_text(encoding="utf-8"))
    payload = build_semantic_contract(stage1, stage2)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "artifact": payload["artifact"],
                "status": payload["status"],
                "population": payload["population"],
                "diagnostics": payload["diagnostics"],
                "closure_decision": payload["closure_decision"],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    print(f"written to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
