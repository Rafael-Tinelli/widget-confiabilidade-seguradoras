from __future__ import annotations

import json
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

LIFECYCLE_PATH = Path("data/derived/v2/entity_lifecycle_relationship_inventory.json")
EXPLORER_PATH = Path("data/derived/v2/public/insurer_explorer.json")
SANDBOX_CONDUCT_PATH = Path("data/derived/v2/sandbox_brand_conduct_evidence.json")
CONDUCT_RELATIONSHIPS_PATH = Path(
    "data/reference/v2/conduct_subject_relationships.json"
)
OUTPUT_PATH = Path("data/derived/v2/public_search_profile_contract.json")
PUBLIC_DIR = Path("data/derived/v2/public")
VERSION = "2.0-public-search-profile-contract-1"


class PublicSearchProfileContractError(RuntimeError):
    """Raised when public search/profile inputs cannot be reconciled safely."""


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _normalize_search(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = "".join(
        char
        for char in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(char)
    )
    return re.sub(r"[^a-z0-9]+", " ", text).strip()


def _profile_file_key(profile_id: str) -> str:
    key = re.sub(r"[^a-zA-Z0-9]+", "--", profile_id).strip("-").lower()
    if not key:
        raise PublicSearchProfileContractError(f"invalid profile id: {profile_id!r}")
    return key


def _metric(
    value: Any,
    *,
    meaning: str,
    availability: str | None = None,
    public_use: str = "displayable",
    zero_semantics: str | None = None,
) -> dict[str, Any]:
    resolved = availability or ("unavailable" if value is None else "available")
    if resolved == "unavailable" and value is not None:
        raise PublicSearchProfileContractError(
            "unavailable metric cannot carry a non-null value"
        )
    return {
        "value": value,
        "availability": resolved,
        "public_use": public_use,
        "zero_semantics": zero_semantics,
        "meaning": meaning,
    }


def _entity_name(entity: dict[str, Any] | None) -> str:
    if not entity:
        return "Entidade"
    return str(
        entity.get("display_name")
        or entity.get("legal_name")
        or entity.get("entity_id")
        or "Entidade"
    )


def _regulatory_label(entity: dict[str, Any]) -> str:
    entity_type = str(entity.get("entity_type") or "unknown")
    regime = str(entity.get("regulatory_regime") or "unknown")
    state = str((entity.get("query_context") or {}).get("entity_state") or "")

    if state == "historical_incorporated_entity":
        return "Entidade histórica incorporada"
    if state == "historical_closed_entity":
        return "Entidade jurídica histórica"
    if entity_type == "sandbox_participant" or regime == "sandbox":
        return "Participante do Sandbox regulatório"
    if regime == "special":
        return "Entidade em regime especial"
    if entity_type == "insurer":
        return "Seguradora"
    if entity_type == "open_pension_entity":
        return "Entidade aberta de previdência"
    if entity_type == "capitalization_company":
        return "Sociedade de capitalização"
    if entity_type in {
        "local_reinsurer",
        "admitted_reinsurer",
        "occasional_reinsurer",
        "reinsurance_broker",
    }:
        return "Entidade do mercado de resseguros"
    return "Entidade identificada no mercado supervisionado"


def _entity_intro(entity: dict[str, Any], by_id: dict[str, dict[str, Any]]) -> str:
    name = _entity_name(entity)
    context = entity.get("query_context") or {}
    state = str(context.get("entity_state") or "")
    entity_type = str(entity.get("entity_type") or "unknown")
    regime = str(entity.get("regulatory_regime") or "unknown")

    if state == "historical_incorporated_entity":
        successor = by_id.get(str(context.get("successor_entity_id") or ""))
        if successor:
            return (
                f"{name} é uma pessoa jurídica histórica. O cadastro de relações "
                f"verificadas aponta sucessão até {_entity_name(successor)}. A análise "
                "atual deve ser consultada na entidade sucessora e não transferida "
                "retroativamente para esta empresa histórica."
            )
        return (
            f"{name} é uma pessoa jurídica histórica incorporada e não recebe uma "
            "avaliação atual."
        )
    if state == "historical_closed_entity":
        return (
            f"{name} aparece como pessoa jurídica histórica/baixada no cadastro jurídico "
            "do projeto e não deve ser confundida com uma seguradora atual."
        )
    if entity_type == "sandbox_participant" or regime == "sandbox":
        return (
            f"{name} aparece no snapshot do projeto como participante do Sandbox "
            "regulatório da SUSEP. Sandbox é um regime experimental e não equivale ao "
            "universo de seguradoras ordinárias usado nas comparações da v2."
        )
    if regime == "special":
        return (
            f"{name} aparece em condição regulatória especial. Permanece pesquisável, "
            "mas fora da comparação ordinária da metodologia v2."
        )
    if entity_type == "open_pension_entity":
        return (
            f"{name} é identificada como entidade aberta de previdência. Permanece "
            "pesquisável, mas não é tratada como seguradora ordinária nesta comparação."
        )
    if entity_type == "capitalization_company":
        return (
            f"{name} é identificada como sociedade de capitalização. Permanece "
            "pesquisável, mas não entra no universo de seguradoras ordinárias."
        )
    if entity_type in {
        "local_reinsurer",
        "admitted_reinsurer",
        "occasional_reinsurer",
        "reinsurance_broker",
    }:
        return (
            f"{name} pertence ao mercado de resseguros, papel diferente do de uma "
            "seguradora considerada pelo comparador ordinário."
        )
    if entity_type == "insurer":
        return (
            f"{name} é identificada como seguradora no cadastro regulatório usado pela "
            "v2. Cada sinal só é apresentado quando sua evidência sustenta a afirmação."
        )
    return (
        f"{name} foi identificada nas fontes do projeto, mas não é classificada como "
        "seguradora ordinária no universo comparável."
    )


def _capital_public(capital: dict[str, Any]) -> dict[str, Any]:
    state = str(capital.get("state") or "")
    value = capital.get("pla_cmr_ratio")
    technical = {
        "label": "PLA/CMR",
        "ratio": _metric(
            value if state != "capital_signal_unavailable" else None,
            meaning=(
                "Relação entre Patrimônio Líquido Ajustado e Capital Mínimo Requerido "
                "na competência de referência."
            ),
        ),
    }
    if state == "capital_meets_or_exceeds_cmr":
        return {
            "state": state,
            "tone": "favorable",
            "plain_language": (
                "Na competência analisada, o patrimônio ajustado alcança o capital "
                "mínimo exigido."
            ),
            "technical": technical,
        }
    if state == "capital_below_cmr":
        return {
            "state": state,
            "tone": "adverse",
            "plain_language": (
                "Na competência analisada, o patrimônio ajustado ficou abaixo do capital "
                "mínimo exigido. É um alerta prudencial relevante, mas não permite "
                "concluir sozinho que a empresa seja insolvente."
            ),
            "technical": technical,
        }
    technical["ratio"] = _metric(
        None,
        meaning="Relação entre Patrimônio Líquido Ajustado e Capital Mínimo Requerido.",
    )
    return {
        "state": state or "capital_signal_unavailable",
        "tone": "unknown",
        "plain_language": (
            "Não há dado utilizável suficiente para concluir a situação de capital "
            "nesta competência."
        ),
        "technical": technical,
    }


def _liquidity_public(liquidity: dict[str, Any]) -> dict[str, Any]:
    state = str(liquidity.get("state") or "")
    value = liquidity.get("value")
    technical = {
        "label": "ILT",
        "ratio": _metric(
            value if state != "ilt_signal_unavailable" else None,
            meaning=(
                "Indicador de Liquidez Total. A referência 1,0 é paridade aritmética da "
                "metodologia, não limite prudencial oficial da SUSEP."
            ),
        ),
    }
    if state == "ilt_at_or_above_arithmetic_parity":
        return {
            "state": state,
            "tone": "favorable",
            "plain_language": (
                "O indicador de liquidez usado pela metodologia não mostrou pressão "
                "segundo sua referência aritmética na competência analisada."
            ),
            "technical": technical,
        }
    if state == "ilt_below_arithmetic_parity":
        return {
            "state": state,
            "tone": "caution",
            "plain_language": (
                "O indicador de liquidez usado pela metodologia ficou abaixo de sua "
                "referência aritmética e merece atenção. Essa referência não é um limite "
                "prudencial oficial da SUSEP."
            ),
            "technical": technical,
        }
    technical["ratio"] = _metric(
        None,
        meaning="Indicador de Liquidez Total; dado indisponível para esta leitura.",
    )
    return {
        "state": state or "ilt_signal_unavailable",
        "tone": "unknown",
        "plain_language": (
            "Não há dado utilizável suficiente para concluir a leitura de liquidez "
            "nesta competência."
        ),
        "technical": technical,
    }


def _conduct_relationships(
    entity: dict[str, Any],
    registry: dict[str, Any],
    *,
    by_cnpj: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    cnpj = str(entity.get("cnpj") or "")
    output: list[dict[str, Any]] = []
    for relation in registry.get("relationships") or []:
        subject_cnpj = str(relation.get("subject_cnpj") or "")
        target_cnpjs = [str(value or "") for value in relation.get("target_cnpjs") or []]
        if cnpj != subject_cnpj and cnpj not in target_cnpjs:
            continue
        subject = by_cnpj.get(subject_cnpj)
        targets = [by_cnpj.get(value) for value in target_cnpjs]
        output.append(
            {
                "relationship_id": relation.get("relationship_id"),
                "relationship_type": relation.get("relationship_type"),
                "role": "subject" if cnpj == subject_cnpj else "target",
                "subject": (
                    {
                        "profile_id": f"entity:{subject['entity_id']}",
                        "name": _entity_name(subject),
                        "cnpj": subject.get("cnpj"),
                    }
                    if subject
                    else {"profile_id": None, "name": None, "cnpj": subject_cnpj}
                ),
                "targets": [
                    {
                        "profile_id": f"entity:{target['entity_id']}",
                        "name": _entity_name(target),
                        "cnpj": target.get("cnpj"),
                    }
                    for target in targets
                    if target
                ],
                "effective_from": relation.get("effective_from"),
                "pressure_policy": relation.get("pressure_policy"),
                "reconciliation_state": relation.get("reconciliation_state"),
                "evidence": relation.get("evidence") or [],
            }
        )
    return output


def _conduct_public(
    conduct: dict[str, Any],
    relationship_context: list[dict[str, Any]],
) -> dict[str, Any]:
    state = str(conduct.get("state") or "")
    reason = conduct.get("reason_code")
    language = {
        "above_expected_with_sufficient_evidence": (
            "Há mais reclamações do que esperaríamos para o tamanho da operação nos "
            "meses comparáveis, e a diferença é sustentada pela evidência disponível."
        ),
        "below_expected_with_sufficient_evidence": (
            "Há menos reclamações do que esperaríamos para o tamanho da operação nos "
            "meses comparáveis. Isso, isoladamente, não prova melhor atendimento ou "
            "maior qualidade."
        ),
        "not_distinguishable_from_expected": (
            "Os dados não mostram diferença suficientemente clara em relação ao esperado "
            "para o tamanho da operação."
        ),
        "pressure_inconclusive_denominator_sensitivity": (
            "A conclusão muda conforme a medida econômica usada para representar o "
            "tamanho da operação; por isso, não apresentamos uma conclusão direcional."
        ),
        "pressure_unavailable_insufficient_temporal_coverage": (
            "Ainda não há meses comparáveis suficientes para uma conclusão anual sobre "
            "a pressão de reclamações."
        ),
        "pressure_unavailable_not_comparable": (
            "Há dados de reclamações, mas não há numerador e denominador comparáveis "
            "suficientes para calcular pressão sem inventar atribuições."
        ),
    }.get(
        state,
        "A evidência de reclamações não sustenta uma conclusão comparativa neste registro.",
    )
    if reason == "brand_specific_exposure_required":
        relation = next(
            (
                item
                for item in relationship_context
                if item.get("relationship_type")
                == "consumer_subject_single_risk_carrier"
                and item.get("role") == "subject"
            ),
            None,
        )
        if relation:
            names = ", ".join(
                str(item.get("name") or item.get("profile_id"))
                for item in relation.get("targets") or []
            )
            language = (
                "Há reclamações registradas contra esta empresa, mas existe relação "
                f"documentada com {names or 'outra seguradora'} como responsável pelo "
                "risco dos produtos considerados. Não usamos automaticamente toda a "
                "produção da outra seguradora para dimensionar essas reclamações, pois "
                "isso poderia distorcer a comparação."
            )
    tone = {
        "above_expected_with_sufficient_evidence": "adverse",
        "below_expected_with_sufficient_evidence": "neutral",
        "not_distinguishable_from_expected": "neutral",
    }.get(state, "unknown")
    return {
        "state": state or None,
        "tone": tone,
        "plain_language": language,
        "comparability_state": conduct.get("comparability_state"),
        "reason_code": reason,
        "technical": {
            "observed_complaints_12m": _metric(
                conduct.get("observed_complaints_12m"),
                meaning=(
                    "Reclamações observadas no Consumer.gov na janela preservada. Zero "
                    "observado não é tratado como selo de boa Conduta."
                ),
                zero_semantics="observed_zero_is_not_a_favorable_finding",
            ),
            "expected_complaints_12m": _metric(
                conduct.get("expected_complaints_12m"),
                meaning=(
                    "Referência estatística proporcional ao tamanho econômico nos meses "
                    "comparáveis; não é quantidade ideal de reclamações."
                ),
            ),
            "observed_expected_ratio": _metric(
                conduct.get("pressure_ratio"),
                meaning=(
                    "Razão observadas/esperadas somente quando população, exposição e "
                    "período são comparáveis."
                ),
            ),
            "comparable_months": _metric(
                conduct.get("comparable_months"),
                meaning="Quantidade de meses utilizáveis na comparação anual.",
            ),
            "persistence": conduct.get("persistence"),
            "trend": conduct.get("trend"),
        },
    }


def _assessment_headline(assessment: dict[str, Any], conduct: dict[str, Any]) -> str:
    if not assessment.get("eligible"):
        if conduct.get("state") == "pressure_unavailable_not_comparable":
            return "Há informações úteis, mas a avaliação conjunta ainda é incompleta"
        return "A avaliação conjunta ainda é incompleta"
    public_class = str(assessment.get("public_class") or "")
    if public_class == "favorable_reading":
        return "Os sinais centrais avaliados não mostram alerta conjunto relevante"
    if public_class == "prudential_warning":
        return "Há um alerta prudencial material na competência analisada"
    if public_class == "attention":
        return "Há sinais que merecem atenção"
    return str(assessment.get("title") or "Leitura conjunta disponível")


def _ordinary_assessment(
    explorer: dict[str, Any],
    relationship_context: list[dict[str, Any]],
) -> dict[str, Any]:
    assessment = explorer.get("assessment") or {}
    financial = explorer.get("financial") or {}
    conduct = explorer.get("conduct") or {}
    market = explorer.get("market_context") or {}
    premium = market.get("insurance_premium_direct_12m")
    comparable = conduct.get("comparability_state") == "direct_one_to_one_candidate"

    if premium is None:
        operation = _metric(
            None,
            meaning="Prêmio direto de seguros na janela de 12 meses.",
        )
    elif comparable and isinstance(premium, (int, float)) and premium > 0:
        operation = _metric(
            premium,
            meaning=(
                "Prêmio direto de seguros na janela de 12 meses. Mede volume econômico, "
                "não qualidade, número de clientes ou número de apólices."
            ),
            zero_semantics="zero_would_not_be_a_quality_finding",
        )
    else:
        operation = _metric(
            premium,
            meaning=(
                "Valor bruto de prêmio direto preservado da fonte. Neste perfil ele não "
                "é adequado para representar o tamanho da operação na comparação de Conduta."
            ),
            public_use="do_not_render_as_operation_size",
            zero_semantics=(
                "literal_source_zero_must_not_be_presented_as_zero_sized_business"
            ),
        )

    return {
        "availability": "available" if assessment.get("eligible") else "incomplete",
        "headline": _assessment_headline(assessment, conduct),
        "summary": assessment.get("summary"),
        "why_it_matters": assessment.get("why_it_matters"),
        "mandatory_limit": assessment.get("mandatory_limit"),
        "matrix_state": assessment.get("matrix_state"),
        "public_class": assessment.get("public_class"),
        "financial": {
            "reference_period": financial.get("reference_period"),
            "capital": _capital_public(financial.get("capital") or {}),
            "liquidity": _liquidity_public(financial.get("liquidity") or {}),
            "operating_context": financial.get("operating_context"),
            "evidence_confidence": financial.get("evidence_confidence"),
        },
        "conduct": _conduct_public(conduct, relationship_context),
        "operation_context": {
            "insurance_premium_direct_12m": operation,
            "public_note": (
                "Prêmio direto só aparece como tamanho da operação quando a relação entre "
                "quem recebe reclamações e a exposição econômica é comparável."
            ),
        },
    }


def _same_group_context(
    entity: dict[str, Any],
    by_id: dict[str, dict[str, Any]],
    group_by_id: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    group = entity.get("economic_group") or {}
    group_id = str(group.get("group_id") or "")
    if not group_id:
        return None
    group_row = group_by_id.get(group_id) or {}
    peers = []
    for entity_id in group_row.get("member_entity_ids") or []:
        if entity_id == entity.get("entity_id"):
            continue
        peer = by_id.get(str(entity_id))
        if peer:
            peers.append(
                {
                    "profile_id": f"entity:{peer['entity_id']}",
                    "entity_id": peer["entity_id"],
                    "name": _entity_name(peer),
                    "entity_type": peer.get("entity_type"),
                    "regulatory_regime": peer.get("regulatory_regime"),
                }
            )
    return {
        "group_id": group_id,
        "group_name": group.get("group_name"),
        "observed_period": group.get("observed_period"),
        "source": group.get("source"),
        "related_entities": sorted(peers, key=lambda row: str(row["name"])),
        "public_note": (
            "O grupo econômico é contexto observado na fonte SUSEP. A coincidência de "
            "grupo não prova, sozinha, incorporação, sucessão, joint venture ou "
            "transferência de carteira."
        ),
    }


def _direct_relationships(
    entity: dict[str, Any],
    by_id: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    output = []
    for relation in entity.get("relationships") or []:
        target_id = relation.get("target_entity_id")
        if not target_id:
            continue
        target = by_id.get(str(target_id))
        output.append(
            {
                "relationship_type": relation.get("relationship_type"),
                "target_profile_id": f"entity:{target_id}",
                "target_name": _entity_name(target) if target else None,
                "effective_date": relation.get("effective_date"),
                "evidence": relation.get("evidence") or {},
            }
        )
    return output


def _incoming_brands(entity_id: str, brands: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output = []
    for brand in brands:
        for relation in brand.get("relationships") or []:
            if relation.get("target_entity_id") != entity_id:
                continue
            output.append(
                {
                    "profile_id": str(brand.get("brand_id")),
                    "brand_id": brand.get("brand_id"),
                    "name": brand.get("name"),
                    "aliases": brand.get("aliases") or [],
                    "relationship_type": relation.get("relationship_type"),
                    "status": relation.get("status"),
                    "scope": relation.get("scope"),
                    "evidence": relation.get("evidence") or {},
                }
            )
    return sorted(output, key=lambda row: str(row.get("name") or ""))


def _sandbox_film(film: dict[str, Any] | None) -> dict[str, Any] | None:
    if not film:
        return None
    satisfaction = film.get("satisfaction_trend") or {}
    return {
        "history_state": film.get("history_state"),
        "months_observed": _metric(
            film.get("months_observed"),
            meaning="Quantidade de meses preservados no filme de Conduta Sandbox.",
        ),
        "months_with_complaints": _metric(
            film.get("months_with_complaints"),
            meaning="Meses da janela em que houve reclamações observadas.",
            zero_semantics="observed_zero_is_not_a_favorable_finding",
        ),
        "satisfaction_trend": {
            "direction": satisfaction.get("direction"),
            "early_half_average": _metric(
                satisfaction.get("early_half_average"),
                meaning="Média de satisfação na primeira metade da janela.",
            ),
            "early_half_sample": _metric(
                satisfaction.get("early_half_sample"),
                meaning="Quantidade de avaliações na primeira metade da janela.",
                zero_semantics="zero_sample_means_no_basis_for_average",
            ),
            "recent_half_average": _metric(
                satisfaction.get("recent_half_average"),
                meaning="Média de satisfação na metade recente da janela.",
            ),
            "recent_half_sample": _metric(
                satisfaction.get("recent_half_sample"),
                meaning="Quantidade de avaliações na metade recente da janela.",
                zero_semantics="zero_sample_means_no_basis_for_average",
            ),
        },
        "public_limit": (
            "O filme Sandbox é contexto longitudinal experimental. Ele não produz score, "
            "ranking ou pressão proporcional."
        ),
    }


def _sandbox_metrics(totals: dict[str, Any]) -> dict[str, Any]:
    return {
        "complaints": _metric(
            totals.get("complaints"),
            meaning="Reclamações observadas contra a entidade Sandbox na janela preservada.",
            zero_semantics="observed_zero_is_not_a_favorable_finding",
        ),
        "responded": _metric(
            totals.get("responded"),
            meaning="Reclamações com resposta registrada; resposta não prova solução.",
        ),
        "response_rate": _metric(
            totals.get("response_rate"),
            meaning="Taxa de resposta observada; não equivale a resolução.",
        ),
        "finalized": _metric(
            totals.get("finalized"),
            meaning="Reclamações finalizadas; finalização não prova solução.",
        ),
        "finalized_rate": _metric(
            totals.get("finalized_rate"),
            meaning="Taxa de finalização observada; não equivale a resolução.",
        ),
        "satisfaction_count": _metric(
            totals.get("satisfaction_count"),
            meaning="Quantidade de avaliações de satisfação preservadas.",
        ),
        "average_satisfaction": _metric(
            totals.get("average_satisfaction"),
            meaning="Média de satisfação entre quem avaliou, dependente da amostra.",
        ),
    }


def _sandbox_conduct_for_entity(
    entity: dict[str, Any],
    sandbox_payload: dict[str, Any],
) -> dict[str, Any] | None:
    entity_id = str(entity.get("entity_id") or "")
    cnpj = str(entity.get("cnpj") or "")
    carrier = next(
        (
            row
            for row in sandbox_payload.get("carriers") or []
            if str(row.get("entity_id") or "") == entity_id
            or (cnpj and str(row.get("cnpj") or "") == cnpj)
        ),
        None,
    )
    if carrier is None:
        return None
    metrics = _sandbox_metrics(carrier.get("totals") or {})
    metrics["relative_pressure"] = _metric(
        None,
        meaning=(
            "A metodologia atual não calcula pressão proporcional para o Sandbox neste "
            "artifact."
        ),
        availability="unavailable",
        public_use="not_applicable_to_sandbox_artifact",
    )
    return {
        "availability": "available",
        "source_role": "sandbox_carrier_conduct_context",
        "plain_language": (
            "Há evidência de reclamações do Consumer.gov associada a esta entidade "
            "Sandbox. Esses dados são contexto de Conduta e não recebem a mesma razão de "
            "pressão usada para seguradoras ordinárias."
        ),
        "provider_labels": carrier.get("consumer_gov_provider_labels_observed") or [],
        "metrics": metrics,
        "trajectory_context": _sandbox_film(carrier.get("film")),
    }


def _sandbox_brand_context(
    brand_id: str,
    sandbox_payload: dict[str, Any],
) -> dict[str, Any] | None:
    row = next(
        (
            item
            for item in sandbox_payload.get("brands") or []
            if str(item.get("brand_id") or "") == brand_id
        ),
        None,
    )
    if row is None:
        return None
    summary = row.get("carrier_conduct_summary") or {}
    return {
        "availability": "available",
        "risk_carrier_profile_id": f"entity:{row.get('risk_carrier_entity_id')}",
        "risk_carrier_name": row.get("risk_carrier_name"),
        "risk_carrier_cnpj": row.get("risk_carrier_cnpj"),
        "product_scope": row.get("product_scope"),
        "conduct_scope": row.get("conduct_scope"),
        "attribution_note": row.get("attribution_note"),
        "plain_language": (
            f"O Consumer.gov registra esta evidência contra "
            f"{row.get('risk_carrier_name') or 'a seguradora Sandbox vinculada'}, entidade "
            "regulada relacionada à marca. O dado é contexto da relação verificada e não "
            "reclamação exclusiva da marca."
        ),
        "metrics": _sandbox_metrics(summary),
        "trajectory_context": _sandbox_film(summary.get("film")),
        "evidence": row.get("evidence") or [],
    }


def _brand_profile(
    brand: dict[str, Any],
    by_id: dict[str, dict[str, Any]],
    sandbox_payload: dict[str, Any],
) -> dict[str, Any]:
    relationships = []
    target_names = []
    for relation in brand.get("relationships") or []:
        target_id = str(relation.get("target_entity_id") or "")
        target = by_id.get(target_id)
        target_name = _entity_name(target) if target else target_id
        target_names.append(target_name)
        relationships.append(
            {
                "relationship_type": relation.get("relationship_type"),
                "target_profile_id": f"entity:{target_id}",
                "target_entity_id": target_id,
                "target_name": target_name,
                "status": relation.get("status"),
                "scope": relation.get("scope"),
                "evidence": relation.get("evidence") or {},
            }
        )
    targets = ", ".join(target_names) if target_names else "nenhuma entidade resolvida"
    return {
        "profile_id": str(brand["brand_id"]),
        "profile_kind": "brand",
        "identity": {
            "brand_id": brand["brand_id"],
            "name": brand.get("name"),
            "aliases": brand.get("aliases") or [],
            "entity_type": "brand",
        },
        "public_summary": {
            "headline": f"{brand.get('name')}: marca identificada no cadastro de relações",
            "quick_answer": (
                f"{brand.get('name')} é uma marca, não uma pessoa jurídica que herda "
                f"avaliação. A relação documentada aponta para {targets}. A ferramenta "
                "mantém marca e entidade regulada separadas para não transferir dados ou "
                "conclusões indevidamente."
            ),
        },
        "relationships": relationships,
        "assessment": {
            "availability": "not_applicable",
            "reason": "brand_never_inherits_entity_assessment",
        },
        "sandbox_conduct_context": _sandbox_brand_context(
            str(brand["brand_id"]), sandbox_payload
        ),
        "limits": [
            "A marca não herda avaliação ou posição da entidade relacionada.",
            "Relação de marca/risk carrier não autoriza transferir reclamações sem política de atribuição.",
        ],
    }


def _entity_profile(
    entity: dict[str, Any],
    *,
    by_id: dict[str, dict[str, Any]],
    by_cnpj: dict[str, dict[str, Any]],
    group_by_id: dict[str, dict[str, Any]],
    brands: list[dict[str, Any]],
    explorer_by_id: dict[str, dict[str, Any]],
    sandbox_payload: dict[str, Any],
    conduct_registry: dict[str, Any],
) -> dict[str, Any]:
    entity_id = str(entity["entity_id"])
    conduct_relationships = _conduct_relationships(
        entity, conduct_registry, by_cnpj=by_cnpj
    )
    explorer = explorer_by_id.get(entity_id)
    context = entity.get("query_context") or {}
    successor = by_id.get(str(context.get("successor_entity_id") or ""))
    return {
        "profile_id": f"entity:{entity_id}",
        "profile_kind": "entity",
        "identity": {
            "entity_id": entity_id,
            "legal_entity_id": entity.get("legal_entity_id"),
            "fip_code": entity.get("fip_code"),
            "cnpj": entity.get("cnpj"),
            "legal_name": entity.get("legal_name"),
            "display_name": entity.get("display_name"),
            "entity_type": entity.get("entity_type"),
        },
        "regulatory": {
            "regime": entity.get("regulatory_regime"),
            "status": entity.get("regulatory_status"),
            "label": _regulatory_label(entity),
            "query_state": context.get("entity_state"),
            "filter_bucket": context.get("filter_bucket"),
        },
        "lifecycle": {
            "legal_lifecycle": entity.get("legal_lifecycle"),
            "is_historical": context.get("filter_bucket") == "historical",
            "successor_profile_id": (
                f"entity:{successor['entity_id']}" if successor else None
            ),
            "successor_name": _entity_name(successor) if successor else None,
            "successor_chain": [
                f"entity:{value}" for value in context.get("successor_chain") or []
            ],
        },
        "public_summary": {
            "headline": _regulatory_label(entity),
            "quick_answer": _entity_intro(entity, by_id),
        },
        "relationship_context": {
            "economic_group": _same_group_context(entity, by_id, group_by_id),
            "direct_relationships": _direct_relationships(entity, by_id),
            "brands": _incoming_brands(entity_id, brands),
            "conduct_reconciliation": conduct_relationships,
        },
        "assessment": (
            _ordinary_assessment(explorer, conduct_relationships)
            if explorer is not None
            else {
                "availability": "not_applicable",
                "reason": (
                    "entity_outside_current_ordinary_assessment_contract"
                    if entity.get("entity_type") != "insurer"
                    or entity.get("regulatory_regime") != "ordinary"
                    else "ordinary_entity_not_present_in_current_assessment_payload"
                ),
            }
        ),
        "sandbox_conduct": _sandbox_conduct_for_entity(entity, sandbox_payload),
        "limits": [
            "Ausência de dado nunca é convertida em zero.",
            "Grupo econômico não prova, sozinho, sucessão, aquisição ou joint venture.",
            "Marca e entidade regulada permanecem identidades distintas.",
        ],
    }


def _search_entry(profile: dict[str, Any]) -> dict[str, Any]:
    identity = profile.get("identity") or {}
    if profile["profile_kind"] == "brand":
        name = str(identity.get("name") or profile["profile_id"])
        aliases = list(identity.get("aliases") or [])
        cnpj = None
        fip_code = None
        filter_bucket = "brands"
        entity_type = "brand"
        targets = ", ".join(
            str(item.get("target_name") or "")
            for item in profile.get("relationships") or []
            if item.get("target_name")
        )
        disambiguation = f"Marca{f' · relacionada a {targets}' if targets else ''}"
    else:
        name = str(
            identity.get("display_name")
            or identity.get("legal_name")
            or profile["profile_id"]
        )
        aliases = []
        cnpj = identity.get("cnpj")
        fip_code = identity.get("fip_code")
        regulatory = profile.get("regulatory") or {}
        filter_bucket = regulatory.get("filter_bucket")
        entity_type = identity.get("entity_type")
        pieces = [
            regulatory.get("label"),
            f"CNPJ {cnpj}" if cnpj else None,
            f"SUSEP {fip_code}" if fip_code else None,
        ]
        disambiguation = " · ".join(str(piece) for piece in pieces if piece)
    terms = [name, *aliases, cnpj, fip_code]
    return {
        "search_id": f"search:{profile['profile_id']}",
        "profile_id": profile["profile_id"],
        "profile_path": f"profiles/{_profile_file_key(profile['profile_id'])}.json",
        "result_kind": profile["profile_kind"],
        "entity_type": entity_type,
        "filter_bucket": filter_bucket,
        "name": name,
        "aliases": aliases,
        "cnpj": cnpj,
        "fip_code": fip_code,
        "disambiguation": disambiguation,
        "search_text": _normalize_search(
            " ".join(str(term) for term in terms if term)
        ),
    }


def _validate_null_semantics(value: Any, path: str = "$") -> None:
    if isinstance(value, dict):
        if (
            {"value", "availability", "public_use", "meaning"} <= set(value)
            and value["availability"] == "unavailable"
            and value["value"] is not None
        ):
            raise PublicSearchProfileContractError(
                f"{path}: unavailable metric has non-null value"
            )
        for key, child in value.items():
            _validate_null_semantics(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _validate_null_semantics(child, f"{path}[{index}]")


def build_public_search_profile_contract(
    lifecycle: dict[str, Any],
    explorer: dict[str, Any],
    sandbox_conduct: dict[str, Any],
    conduct_relationships: dict[str, Any],
) -> dict[str, Any]:
    if lifecycle.get("artifact") != "v2_lifecycle_relationship_inventory":
        raise PublicSearchProfileContractError("unexpected lifecycle artifact")
    if explorer.get("artifact") != "v2_public_insurer_explorer":
        raise PublicSearchProfileContractError("unexpected explorer artifact")
    if sandbox_conduct.get("artifact") != "v2_sandbox_brand_conduct_evidence":
        raise PublicSearchProfileContractError("unexpected Sandbox Conduct artifact")

    entities = list(lifecycle.get("entities") or [])
    brands = list(lifecycle.get("brands") or [])
    groups = list(lifecycle.get("groups") or [])
    explorer_entities = list(explorer.get("entities") or [])
    by_id = {str(row["entity_id"]): row for row in entities}
    if len(by_id) != len(entities):
        raise PublicSearchProfileContractError("duplicate lifecycle entity_id")
    by_cnpj = {str(row["cnpj"]): row for row in entities if row.get("cnpj")}
    if len(by_cnpj) != sum(bool(row.get("cnpj")) for row in entities):
        raise PublicSearchProfileContractError("lifecycle CNPJ is not unique")
    group_by_id = {
        str(row["group_id"]): row for row in groups if row.get("group_id")
    }
    explorer_by_id = {str(row["entity_id"]): row for row in explorer_entities}
    missing = sorted(set(explorer_by_id) - set(by_id))
    if missing:
        raise PublicSearchProfileContractError(
            "explorer contains entities absent from lifecycle: " + ", ".join(missing[:5])
        )

    profiles = [
        _entity_profile(
            entity,
            by_id=by_id,
            by_cnpj=by_cnpj,
            group_by_id=group_by_id,
            brands=brands,
            explorer_by_id=explorer_by_id,
            sandbox_payload=sandbox_conduct,
            conduct_registry=conduct_relationships,
        )
        for entity in entities
    ]
    profiles.extend(
        _brand_profile(brand, by_id, sandbox_conduct) for brand in brands
    )
    profile_ids = [str(row["profile_id"]) for row in profiles]
    if len(set(profile_ids)) != len(profile_ids):
        raise PublicSearchProfileContractError("duplicate public profile_id")
    file_keys = [_profile_file_key(profile_id) for profile_id in profile_ids]
    if len(set(file_keys)) != len(file_keys):
        raise PublicSearchProfileContractError("profile file key collision")
    for profile in profiles:
        _validate_null_semantics(profile)

    search_entries = [_search_entry(profile) for profile in profiles]
    ordinary = [
        profile
        for profile in profiles
        if profile["profile_kind"] == "entity"
        and (profile.get("regulatory") or {}).get("regime") == "ordinary"
        and (profile.get("identity") or {}).get("entity_type") == "insurer"
        and (profile.get("regulatory") or {}).get("query_state")
        == "current_ordinary_insurer"
    ]
    sandbox = [
        profile
        for profile in profiles
        if profile["profile_kind"] == "entity"
        and (profile.get("regulatory") or {}).get("regime") == "sandbox"
    ]
    generated_at = _utc_now()
    return {
        "artifact": "v2_public_search_profile_contract",
        "generated_at": generated_at,
        "version": VERSION,
        "status": "public_search_profile_contract_closed",
        "source_contracts": {
            "identity_lifecycle_relationships": lifecycle.get("artifact"),
            "ordinary_assessment_explorer": explorer.get("artifact"),
            "sandbox_conduct": sandbox_conduct.get("artifact"),
            "conduct_relationship_registry": (
                "data/reference/v2/conduct_subject_relationships.json"
            ),
        },
        "publication_policy": {
            "search_is_broader_than_ordinary_assessment": True,
            "frontend_may_use_fuzzy_search_to_rank_candidates": True,
            "frontend_may_use_fuzzy_search_to_decide_identity": False,
            "brand_inherits_entity_assessment": False,
            "group_membership_implies_succession_or_joint_venture": False,
            "missing_value_may_be_coerced_to_zero": False,
            "raw_zero_may_be_relabelled_as_missing": False,
            "zero_complaints_is_automatically_favorable": False,
            "sandbox_enters_ordinary_ranking": False,
            "php_may_recompute_methodology": False,
            "profile_progression": [
                "quick_answer",
                "plain_language_signals",
                "relationships_and_identity_context",
                "technical_metrics",
                "methodological_limits",
            ],
        },
        "population": {
            "lifecycle_entities": len(entities),
            "brands": len(brands),
            "profiles": len(profiles),
            "search_entries": len(search_entries),
            "ordinary_current_insurer_profiles": len(ordinary),
            "ordinary_profiles_with_assessment_payload": sum(
                (profile.get("assessment") or {}).get("availability")
                in {"available", "incomplete"}
                for profile in ordinary
            ),
            "sandbox_entity_profiles": len(sandbox),
            "sandbox_profiles_with_conduct_context": sum(
                profile.get("sandbox_conduct") is not None for profile in sandbox
            ),
        },
        "public_output_contract": {
            "search_index_file": "public/search_index.json",
            "profile_manifest_file": "public/profile_manifest.json",
            "profiles_directory": "public/profiles",
            "insurer_explorer_role": (
                "exploration_and_comparison_dataset_only_not_complete_identity_profile"
            ),
            "host_role": (
                "PHP/JS renders precomputed profiles and may rank search candidates; "
                "it must not infer identity or recompute methodology."
            ),
        },
        "search_index": sorted(
            search_entries,
            key=lambda row: (str(row["name"]).casefold(), row["profile_id"]),
        ),
        "profiles": sorted(profiles, key=lambda row: row["profile_id"]),
    }


def write_public_outputs(
    payload: dict[str, Any],
    *,
    root: Path = PUBLIC_DIR,
) -> list[Path]:
    root.mkdir(parents=True, exist_ok=True)
    profile_dir = root / "profiles"
    profile_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    search_payload = {
        "artifact": "v2_public_search_index",
        "generated_at": payload["generated_at"],
        "version": payload["version"],
        "status": "public_search_index_ready",
        "publication_policy": payload["publication_policy"],
        "population": payload["population"],
        "entries": payload["search_index"],
    }
    search_path = root / "search_index.json"
    search_path.write_text(
        json.dumps(search_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    written.append(search_path)

    manifest_entries = []
    for profile in payload["profiles"]:
        filename = f"{_profile_file_key(profile['profile_id'])}.json"
        path = profile_dir / filename
        body = {
            "artifact": "v2_public_entity_or_brand_profile",
            "generated_at": payload["generated_at"],
            "version": payload["version"],
            "status": "public_profile_ready",
            **profile,
        }
        path.write_text(
            json.dumps(body, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        written.append(path)
        manifest_entries.append(
            {
                "profile_id": profile["profile_id"],
                "profile_kind": profile["profile_kind"],
                "path": f"profiles/{filename}",
            }
        )

    manifest = {
        "artifact": "v2_public_profile_manifest",
        "generated_at": payload["generated_at"],
        "version": payload["version"],
        "status": "public_profile_manifest_ready",
        "population": payload["population"],
        "profiles": manifest_entries,
    }
    manifest_path = root / "profile_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    written.append(manifest_path)
    return written


def build_from_files(
    *,
    lifecycle_path: Path = LIFECYCLE_PATH,
    explorer_path: Path = EXPLORER_PATH,
    sandbox_conduct_path: Path = SANDBOX_CONDUCT_PATH,
    conduct_relationships_path: Path = CONDUCT_RELATIONSHIPS_PATH,
) -> dict[str, Any]:
    return build_public_search_profile_contract(
        json.loads(lifecycle_path.read_text(encoding="utf-8")),
        json.loads(explorer_path.read_text(encoding="utf-8")),
        json.loads(sandbox_conduct_path.read_text(encoding="utf-8")),
        json.loads(conduct_relationships_path.read_text(encoding="utf-8")),
    )


def main() -> None:
    payload = build_from_files()
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
                "public_files": [str(path) for path in written],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
