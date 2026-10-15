from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

import api.v2.build_public_search_profile_contract as base_contract
from api.v2.public_information_projection import project_from_files

VERSION = "2.0-public-search-profile-contract-4"
MARKET_RELATIONSHIP_CONDUCT_PATH = Path(
    "data/derived/v2/market_relationship_conduct_evidence.json"
)
SSPE_QUERY_STATE = "special_purpose_insurer"
SSPE_LABEL = "Seguradora de propósito específico (SSPE)"
SSPE_ASSESSMENT_REASON = "special_purpose_insurer_outside_consumer_assessment"
COOPERATIVE_QUERY_STATE = "insurance_cooperative"
COOPERATIVE_LABEL = "Sociedade cooperativa de seguros"
COOPERATIVE_ASSESSMENT_REASON = "insurance_cooperative_outside_ordinary_assessment"


def _profile_name(profile: dict[str, Any]) -> str:
    identity = profile.get("identity") or {}
    return str(
        identity.get("display_name")
        or identity.get("legal_name")
        or profile.get("profile_id")
        or "Entidade"
    )


def _sspe_quick_answer(profile: dict[str, Any]) -> str:
    name = _profile_name(profile)
    return (
        f"{name} é identificada como seguradora de propósito específico (SSPE). "
        "Esse subtipo permanece pesquisável e supervisionado, mas sua finalidade "
        "específica não corresponde ao universo de seguradoras ordinárias usado pelo "
        "comparador voltado ao consumidor. Por isso, não recebe avaliação "
        "conjunta nem posição de ranking nesse universo."
    )


def _cooperative_quick_answer(profile: dict[str, Any]) -> str:
    name = _profile_name(profile)
    return (
        f"{name} é identificada como sociedade cooperativa de seguros. A identidade "
        "permanece pesquisável e supervisionada, mas essa forma regulatória é mantida "
        "separada do benchmark de seguradoras ordinárias. Enquanto o contrato de "
        "assessment não autorizar comparabilidade específica para cooperativas, a "
        "ausência de avaliação não representa conclusão favorável nem adversa."
    )


def _subtype_disambiguation(profile: dict[str, Any], label: str) -> str:
    identity = profile.get("identity") or {}
    pieces = [
        label,
        f"CNPJ {identity.get('cnpj')}" if identity.get("cnpj") else None,
        f"SUSEP {identity.get('fip_code')}" if identity.get("fip_code") else None,
    ]
    return " · ".join(str(piece) for piece in pieces if piece)


def _public_metric(
    value: Any,
    *,
    meaning: str,
    zero_semantics: str | None = None,
) -> dict[str, Any]:
    return {
        "value": value,
        "availability": "unavailable" if value is None else "available",
        "public_use": "displayable",
        "zero_semantics": zero_semantics,
        "meaning": meaning,
    }


def _trajectory_public(film: dict[str, Any] | None) -> dict[str, Any] | None:
    if not film:
        return None
    satisfaction = film.get("satisfaction_trend") or {}
    return {
        "history_state": film.get("history_state"),
        "months_observed": _public_metric(
            film.get("months_observed"),
            meaning="Quantidade de meses preservados na janela de Conduta.",
        ),
        "months_with_complaints": _public_metric(
            film.get("months_with_complaints"),
            meaning="Meses da janela em que houve reclamações observadas.",
            zero_semantics="observed_zero_is_not_a_favorable_finding",
        ),
        "satisfaction_trend": {
            "direction": satisfaction.get("direction"),
            "early_half_average": _public_metric(
                satisfaction.get("early_half_average"),
                meaning="Média de satisfação na primeira metade da janela.",
            ),
            "early_half_sample": _public_metric(
                satisfaction.get("early_half_sample"),
                meaning="Quantidade de avaliações na primeira metade da janela.",
                zero_semantics="zero_sample_means_no_basis_for_average",
            ),
            "recent_half_average": _public_metric(
                satisfaction.get("recent_half_average"),
                meaning="Média de satisfação na metade recente da janela.",
            ),
            "recent_half_sample": _public_metric(
                satisfaction.get("recent_half_sample"),
                meaning="Quantidade de avaliações na metade recente da janela.",
                zero_semantics="zero_sample_means_no_basis_for_average",
            ),
        },
        "public_limit": (
            "A série é contexto longitudinal do sujeito observado. Ela não produz score, "
            "ranking nem autoriza transferir reclamações entre participantes do arranjo."
        ),
    }


def _conduct_context(
    summary: dict[str, Any],
    *,
    source_role: str,
    plain_language: str,
) -> dict[str, Any]:
    return {
        "availability": "available",
        "source_role": source_role,
        "provider_labels": summary.get("provider_labels") or [],
        "metrics": {
            "complaints": _public_metric(
                summary.get("complaints"),
                meaning="Reclamações observadas contra este sujeito na janela preservada.",
                zero_semantics="observed_zero_is_not_a_favorable_finding",
            ),
            "responded": _public_metric(
                summary.get("responded"),
                meaning="Reclamações com resposta registrada; resposta não prova solução.",
            ),
            "response_rate": _public_metric(
                summary.get("response_rate"),
                meaning="Taxa de resposta observada; não equivale a resolução.",
            ),
            "finalized": _public_metric(
                summary.get("finalized"),
                meaning="Reclamações finalizadas; finalização não prova solução.",
            ),
            "finalized_rate": _public_metric(
                summary.get("finalized_rate"),
                meaning="Taxa de finalização observada; não equivale a resolução.",
            ),
            "satisfaction_count": _public_metric(
                summary.get("satisfaction_count"),
                meaning="Quantidade de avaliações de satisfação preservadas.",
                zero_semantics="zero_sample_means_no_basis_for_average",
            ),
            "average_satisfaction": _public_metric(
                summary.get("average_satisfaction"),
                meaning="Média de satisfação entre quem avaliou, dependente da amostra.",
            ),
        },
        "trajectory_context": _trajectory_public(summary.get("film")),
        "plain_language": plain_language,
    }


def apply_market_relationship_semantics(
    payload: dict[str, Any],
    market_relationship_conduct: dict[str, Any],
) -> dict[str, Any]:
    """Attach distributor/risk-bearer context independently of regulatory regime."""

    output = deepcopy(payload)
    if market_relationship_conduct.get("artifact") != (
        "v2_market_relationship_conduct_evidence"
    ):
        raise ValueError("unexpected market relationship conduct artifact")

    rows = {
        str(row.get("brand_id") or ""): row
        for row in market_relationship_conduct.get("brands") or []
        if row.get("brand_id")
    }
    contextualized = 0
    multi_carrier = 0

    for profile in output.get("profiles") or []:
        if profile.get("profile_kind") != "brand":
            continue
        brand_id = str(profile.get("profile_id") or "")
        row = rows.get(brand_id)
        if row is None:
            continue

        market = dict(row.get("market_identity") or {})
        brand_summary = row.get("market_identity_conduct_summary") or {}
        distributor = {
            "identity_state": market.get("identity_state") or "unverified",
            "public_label": market.get("public_label"),
            "legal_name": market.get("legal_name"),
            "trade_name": market.get("trade_name"),
            "cnpj": market.get("cnpj"),
            "distribution_role": row.get("distribution_role")
            or market.get("distribution_role"),
            "distribution_role_state": row.get("distribution_role_state")
            or market.get("distribution_role_state")
            or "unverified",
            "public_note": market.get("public_note"),
            "evidence": market.get("evidence") or {},
        }
        distributor_conduct = _conduct_context(
            brand_summary,
            source_role="consumer_gov_market_identity_subject",
            plain_language=(
                "Estes números pertencem às reclamações que a fonte permite atribuir "
                "diretamente à pessoa jurídica que comercializa/distribui. Eles permanecem "
                "separados das reclamações das entidades que assumem o risco."
            ),
        )

        risk_bearers = []
        for bearer in row.get("risk_bearers") or []:
            bearer_summary = bearer.get("conduct_summary") or {}
            name = bearer.get("name") or "a entidade que assume o risco"
            sandbox_history = dict(bearer.get("sandbox_history") or {})
            graduated_from_sandbox = (
                sandbox_history.get("current_outcome")
                == "graduated_to_ordinary_license"
            )
            carrier_plain_language = (
                (
                    f"Estes números são do Consumer.gov contra {name}, no recorte "
                    "canônico de meses elegíveis ao regime ordinário. O histórico anterior "
                    "à graduação é preservado separadamente e não entra nessa contagem; "
                    "também não são reclamações da distribuidora."
                )
                if (
                    graduated_from_sandbox
                    and bearer.get("conduct_source")
                    == "canonical_consumer_gov_conduct_evidence"
                )
                else (
                    f"Estes números são do Consumer.gov contra {name}. A entidade possui "
                    "histórico no Sandbox e hoje aparece como seguradora ordinária; esta série "
                    "é contexto do sujeito legal e pode incluir observações anteriores à "
                    "graduação. Ela não deve ser lida como pressão exclusiva do regime "
                    "ordinário nem como reclamações da distribuidora."
                )
                if graduated_from_sandbox
                else (
                    f"Estes números são do Consumer.gov contra {name}. Eles são "
                    "contexto da entidade que assume o risco e não são reclamações "
                    "da distribuidora nem reclamações exclusivas desta marca/produto."
                )
            )
            risk_bearers.append(
                {
                    "public_role_label": "Entidade que assume o risco",
                    "profile_id": bearer.get("profile_id"),
                    "entity_id": bearer.get("entity_id"),
                    "name": name,
                    "cnpj": bearer.get("cnpj"),
                    "entity_type": bearer.get("entity_type"),
                    "regulatory_regime": bearer.get("regulatory_regime"),
                    "regulatory_status": bearer.get("regulatory_status"),
                    "regulatory_class": bearer.get("regulatory_class"),
                    "regulatory_history": {
                        "sandbox": sandbox_history or None,
                    },
                    "relationship_status": bearer.get("relationship_status"),
                    "product_scope": bearer.get("product_scope"),
                    "effective_from": bearer.get("effective_from"),
                    "effective_to": bearer.get("effective_to"),
                    "evidence": bearer.get("evidence") or {},
                    "conduct_source": bearer.get("conduct_source"),
                    "conduct_context": _conduct_context(
                        bearer_summary,
                        source_role="risk_bearer_legal_subject",
                        plain_language=carrier_plain_language,
                    ),
                }
            )

        profile["market_relationship_context"] = {
            "availability": "available",
            "distributor": distributor,
            "distributor_conduct": distributor_conduct,
            "risk_bearers": risk_bearers,
            "conduct_scope": "separate_market_identity_and_risk_bearer_subjects",
            "multi_carrier": len(risk_bearers) > 1,
            "public_limit": (
                "Distribuidora/representante, marca e entidade que assume o risco são "
                "sujeitos distintos. Relação comercial não transfere reclamações, "
                "avaliação ou ranking entre eles."
            ),
        }
        limits = list(profile.get("limits") or [])
        generic_limit = (
            "O papel de distribuição e a entidade que assume o risco são modelados "
            "independentemente do regime regulatório do carrier."
        )
        if generic_limit not in limits:
            limits.append(generic_limit)
        profile["limits"] = limits
        contextualized += 1
        if len(risk_bearers) > 1:
            multi_carrier += 1

    population = dict(output.get("population") or {})
    population["market_relationship_brand_profiles"] = contextualized
    population["multi_risk_bearer_brand_profiles"] = multi_carrier
    output["population"] = population

    policy = dict(output.get("publication_policy") or {})
    policy["distribution_relationship_model_is_regime_agnostic"] = True
    policy["market_identity_and_risk_bearer_are_distinct_subjects"] = True
    policy["risk_bearer_relation_never_transfers_complaints"] = True
    policy["multi_carrier_relationships_must_remain_separate"] = True
    policy["unverified_distribution_role_may_not_be_presented_as_verified"] = True
    output["publication_policy"] = policy
    output["version"] = VERSION
    return output


def apply_regulatory_profile_semantics(payload: dict[str, Any]) -> dict[str, Any]:
    """Apply public subtype semantics without changing identity or methodology."""

    output = deepcopy(payload)
    if output.get("artifact") != "v2_public_search_profile_contract":
        raise ValueError("unexpected public search/profile artifact")

    profiles = list(output.get("profiles") or [])
    sspe_ids: set[str] = set()
    cooperative_ids: set[str] = set()
    for profile in profiles:
        if profile.get("profile_kind") != "entity":
            continue
        regulatory = profile.get("regulatory") or {}
        query_state = regulatory.get("query_state")
        profile_id = str(profile.get("profile_id") or "")
        if query_state not in {SSPE_QUERY_STATE, COOPERATIVE_QUERY_STATE}:
            continue
        if not profile_id:
            raise ValueError("regulated subtype profile without profile_id")

        limits = list(profile.get("limits") or [])
        if query_state == SSPE_QUERY_STATE:
            sspe_ids.add(profile_id)
            label = SSPE_LABEL
            quick_answer = _sspe_quick_answer(profile)
            reason = SSPE_ASSESSMENT_REASON
            limit = (
                "SSPE permanece fora do assessment e do ranking de seguradoras ordinárias; "
                "a exclusão decorre do escopo regulatório, não de desempenho."
            )
        else:
            cooperative_ids.add(profile_id)
            label = COOPERATIVE_LABEL
            quick_answer = _cooperative_quick_answer(profile)
            reason = COOPERATIVE_ASSESSMENT_REASON
            limit = (
                "Cooperativa de seguros permanece fora do assessment e do benchmark "
                "ordinário até existir contrato metodológico específico de comparabilidade; "
                "a ausência de avaliação não é evidência adversa."
            )

        regulatory["label"] = label
        profile["regulatory"] = regulatory
        profile["public_summary"] = {
            "headline": label,
            "quick_answer": quick_answer,
        }
        profile["assessment"] = {
            "availability": "not_applicable",
            "reason": reason,
        }
        if limit not in limits:
            limits.append(limit)
        profile["limits"] = limits

    profile_by_id = {
        str(profile.get("profile_id") or ""): profile for profile in profiles
    }
    if len(profile_by_id) != len(profiles):
        raise ValueError("duplicate or empty profile_id after regulatory semantics")

    search_index = list(output.get("search_index") or [])
    for entry in search_index:
        profile_id = str(entry.get("profile_id") or "")
        if profile_id in sspe_ids:
            label = SSPE_LABEL
        elif profile_id in cooperative_ids:
            label = COOPERATIVE_LABEL
        else:
            continue
        profile = profile_by_id[profile_id]
        entry["disambiguation"] = _subtype_disambiguation(profile, label)
        entry["filter_bucket"] = (profile.get("regulatory") or {}).get("filter_bucket")

    population = dict(output.get("population") or {})
    population["special_purpose_insurer_profiles"] = len(sspe_ids)
    population["insurance_cooperative_profiles"] = len(cooperative_ids)
    output["population"] = population

    policy = dict(output.get("publication_policy") or {})
    policy["sspe_enters_ordinary_assessment"] = False
    policy["sspe_enters_ordinary_ranking"] = False
    policy["insurance_cooperative_enters_ordinary_assessment"] = False
    policy["insurance_cooperative_enters_ordinary_ranking"] = False
    output["publication_policy"] = policy
    output["profiles"] = profiles
    output["search_index"] = search_index
    output["version"] = VERSION
    return output


def build_closed_public_search_profile_contract(
    lifecycle: dict[str, Any],
    explorer: dict[str, Any],
    sandbox_conduct: dict[str, Any],
    conduct_relationships: dict[str, Any],
    market_relationship_conduct: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = base_contract.build_public_search_profile_contract(
        lifecycle,
        explorer,
        sandbox_conduct,
        conduct_relationships,
    )
    if market_relationship_conduct is not None:
        payload = apply_market_relationship_semantics(
            payload, market_relationship_conduct
        )
    return apply_regulatory_profile_semantics(payload)


def build_from_files() -> dict[str, Any]:
    payload = base_contract.build_from_files()
    market_relationship_conduct = json.loads(
        MARKET_RELATIONSHIP_CONDUCT_PATH.read_text(encoding="utf-8")
    )
    payload = apply_market_relationship_semantics(
        payload, market_relationship_conduct
    )
    return apply_regulatory_profile_semantics(payload)


def main() -> None:
    payload = build_from_files()
    base_contract.OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    base_contract.OUTPUT_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    written = base_contract.write_public_outputs(payload)

    # §19.2: transport approved source-period context to the public contract.
    # This projection runs before validate_public_search_profile_contract in Gate 4
    # and never derives a methodological state in the frontend.
    payload, _ = project_from_files()

    print(
        json.dumps(
            {
                "artifact": payload["artifact"],
                "status": payload["status"],
                "version": payload["version"],
                "population": payload["population"],
                "public_information_projection": payload.get(
                    "public_information_projection"
                ),
                "public_files": [str(path) for path in written],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
