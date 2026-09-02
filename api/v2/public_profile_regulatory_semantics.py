from __future__ import annotations

import json
from copy import deepcopy
from typing import Any

import api.v2.build_public_search_profile_contract as base_contract
from api.v2.public_information_projection import project_from_files

VERSION = "2.0-public-search-profile-contract-3"
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
        "comparador de consumo da metodologia v2. Por isso, não recebe avaliação "
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


def apply_regulatory_profile_semantics(payload: dict[str, Any]) -> dict[str, Any]:
    """Apply public subtype semantics without changing identity or methodology.

    The base profile builder intentionally keeps the supervised legal superclass
    (``entity_type=insurer``). This layer adds public distinctions carried by lifecycle
    ``query_state`` so SSPEs and insurance cooperatives cannot be mistaken for the
    ordinary consumer-insurer comparator merely because they share a superclass.
    """

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
) -> dict[str, Any]:
    payload = base_contract.build_public_search_profile_contract(
        lifecycle,
        explorer,
        sandbox_conduct,
        conduct_relationships,
    )
    return apply_regulatory_profile_semantics(payload)


def build_from_files() -> dict[str, Any]:
    return apply_regulatory_profile_semantics(base_contract.build_from_files())


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
