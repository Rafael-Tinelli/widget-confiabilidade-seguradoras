from api.v2.consumer_gov_receita_resolution import (
    build_receita_provider_index,
    resolve_provider_via_receita,
)
from api.v2.consumer_gov_universe_resolution import build_full_universe_provider_index


def _entity(entity_id: str, cnpj: str, legal_name: str, eligible: bool = True):
    return {
        "entity_id": entity_id,
        "cnpj": cnpj,
        "legal_name": legal_name,
        "entity_type": "insurer",
        "eligibility": {
            "regulatory_universe_eligible": eligible,
            "reason_codes": [],
        },
    }


def _payload(provider: str, candidate: dict, method: str = "trade_name_surface_exact"):
    return {
        "artifact": "v2_receita_cnpj_identity",
        "provider_matches": [
            {
                "provider": provider,
                "provider_key": provider.lower(),
                "candidate_state": "unique_candidate",
                "match_method": method,
                "candidates": [candidate],
            }
        ],
    }


def test_receita_name_candidate_only_admits_insurer_after_exact_cnpj_susep_match() -> None:
    entities = [
        _entity(
            "fip:005193",
            "92751213000173",
            "COMPANHIA DE SEGUROS PREVIDENCIA DO SUL",
        )
    ]
    candidate = {
        "cnpj": "92751213000173",
        "legal_name_receita": "COMPANHIA DE SEGUROS PREVIDENCIA DO SUL",
        "trade_name": "PREVISUL",
        "primary_cnae_code": "6511101",
    }
    result = resolve_provider_via_receita(
        "Previsul",
        build_receita_provider_index(_payload("Previsul", candidate)),
        build_full_universe_provider_index(entities),
    )
    assert result is not None
    assert result["resolution_state"] == "matched_current_insurer"
    assert result["entity_id"] == "fip:005193"
    assert "cnpj_to_canonical" in result["match_method"]


def test_capitalization_candidate_is_excluded_without_transfer_to_group_insurer() -> None:
    entities = [
        _entity("fip:005321", "61557924000135", "ITAU SEGUROS S.A.")
    ]
    candidate = {
        "cnpj": "23025711000116",
        "legal_name_receita": "CIA ITAU DE CAPITALIZACAO",
        "trade_name": "ITAU UNIBANCO CAPITALIZACAO",
        "primary_cnae_code": "6450600",
    }
    result = resolve_provider_via_receita(
        "Itaú Unibanco Capitalização",
        build_receita_provider_index(
            _payload("Itaú Unibanco Capitalização", candidate)
        ),
        build_full_universe_provider_index(entities),
    )
    assert result is not None
    assert result["resolution_state"] == "outside_157"
    assert result["entity_id"] is None
    assert result["reason_code"] == "receita_capitalization_activity"


def test_closed_pension_candidate_is_safely_outside_157() -> None:
    candidate = {
        "cnpj": "08071645000127",
        "legal_name_receita": "CIASPREV",
        "trade_name": "CIASPREV",
        "primary_cnae_code": "6541300",
        "legal_nature_code": "3999",
    }
    result = resolve_provider_via_receita(
        "Ciasprev",
        build_receita_provider_index(_payload("Ciasprev", candidate)),
        build_full_universe_provider_index([]),
    )
    assert result is not None
    assert result["resolution_state"] == "outside_157"
    assert result["reason_code"] == "receita_closed_pension_activity"


def test_open_pension_keyword_does_not_exclude_a_current_susep_insurer() -> None:
    entities = [
        _entity(
            "fip:002801",
            "95611141000157",
            "UNIAO SEGURADORA S.A. - VIDA E PREVIDENCIA",
        )
    ]
    candidate = {
        "cnpj": "95611141000157",
        "legal_name_receita": "UNIAO SEGURADORA S.A. - VIDA E PREVIDENCIA",
        "trade_name": "UNIAO SEGURADORA S.A.",
        "primary_cnae_code": "6511101",
        "secondary_cnaes": [
            {"code": "6542100", "description": "Previdencia complementar aberta"}
        ],
    }
    result = resolve_provider_via_receita(
        "União Seguradora",
        build_receita_provider_index(_payload("União Seguradora", candidate)),
        build_full_universe_provider_index(entities),
    )
    assert result is not None
    assert result["resolution_state"] == "matched_current_insurer"
    assert result["entity_id"] == "fip:002801"


def test_broker_candidate_is_outside_but_does_not_transfer_to_carrier() -> None:
    candidate = {
        "cnpj": "00000000000100",
        "legal_name_receita": "SUDAVIDA CORRETORA DE SEGUROS LTDA",
        "trade_name": "SUDAVIDA",
        "primary_cnae_code": "6622300",
    }
    result = resolve_provider_via_receita(
        "SudaVida",
        build_receita_provider_index(_payload("SudaVida", candidate)),
        build_full_universe_provider_index([]),
    )
    assert result is not None
    assert result["resolution_state"] == "outside_157"
    assert result["reason_code"] == "receita_insurance_broker_or_agent_activity"


def test_ambiguous_receita_candidates_never_resolve() -> None:
    payload = {
        "artifact": "v2_receita_cnpj_identity",
        "provider_matches": [
            {
                "provider": "Marca X",
                "candidate_state": "ambiguous_candidates",
                "candidates": [
                    {"cnpj": "11111111000111", "primary_cnae_code": "6512000"},
                    {"cnpj": "22222222000122", "primary_cnae_code": "6512000"},
                ],
            }
        ],
    }
    assert (
        resolve_provider_via_receita(
            "Marca X",
            build_receita_provider_index(payload),
            build_full_universe_provider_index([]),
        )
        is None
    )


def test_insurance_cnae_without_susep_current_entity_does_not_admit() -> None:
    candidate = {
        "cnpj": "99999999000199",
        "legal_name_receita": "EMPRESA COM CNAE DE SEGUROS",
        "trade_name": "EXEMPLO",
        "primary_cnae_code": "6512000",
    }
    assert (
        resolve_provider_via_receita(
            "Exemplo",
            build_receita_provider_index(_payload("Exemplo", candidate)),
            build_full_universe_provider_index([]),
        )
        is None
    )
