from api.v2.consumer_gov_receita_resolution import (
    resolve_provider_via_receita_payload,
)


def _entity(
    entity_id: str,
    cnpj: str,
    legal_name: str,
    *,
    entity_type: str = "insurer",
    eligible: bool = True,
):
    return {
        "entity_id": entity_id,
        "cnpj": cnpj,
        "legal_name": legal_name,
        "entity_type": entity_type,
        "eligibility": {
            "regulatory_universe_eligible": eligible,
            "reason_codes": [] if eligible else [f"different_market_{entity_type}"],
        },
    }


def _payload(provider: str, state: str, candidates: list[dict], canonical: list[dict]):
    return {
        "artifact": "v2_receita_cnpj_identity",
        "provider_matches": [
            {
                "provider": provider,
                "provider_key": provider.lower(),
                "candidate_state": state,
                "match_method": None,
                "candidates": candidates,
            }
        ],
        "canonical_records": canonical,
    }


def test_previsul_ambiguous_trade_name_is_resolved_by_unique_sector_candidate() -> None:
    insurer = _entity(
        "fip:005193",
        "92751213000173",
        "COMPANHIA DE SEGUROS PREVIDENCIA DO SUL - PREVISUL",
    )
    candidates = [
        {
            "cnpj": "15462856000156",
            "cnpj_base": "15462856",
            "trade_name": "PREVISUL",
            "primary_cnae_code": "8430200",
        },
        {
            "cnpj": "27988561000105",
            "cnpj_base": "27988561",
            "trade_name": "PREVISUL",
            "primary_cnae_code": "4120400",
        },
        {
            "cnpj": "92751213000173",
            "cnpj_base": "92751213",
            "trade_name": "PREVISUL",
            "legal_name_receita": "COMPANHIA DE SEGUROS PREVIDENCIA DO SUL",
            "primary_cnae_code": "6511101",
        },
    ]
    payload = _payload("Previsul", "ambiguous_candidates", candidates, [])
    result = resolve_provider_via_receita_payload(
        "Previsul", payload, [insurer], verified_hints={}
    )
    assert result is not None
    assert result["resolution_state"] == "matched_current_insurer"
    assert result["entity_id"] == "fip:005193"
    assert result["match_method"] == (
        "receita_ambiguous_candidates_unique_insurance_sector_candidate"
    )


def test_equatorial_previdencia_uses_regulatory_qualifier_to_exclude_pension() -> None:
    pension = _entity(
        "fip:010120",
        "42150987000170",
        "EQUATORIAL PREVIDENCIA COMPLEMENTAR",
        entity_type="open_pension_entity",
        eligible=False,
    )
    insurer = _entity(
        "fip:009999",
        "02591632000100",
        "EQUATORIAL SEGUROS S.A.",
    )
    candidates = [
        {
            "cnpj": "42150987000170",
            "cnpj_base": "42150987",
            "trade_name": "EQUATORIAL",
            "legal_name_receita": "EQUATORIAL PREVIDENCIA COMPLEMENTAR",
            "primary_cnae_code": "6542100",
        },
        {
            "cnpj": "02591632000100",
            "cnpj_base": "02591632",
            "trade_name": "EQUATORIAL SEGUROS",
            "primary_cnae_code": "6511101",
        },
    ]
    payload = _payload("Equatorial Previdência", "ambiguous_candidates", candidates, [])
    result = resolve_provider_via_receita_payload(
        "Equatorial Previdência", payload, [pension, insurer], verified_hints={}
    )
    assert result is not None
    assert result["resolution_state"] == "outside_157"
    assert result["matched_canonical_entity_id"] == "fip:010120"
    assert result["match_method"] == (
        "receita_ambiguous_candidates_unique_regulatory_qualifier"
    )


def test_mg_seguros_remains_unresolved_when_insurer_and_broker_share_trade_name() -> None:
    insurer = _entity(
        "fip:003417",
        "26136748000100",
        "BMG SEGURADORA S.A.",
    )
    candidates = [
        {
            "cnpj": "26136748000100",
            "cnpj_base": "26136748",
            "trade_name": "MG SEGUROS",
            "primary_cnae_code": "6511101",
        },
        {
            "cnpj": "23846343000177",
            "cnpj_base": "23846343",
            "trade_name": "MG SEGUROS",
            "primary_cnae_code": "6622300",
        },
    ]
    payload = _payload(
        "MG Seguros",
        "ambiguous_candidates",
        candidates,
        [
            {
                "cnpj": "26136748000100",
                "cnpj_base": "26136748",
                "trade_name": "MG SEGUROS",
                "legal_name_receita": "BMG SEGURADORA S.A",
                "primary_cnae_code": "6511101",
            }
        ],
    )
    assert (
        resolve_provider_via_receita_payload(
            "MG Seguros", payload, [insurer], verified_hints={}
        )
        is None
    )


def test_itau_capitalizacao_can_fallback_to_unique_canonical_receita_record() -> None:
    capitalization = _entity(
        "fip:021661",
        "23025711000116",
        "CIA. ITAU DE CAPITALIZACAO",
        entity_type="capitalization_company",
        eligible=False,
    )
    payload = _payload(
        "Itaú Unibanco Capitalização",
        "unique_candidate",
        [
            {
                "cnpj": "60701190000104",
                "cnpj_base": "60701190",
                "trade_name": "ITAU UNIBANCO",
                "legal_name_receita": "ITAU UNIBANCO S.A.",
                "primary_cnae_code": "6421200",
            }
        ],
        [
            {
                "cnpj": "23025711000116",
                "cnpj_base": "23025711",
                "legal_name_receita": "CIA ITAU DE CAPITALIZACAO",
                "project_legal_name": "CIA. ITAU DE CAPITALIZACAO",
                "primary_cnae_code": "6450600",
            }
        ],
    )
    result = resolve_provider_via_receita_payload(
        "Itaú Unibanco Capitalização",
        payload,
        [capitalization],
        verified_hints={},
    )
    assert result is not None
    assert result["resolution_state"] == "outside_157"
    assert result["matched_canonical_entity_id"] == "fip:021661"
    assert result["match_method"] == "receita_canonical_record_name_tier_3"


def test_canonical_fallback_does_not_choose_only_resolvable_entity_from_name_collision() -> None:
    surety = _entity(
        "fip:003964",
        "58138452000114",
        "LIBERTY MUTUAL SURETY BRASIL SEGUROS S.A.",
    )
    payload = _payload(
        "Liberty Mutual",
        "no_candidate",
        [],
        [
            {
                "cnpj": "08712487000147",
                "cnpj_base": "08712487",
                "legal_name_receita": "LIBERTY MUTUAL INSURANCE COMPANY",
                "primary_cnae_code": "6512000",
            },
            {
                "cnpj": "58138452000114",
                "cnpj_base": "58138452",
                "legal_name_receita": "LIBERTY MUTUAL SURETY BRASIL SEGUROS S.A.",
                "primary_cnae_code": "6512000",
            },
        ],
    )
    assert (
        resolve_provider_via_receita_payload(
            "Liberty Mutual", payload, [surety], verified_hints={}
        )
        is None
    )


def test_verified_hint_can_exclude_closed_pension_not_found_by_generic_name_scan() -> None:
    payload = _payload("Ciasprev", "no_candidate", [], [])
    hints = {
        "ciasprev": {
            "provider_name": "Ciasprev",
            "target_cnpj": "08071645000127",
            "resolution_state": "outside_157",
            "legal_name_receita": "CIASPREV",
            "primary_cnae_code": "6541300",
            "evidence": [{"authority": "Receita Federal do Brasil"}],
        }
    }
    result = resolve_provider_via_receita_payload(
        "Ciasprev", payload, [], verified_hints=hints
    )
    assert result is not None
    assert result["resolution_state"] == "outside_157"
    assert result["reason_code"] == "receita_closed_pension_activity"
    assert result["match_method"] == "verified_receita_provider_hint_safe_primary_cnae"


def test_verified_branch_trade_name_hint_still_requires_current_susep_head_office() -> None:
    insurer = _entity(
        "fip:005819",
        "67865360000127",
        "ALLSEG SEGURADORA S/A",
    )
    payload = _payload("American Life Seguros", "no_candidate", [], [])
    hints = {
        "american life seguros": {
            "provider_name": "American Life Seguros",
            "target_cnpj": "67865360000127",
            "resolution_state": "matched_current_insurer",
            "legal_name_receita": "ALLSEG SEGURADORA S/A",
            "trade_name": "AMERICAN LIFE COMPANHIA DE SEGUROS",
            "primary_cnae_code": "6511101",
            "evidence": [{"authority": "Receita Federal do Brasil"}],
        }
    }
    result = resolve_provider_via_receita_payload(
        "American Life Seguros", payload, [insurer], verified_hints=hints
    )
    assert result is not None
    assert result["resolution_state"] == "matched_current_insurer"
    assert result["entity_id"] == "fip:005819"
    assert result["match_method"] == "verified_receita_provider_hint_cnpj_to_current_susep"
