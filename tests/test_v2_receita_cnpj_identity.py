from api.sources.receita_cnpj_identity import (
    _build_query_indexes,
    _business_core,
    _finalize_provider_matches,
    _match_name_to_queries,
    _surface_name,
)


def test_surface_name_removes_legal_form_but_preserves_business_words() -> None:
    assert _surface_name("UNIAO SEGURADORA S.A.") == "uniao seguradora"
    assert _surface_name("FAIRFAX BRASIL SEGUROS CORPORATIVOS SA") == (
        "fairfax brasil seguros corporativos"
    )


def test_business_core_handles_previsul_fairfax_and_equatorial() -> None:
    assert _business_core("PREVISUL") == "previsul"
    assert _business_core("FAIRFAX BRASIL SEGUROS CORPORATIVOS SA") == "fairfax brasil"
    assert _business_core("Equatorial Previdência Complementar") == "equatorial"
    # Generic one-token names are deliberately rejected from core matching.
    assert _business_core("União Seguradora") == ""


def test_trade_name_exact_can_resolve_uniao_without_fuzzy() -> None:
    queries = _build_query_indexes(["União Seguradora"])
    matches = _match_name_to_queries(
        "UNIAO SEGURADORA S.A.",
        queries,
        prefix="trade_name",
    )
    assert matches == [("uniao seguradora", "trade_name_surface_exact")]


def test_legal_core_can_discover_fairfax_as_candidate() -> None:
    queries = _build_query_indexes(["Fairfax Brasil"])
    matches = _match_name_to_queries(
        "FAIRFAX BRASIL SEGUROS CORPORATIVOS SA",
        queries,
        prefix="legal_name",
    )
    assert matches == [("fairfax brasil", "legal_name_business_core")]


def test_finalizer_accepts_only_unique_candidate_at_best_tier() -> None:
    queries = _build_query_indexes(["Previsul"])
    rows = {
        "previsul": [
            {
                "cnpj": "92751213000173",
                "cnpj_base": "92751213",
                "trade_name": "PREVISUL",
                "match_method": "trade_name_surface_exact",
                "is_head_office": True,
            },
            {
                "cnpj": "00000000000100",
                "cnpj_base": "00000000",
                "trade_name": "PREVISUL",
                "match_method": "trade_name_business_core",
                "is_head_office": True,
            },
        ]
    }
    result = _finalize_provider_matches(queries, rows)[0]
    assert result["candidate_state"] == "unique_candidate"
    assert result["match_method"] == "trade_name_surface_exact"
    assert result["candidates"][0]["cnpj"] == "92751213000173"


def test_finalizer_preserves_ambiguity_at_same_match_tier() -> None:
    queries = _build_query_indexes(["Marca Exemplo"])
    rows = {
        "marca exemplo": [
            {
                "cnpj": "11111111000111",
                "cnpj_base": "11111111",
                "match_method": "trade_name_surface_exact",
                "is_head_office": True,
            },
            {
                "cnpj": "22222222000122",
                "cnpj_base": "22222222",
                "match_method": "trade_name_surface_exact",
                "is_head_office": True,
            },
        ]
    }
    result = _finalize_provider_matches(queries, rows)[0]
    assert result["candidate_state"] == "ambiguous_candidates"
    assert result["match_method"] is None
    assert len(result["candidates"]) == 2
