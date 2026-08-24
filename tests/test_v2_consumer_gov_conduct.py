from __future__ import annotations

from pathlib import Path

import pytest

from api.v2.consumer_gov_conduct import (
    accumulate_row,
    build_conduct_film,
    finalize_month_evidence,
    new_month_evidence,
)
from api.v2.consumer_gov_conduct_core import (
    aggregate_entry_to_month,
    build_cached_taxonomy_enrichment,
    taxonomy_cache_state,
)


def _month(
    month: str,
    *,
    complaints: int,
    market: int,
    evaluated: int,
    resolved: int,
    satisfaction_count: int,
    satisfaction: float,
    group: str = "Contrato / Oferta",
) -> dict:
    return {
        "month": month,
        "complaints": complaints,
        "matched_current_insurer_market_complaints": market,
        "complaint_share_among_matched_current_insurers": complaints / market,
        "responded": complaints,
        "response_rate": 1.0,
        "finalized": complaints,
        "finalized_rate": 1.0,
        "evaluated": evaluated,
        "consumer_resolved": resolved,
        "consumer_not_resolved": evaluated - resolved,
        "consumer_resolved_rate_among_evaluated": (
            resolved / evaluated if evaluated else None
        ),
        "satisfaction_count": satisfaction_count,
        "average_satisfaction": satisfaction if satisfaction_count else None,
        "response_time_count": 0,
        "average_response_time_days": None,
        "consumer_contacted_company_yes": 0,
        "consumer_contacted_company_no": 0,
        "consumer_contacted_company_yes_rate": None,
        "taxonomy": {
            "area": {"Serviços Financeiros": complaints},
            "assunto": {"Seguro": complaints},
            "grupo_problema": {group: complaints},
            "problema": {"Cobertura": complaints},
            "como_comprou_contratou": {"Internet": complaints},
            "canal_origem": {"Consumidor.gov.br": complaints},
        },
        "situacao": {},
        "avaliacao_reclamacao": {},
        "analise_recusa": {},
    }


def test_accumulate_row_preserves_raw_outcome_and_taxonomy_semantics() -> None:
    evidence = new_month_evidence("2026-01")
    accumulate_row(
        evidence,
        {
            "Respondida": "S",
            "Situação": "Finalizada avaliada",
            "Avaliação Reclamação": "Resolvida",
            "Nota do Consumidor": "4",
            "Tempo Resposta": "3,5",
            "Procurou Empresa": "S",
            "Área": "Serviços Financeiros",
            "Assunto": "Seguro",
            "Grupo Problema": "Contrato / Oferta",
            "Problema": "Oferta não cumprida",
            "Como Comprou Contratou": "Internet",
            "Canal de Origem": "Consumidor.gov.br",
            "Análise da Recusa": "Procedente",
        },
    )
    month = finalize_month_evidence(
        evidence,
        matched_current_insurer_market_complaints=10,
    )

    assert month["complaints"] == 1
    assert month["consumer_resolved_rate_among_evaluated"] == pytest.approx(1.0)
    assert month["taxonomy"]["grupo_problema"]["Contrato / Oferta"] == 1


def test_preserved_aggregate_builds_core_without_inventing_evaluated_denominator() -> None:
    entry = {
        "statistics": {
            "complaintsCount": 20,
            "respondedCount": 19,
            "finalizedCount": 18,
            "resolvedCount": 12,
            "satisfactionCount": 15,
            "scoreSum": 57.0,
        }
    }
    month = aggregate_entry_to_month(
        "2026-06",
        entry,
        matched_current_insurer_market_complaints=1000,
    )

    assert month["complaints"] == 20
    assert month["responded"] == 19
    assert month["finalized"] == 18
    assert month["consumer_resolved"] == 12
    assert month["evaluated"] == 0
    assert month["consumer_resolved_rate_among_evaluated"] is None
    assert month["consumer_resolution_denominator_state"] == (
        "not_preserved_in_legacy_monthly_aggregate"
    )
    assert month["satisfaction_count"] == 15
    assert month["average_satisfaction"] == pytest.approx(3.8)


def test_film_uses_preserved_satisfaction_when_resolution_denominator_is_unavailable() -> None:
    months = []
    for index in range(12):
        early = index < 6
        months.append(
            _month(
                f"2026-{index + 1:02d}",
                complaints=40 if early else 20,
                market=1000,
                evaluated=0,
                resolved=0,
                satisfaction_count=20,
                satisfaction=2.8 if early else 3.8,
            )
        )

    film = build_conduct_film(months)

    assert film["history_state"] == "established"
    assert film["complaint_share_trend"]["direction"] == "falling"
    assert film["consumer_resolution_trend"]["direction"] == "insufficient"
    assert film["satisfaction_trend"]["direction"] == "improving"
    assert film["conduct_signal"] == "improving_observed_pattern"


def test_taxonomy_missing_cache_is_optional_and_does_not_raise(tmp_path: Path) -> None:
    (tmp_path / "basecompleta_2026-05.csv").write_bytes(b"x" * 20)
    state = taxonomy_cache_state(
        ["2026-05", "2026-06"],
        raw_dir=tmp_path,
        min_month_bytes=10,
    )

    assert state["state"] == "source_unavailable"
    assert state["required_for_conduct_core"] is False
    assert state["missing_months"] == ["2026-06"]


def test_taxonomy_builder_returns_empty_enrichment_when_source_is_missing(
    tmp_path: Path,
) -> None:
    state, counters, catalog = build_cached_taxonomy_enrichment(
        ["2026-05", "2026-06"],
        ["fip:000001"],
        lambda _provider, _month: {
            "resolution_state": "matched_current_insurer",
            "entity_id": "fip:000001",
        },
        core_source_month_totals={"2026-05": 1, "2026-06": 1},
        core_market_month_totals={"2026-05": 1, "2026-06": 1},
        raw_dir=tmp_path,
        min_month_bytes=10,
    )

    assert state["state"] == "source_unavailable"
    assert counters["fip:000001"]["2026-05"]["problema"] == {}
    assert catalog["problema"] == {}
