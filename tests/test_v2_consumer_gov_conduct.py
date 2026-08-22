from __future__ import annotations

import pytest

import api.v2.build_consumer_gov_conduct_evidence as conduct_builder
from api.v2.consumer_gov_conduct import (
    accumulate_row,
    build_conduct_film,
    finalize_month_evidence,
    new_month_evidence,
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
        "consumer_resolved_rate_among_evaluated": resolved / evaluated if evaluated else None,
        "satisfaction_count": satisfaction_count,
        "average_satisfaction": satisfaction if satisfaction_count else None,
        "response_time_count": complaints,
        "average_response_time_days": 5.0,
        "consumer_contacted_company_yes": complaints,
        "consumer_contacted_company_no": 0,
        "consumer_contacted_company_yes_rate": 1.0,
        "taxonomy": {
            "area": {"Serviços Financeiros": complaints},
            "assunto": {"Seguro": complaints},
            "grupo_problema": {group: complaints},
            "problema": {"Cobertura": complaints},
            "como_comprou_contratou": {"Internet": complaints},
            "canal_origem": {"Consumidor.gov.br": complaints},
        },
        "situacao": {"Finalizada avaliada": complaints},
        "avaliacao_reclamacao": {"Resolvida": resolved},
        "analise_recusa": {},
    }


def test_accumulate_row_preserves_outcome_and_taxonomy_semantics() -> None:
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
    assert month["responded"] == 1
    assert month["finalized"] == 1
    assert month["evaluated"] == 1
    assert month["consumer_resolved"] == 1
    assert month["consumer_resolved_rate_among_evaluated"] == pytest.approx(1.0)
    assert month["average_satisfaction"] == pytest.approx(4.0)
    assert month["average_response_time_days"] == pytest.approx(3.5)
    assert month["complaint_share_among_matched_current_insurers"] == pytest.approx(0.1)
    assert month["taxonomy"]["grupo_problema"]["Contrato / Oferta"] == 1
    assert month["taxonomy"]["problema"]["Oferta não cumprida"] == 1
    assert month["analise_recusa"]["Procedente"] == 1


def test_film_can_identify_improving_observed_pattern_without_score() -> None:
    months = []
    for index in range(12):
        early = index < 6
        complaints = 40 if early else 20
        resolved = 10 if early else 18
        satisfaction = 2.8 if early else 3.8
        months.append(
            _month(
                f"2026-{index + 1:02d}",
                complaints=complaints,
                market=1000,
                evaluated=20,
                resolved=resolved,
                satisfaction_count=20,
                satisfaction=satisfaction,
            )
        )

    film = build_conduct_film(months)

    assert "score" not in film
    assert film["history_state"] == "established"
    assert film["complaint_share_trend"]["direction"] == "falling"
    assert film["consumer_resolution_trend"]["direction"] == "improving"
    assert film["satisfaction_trend"]["direction"] == "improving"
    assert film["conduct_signal"] == "improving_observed_pattern"


def test_film_can_identify_persistent_observed_pressure_without_score() -> None:
    months = []
    for index in range(12):
        early = index < 6
        complaints = 15 if early else 35
        resolved = 18 if early else 10
        satisfaction = 4.0 if early else 3.0
        months.append(
            _month(
                f"2026-{index + 1:02d}",
                complaints=complaints,
                market=1000,
                evaluated=20,
                resolved=resolved,
                satisfaction_count=20,
                satisfaction=satisfaction,
            )
        )

    film = build_conduct_film(months)

    assert film["history_state"] == "established"
    assert film["complaint_share_trend"]["direction"] == "rising"
    assert film["consumer_resolution_trend"]["direction"] == "worsening"
    assert film["satisfaction_trend"]["direction"] == "worsening"
    assert film["conduct_signal"] == "persistent_observed_pressure"


def test_film_keeps_small_samples_indeterminate() -> None:
    months = [
        _month(
            f"2026-{index + 1:02d}",
            complaints=1,
            market=1000,
            evaluated=1,
            resolved=1,
            satisfaction_count=1,
            satisfaction=5.0,
        )
        for index in range(12)
    ]
    film = build_conduct_film(months)

    assert film["history_state"] == "limited"
    assert film["conduct_signal"] == "indeterminate"
    assert film["consumer_resolution_trend"]["direction"] == "insufficient"
    assert film["satisfaction_trend"]["direction"] == "insufficient"



def test_raw_csv_cache_is_used_without_ckan_lookup(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(conduct_builder, "RAW_DIR", tmp_path)
    monkeypatch.setattr(conduct_builder, "CG_MIN_MONTH_BYTES", 10)
    for month in ("2026-05", "2026-06"):
        (tmp_path / f"basecompleta_{month}.csv").write_bytes(b"x" * 20)

    def fail_if_called():
        raise AssertionError("CKAN discovery must not run when every raw month is cached")

    monkeypatch.setattr(conduct_builder, "_list_basecompleta_resources", fail_if_called)

    raw = conduct_builder._ensure_raw_csvs(["2026-05", "2026-06"])

    assert set(raw) == {"2026-05", "2026-06"}
    assert raw["2026-05"]["acquisition"] == "cache"
    assert raw["2026-06"]["acquisition"] == "cache"
    assert raw["2026-05"]["resource_url"] is None


def test_missing_raw_csv_reports_taxonomy_source_unavailable(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(conduct_builder, "RAW_DIR", tmp_path)
    monkeypatch.setattr(conduct_builder, "CG_MIN_MONTH_BYTES", 10)
    (tmp_path / "basecompleta_2026-05.csv").write_bytes(b"x" * 20)

    def fail_ckan():
        raise RuntimeError("temporary CKAN failure")

    monkeypatch.setattr(conduct_builder, "_list_basecompleta_resources", fail_ckan)

    with pytest.raises(
        conduct_builder.TaxonomyRawSourceUnavailable,
        match="taxonomy_raw_source_unavailable",
    ) as exc_info:
        conduct_builder._ensure_raw_csvs(["2026-05", "2026-06"])

    assert "2026-06" in str(exc_info.value)
