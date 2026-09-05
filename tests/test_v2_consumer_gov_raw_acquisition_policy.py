from __future__ import annotations

import pytest

import api.v2.acquire_consumer_gov_raw as acquisition
from api.sources.consumer_gov_direct import ConsumerGovPublicationNotFound, Publication


def _publication(title: str, filename: str | None) -> Publication:
    return Publication(
        code="20260800000010944",
        title=title,
        filename=filename,
        published_at="2026-08-23",
        month="2026-06",
        discovery_method="test",
    )


def test_explicit_basecompleta_policy_rejects_finalizadas_and_generic_dados() -> None:
    assert acquisition.is_explicit_basecompleta(
        _publication(
            "Base Completa Consumidor.gov.br - Junho_2026",
            "basecompleta2026-06.csv",
        )
    )
    assert not acquisition.is_explicit_basecompleta(
        _publication(
            "Reclamações finalizadas em Junho de 2026",
            "finalizadas_2026-06.zip",
        )
    )
    assert not acquisition.is_explicit_basecompleta(
        _publication("Dados - Jun/2026", "dados_2026-06.zip")
    )


def test_missing_basecompleta_candidate_fails_before_download(monkeypatch) -> None:
    monkeypatch.setattr(acquisition, "_uncached_months", lambda _months: ["2026-06"])
    monkeypatch.setattr(
        acquisition,
        "discover_publications",
        lambda _months: [
            _publication(
                "Reclamações finalizadas em Junho de 2026",
                "finalizadas_2026-06.zip",
            )
        ],
    )

    with pytest.raises(ConsumerGovPublicationNotFound, match="not accepted substitutes"):
        acquisition.require_explicit_basecompleta_candidates(["2026-06"])
