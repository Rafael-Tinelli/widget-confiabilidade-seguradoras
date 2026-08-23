from __future__ import annotations

import csv
import json
import zipfile
from pathlib import Path

import pytest

from api.sources import consumer_gov_direct as direct


def _valid_csv(
    path: Path,
    *,
    month: str = "2026-03",
    rows: int = 3,
) -> None:
    columns = [
        "Nome Fantasia",
        "Segmento de Mercado",
        "Respondida",
        "Situação",
        "Avaliação Reclamação",
        "Nota do Consumidor",
        "Tempo Resposta",
        "Procurou Empresa",
        "Área",
        "Assunto",
        "Grupo Problema",
        "Problema",
        "Como Comprou Contratou",
        "Canal de Origem",
        "Ano Abertura",
        "Mês Abertura",
    ]
    year, month_number = month.split("-")
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, delimiter=";")
        writer.writeheader()
        for index in range(rows):
            writer.writerow(
                {
                    "Nome Fantasia": f"Empresa {index}",
                    "Segmento de Mercado": (
                        "Seguros, Capitalização e Previdência"
                    ),
                    "Respondida": "S",
                    "Situação": "Finalizada avaliada",
                    "Avaliação Reclamação": "Resolvida",
                    "Nota do Consumidor": "4",
                    "Tempo Resposta": "5",
                    "Procurou Empresa": "S",
                    "Área": "Serviços Financeiros",
                    "Assunto": "Seguro",
                    "Grupo Problema": "Contrato / Oferta",
                    "Problema": "Oferta não cumprida",
                    "Como Comprou Contratou": "Internet",
                    "Canal de Origem": "Consumidor.gov.br",
                    "Ano Abertura": year,
                    "Mês Abertura": str(int(month_number)),
                }
            )


def test_extracts_generated_datatables_and_ajax_urls() -> None:
    page = (
        '<script src="//consumidor.gov.br/datatablesController/'
        'datatables-67143.min.js?id=publicacoesDT&amp;t=main&amp;'
        'c=%2Fpages%2Fdadosabertos%2Fexterno%2F"></script>'
    )
    urls = direct._datatables_script_urls(page)
    assert urls == [
        (
            "https://consumidor.gov.br/datatablesController/"
            "datatables-67143.min.js?id=publicacoesDT&t=main&"
            "c=%2Fpages%2Fdadosabertos%2Fexterno%2F"
        )
    ]

    js = (
        'var o={"sAjaxSource":"/pages/dadosabertos/externo/listar",'
        '"bServerSide":true};'
    )
    assert direct._ajax_urls(js, urls[0]) == [
        "https://consumidor.gov.br/pages/dadosabertos/externo/listar"
    ]


def test_publication_record_and_dom_are_resolved_semantically() -> None:
    record = {
        "codigo": "20260400000012345",
        "texto": "Dados - Março de 2026",
        "nomeArquivo": "basecompleta2026-03.zip",
        "dataPublicacao": "06/04/2026",
    }
    publication = direct._publication_from_record(record, "structured_http")
    assert publication is not None
    assert publication.month == "2026-03"
    assert publication.code == "20260400000012345"
    assert direct._is_monthly_data_publication(publication)

    dom = direct._dom_publication(
        "Dados - Março de 2026 06/04/2026",
        "download('20260400000012345');",
        "Download 'basecompleta2026-03.zip'",
    )
    assert dom is not None
    assert dom.month == "2026-03"
    assert dom.filename == "basecompleta2026-03.zip"


def test_schema_validation_accepts_required_taxonomy(
    tmp_path,
    monkeypatch,
) -> None:
    path = tmp_path / "basecompleta_2026-03.csv"
    _valid_csv(path, rows=3)
    monkeypatch.setattr(direct, "MIN_MONTH_BYTES", 1)

    result = direct.validate_month_csv(path, "2026-03")

    assert result.rows == 3
    assert result.month_observations == 3
    assert result.month_matches == 3
    assert result.field_map["problem_group"] == "Grupo Problema"
    assert result.field_map["problem"] == "Problema"


def test_schema_validation_fails_closed_when_taxonomy_disappears(
    tmp_path,
    monkeypatch,
) -> None:
    path = tmp_path / "basecompleta_2026-03.csv"
    path.write_text(
        "Nome Fantasia;Segmento de Mercado;Respondida\n"
        "Empresa;Seguros;S\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(direct, "MIN_MONTH_BYTES", 1)

    with pytest.raises(direct.ConsumerGovSchemaMismatch) as exc_info:
        direct.validate_month_csv(path, "2026-03")

    assert "consumer_gov_schema_mismatch" in str(exc_info.value)


def test_zip_download_is_materialized_and_validated(
    tmp_path,
    monkeypatch,
) -> None:
    csv_path = tmp_path / "inside.csv"
    _valid_csv(csv_path, rows=2)
    archive = tmp_path / "download.zip"
    with zipfile.ZipFile(archive, "w") as handle:
        handle.write(csv_path, arcname="basecompleta2026-03.csv")

    destination = tmp_path / "basecompleta_2026-03.csv"
    monkeypatch.setattr(direct, "MIN_MONTH_BYTES", 1)

    direct._materialize_download(archive, destination)
    result = direct.validate_month_csv(destination, "2026-03")

    assert result.rows == 2
    assert destination.exists()


def test_permanent_manifest_short_circuits_discovery(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(direct, "MIN_MONTH_BYTES", 1)
    path = tmp_path / "basecompleta_2026-03.csv"
    _valid_csv(path, rows=2)

    first = direct.ensure_months(
        ["2026-03"],
        raw_dir=tmp_path,
        manifest_path=tmp_path / "manifest.json",
    )
    assert first["2026-03"]["acquisition"] == "cache_schema_validated"

    def fail_discovery(_wanted):
        raise AssertionError(
            "discovery must not run for validated permanent cache"
        )

    monkeypatch.setattr(direct, "discover_publications", fail_discovery)
    second = direct.ensure_months(
        ["2026-03"],
        raw_dir=tmp_path,
        manifest_path=tmp_path / "manifest.json",
    )

    assert second["2026-03"]["acquisition"] == "cache_manifest"
    manifest = json.loads(
        (tmp_path / "manifest.json").read_text(encoding="utf-8")
    )
    assert manifest["months"]["2026-03"]["validation"] == "passed"


def test_explicit_error_when_month_cannot_be_resolved(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(direct, "MIN_MONTH_BYTES", 1)
    monkeypatch.setattr(direct, "discover_publications", lambda _wanted: [])

    with pytest.raises(direct.ConsumerGovPublicationNotFound) as exc_info:
        direct.ensure_months(
            ["2026-03"],
            raw_dir=tmp_path,
            manifest_path=tmp_path / "manifest.json",
        )

    assert "consumer_gov_publication_not_found" in str(exc_info.value)
