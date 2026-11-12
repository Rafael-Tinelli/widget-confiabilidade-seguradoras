"""Contrato OBSERVACIONAL: download oficial, schema e não publicação."""
from __future__ import annotations

import csv
import io
import json
import shutil
import zipfile
from pathlib import Path

import pytest

from api.probes import finalizadas_observational as obs

URL = "https://www.consumidor.gov.br/pages/publicacao/externo/20261100000011098/download"


def _row(**overrides: str) -> dict[str, str]:
    row = dict.fromkeys(obs.COLUMNS, "")
    row.update({
        "data_finalizacao": "14/10/2026",
        "segmento_de_mercado": obs.TARGET,
        "nome_fantasia": "Alfa Seguros",
        "respondida": "Sim",
        "situacao": "Finalizada avaliada",
        "avaliacao_reclamacao": "Resolvida",
        "nota_do_consumidor": "5",
    })
    row.update(overrides)
    return row


def _zip(path: Path, rows: list[dict[str, str]], *,
         columns: list[str] | None = None, member: str = "finalizadas_2026-10.csv") -> Path:
    text = io.StringIO(newline="")
    writer = csv.DictWriter(text, fieldnames=columns or list(obs.COLUMNS),
                            delimiter=";", extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(member, text.getvalue().encode("utf-8-sig"))
    return path


def test_contract_october_two_providers_and_legacy_statistics(tmp_path, monkeypatch):
    monkeypatch.setattr(obs, "MIN_CSV_BYTES", 0)
    archive = _zip(tmp_path / "input.zip", [
        _row(),
        _row(nome_fantasia="Beta Capitalização", respondida="Não",
             situacao="Finalizada não avaliada", avaliacao_reclamacao="Não Avaliada",
             nota_do_consumidor=""),
        _row(segmento_de_mercado="Telecomunicações", nome_fantasia="Fora do recorte"),
    ])
    agg, evidence = obs.aggregate_zip(archive, "2026-10", URL)
    assert evidence["rows_total"] == 3
    assert evidence["rows_segment"] == 2
    assert evidence["providers"] == 2
    assert evidence["responded"] == 1
    assert evidence["evaluated"] == evidence["resolved"] == 1
    assert evidence["unassessed"] == 1
    assert agg["meta"]["source_family"] == "finalizadas"
    assert agg["meta"]["reference_event"] == "finalizacao"
    assert agg["meta"]["status"] == "experimental"
    assert "by_name_key" not in agg
    assert agg["by_name_key_raw"]["alfa seguros"]["statistics"]["scoreSum"] == 5
    assert agg["by_name_key_raw"]["beta capitalizacao"]["statistics"]["finalizedCount"] == 1


@pytest.mark.parametrize("change", [
    {"data_finalizacao": "01/09/2026"},
    {"respondida": "S"},
    {"situacao": "Em andamento"},
    {"avaliacao_reclamacao": "Não Avaliada"},
    {"nota_do_consumidor": "0"},
    {"nome_fantasia": ""},
])
def test_fail_closed_if_invalid_row(tmp_path, monkeypatch, change):
    monkeypatch.setattr(obs, "MIN_CSV_BYTES", 0)
    archive = _zip(tmp_path / "invalid.zip", [_row(**change)])
    with pytest.raises(obs.ObservationalError):
        obs.aggregate_zip(archive, "2026-10", URL)


def test_wrong_csv_name_and_schema_rejected(tmp_path, monkeypatch):
    monkeypatch.setattr(obs, "MIN_CSV_BYTES", 0)
    source = _zip(tmp_path / "name.zip", [_row()], member="../finalizadas_2026-10.csv")
    with pytest.raises(obs.ObservationalError, match="único CSV"):
        obs.aggregate_zip(source, "2026-10", URL)
    source = _zip(tmp_path / "schema.zip", [_row()], columns=list(obs.COLUMNS[:-1]))
    with pytest.raises(obs.ObservationalError, match="Schema"):
        obs.aggregate_zip(source, "2026-10", URL)


@pytest.mark.parametrize("url", [
    "http://www.consumidor.gov.br/pages/publicacao/externo/20261100000011098/download",
    "https://evil.example/pages/publicacao/externo/20261100000011098/download",
    "https://www.consumidor.gov.br.evil.example/pages/publicacao/externo/20261100000011098/download",
    "https://www.consumidor.gov.br/pages/publicacao/externo/guess/download",
    "https://www.consumidor.gov.br/pages/publicacao/externo/20261100000011098/download?token=x",
    "https://www.consumidor.gov.br/pages/dadosabertos/externo/",
])
def test_official_url_allowlist(url):
    with pytest.raises(obs.ObservationalError):
        obs.official_url(url)


def test_catalog_conflict_and_unknown_month(tmp_path):
    catalog = tmp_path / "source.json"
    catalog.write_text(json.dumps({"publications": [
        {"month": "2026-10", "url": URL},
        {"month": "2026-10", "url": URL + "?x=y"},
    ]}), encoding="utf-8")
    with pytest.raises(obs.ObservationalError):
        obs.read_catalog(catalog)
    catalog.write_text(json.dumps({"publications": [
        {"month": "2026-13", "url": URL},
    ]}), encoding="utf-8")
    with pytest.raises(obs.ObservationalError):
        obs.read_catalog(catalog)


def test_previous_hash_difference_is_recorded_not_published(tmp_path, monkeypatch):
    monkeypatch.setattr(obs, "MIN_CSV_BYTES", 0)
    source = _zip(tmp_path / "source.zip", [_row()])
    digest = obs.sha256(source)
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps({"publications": [
        {"month": "2026-10", "url": URL}
    ]}), encoding="utf-8")
    previous = tmp_path / "previous.json"
    previous.write_text(json.dumps({"months": {
        "2026-10": {"zip_sha256": "a" * 64}
    }}), encoding="utf-8")

    def download(_url, dest, http=None):
        shutil.copyfile(source, dest)
        return digest

    monkeypatch.setattr(obs, "download_direct", download)
    out = tmp_path / "experiment"
    assert obs.main(["--catalog", str(catalog), "--output-dir", str(out),
                     "--previous-manifest", str(previous)]) == 0
    manifest = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["revisions"] == ["2026-10"]
    assert manifest["months"]["2026-10"]["status"] == "requires_manual_review"
    aggregate = json.loads((out / "2026-10" / "agregado-experimental.json").read_text())
    assert aggregate["meta"]["publication_status"] == "isolated_not_for_publication"


def test_deterministic_aggregation_same_file(tmp_path, monkeypatch):
    monkeypatch.setattr(obs, "MIN_CSV_BYTES", 0)
    source = _zip(tmp_path / "source.zip", [_row(), _row()])
    assert obs.aggregate_zip(source, "2026-10", URL) == obs.aggregate_zip(
        source, "2026-10", URL
    )


def test_block_writing_under_production_data(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(obs.ObservationalError, match="protegida"):
        obs._safe_output(Path("data/derived/consumidor_gov/monthly"))
