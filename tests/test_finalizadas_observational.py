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



class _OfficialResponse:
    def __init__(self, records, *, type_="application/json;charset=ISO-8859-1",
                 redirect=False):
        self._raw = json.dumps(records, ensure_ascii=False).encode("iso-8859-1")
        self.headers = {"Content-Type": type_}
        self.encoding = "iso-8859-1"
        self.is_redirect = redirect

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def raise_for_status(self):
        return None

    def iter_content(self, chunk_size=65536):
        for start in range(0, len(self._raw), chunk_size):
            yield self._raw[start:start + chunk_size]


class _OfficialSession:
    def __init__(self, records, **response_options):
        self.records = records
        self.options = response_options
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return _OfficialResponse(self.records, **self.options)


def _official(month, code, *, deleted=False, size=15_692_004):
    return {
        "nomeArquivo": f"finalizadas_{month}.zip",
        "codigo": code,
        "tamanhoArquivo": size,
        "indicadorExclusaoLogica": deleted,
        "dataPublicacao": "06/11/2026 às 15:13",
        "dataAlteracao": "06/11/2026 às 15:13",
        "titulo": "Dados - Outubro/2026",
        "texto": "Arquivo em formato CSV com dados de todas as reclamações finalizadas.",
    }


def test_official_iso_json_auto_discovers_month_and_merges_pinned_sha(tmp_path):
    source = tmp_path / "catalog.json"
    known_hash = "963489d07299c5f762476ca9f6cef21edb55337ac9cb1caa7809a68efd7fcdc1"
    source.write_text(json.dumps({"publications": [
        {"month": "2026-10", "url": URL, "expected_sha256": known_hash}
    ]}), encoding="utf-8")
    new_code = "20261200000011199"
    session = _OfficialSession([
        {"nomeArquivo": "usuarios_consolidados.csv", "codigo": "20261100000011109"},
        _official("2026-10", "20261100000011098"),
        _official("2026-11", new_code),
        _official("2026-08", "20260900000010980", deleted=True),
    ])
    selected = obs.read_catalog(
        source, feed_url=obs.OFFICIAL_CATALOG_URL, http=session
    )
    assert [x["month"] for x in selected] == ["2026-10", "2026-11"]
    assert selected[0]["expected_sha256"] == known_hash
    assert selected[0]["announced_bytes"] == 15_692_004
    assert selected[1]["url"] == (
        f"https://www.consumidor.gov.br/pages/publicacao/externo/{new_code}/download"
    )
    assert selected[1]["discovery"] == "official_publicacoes_json"
    assert len(session.calls) == 1
    assert session.calls[0][0] == obs.OFFICIAL_CATALOG_URL
    assert session.calls[0][1]["allow_redirects"] is False


def test_official_feed_conflicting_codes_fail_closed(tmp_path):
    catalog = tmp_path / "catalog.json"
    catalog.write_text('{"publications":[]}', encoding="utf-8")
    session = _OfficialSession([
        _official("2026-10", "20261100000011098"),
        _official("2026-10", "20261100000011999"),
    ])
    with pytest.raises(obs.ObservationalError, match="Mais de uma publicação"):
        obs.read_catalog(catalog, feed_url=obs.OFFICIAL_CATALOG_URL, http=session)


@pytest.mark.parametrize("url", [
    "https://example.com/pages/publicacao/externo/publicacoes.json?indicadorTipoPublicacao=2",
    "https://www.consumidor.gov.br/pages/dadosabertos/externo/",
    "https://www.consumidor.gov.br/pages/publicacao/externo/publicacoes.json?indicadorTipoPublicacao=1",
    "https://www.consumidor.gov.br/pages/publicacao/externo/publicacoes.json?indicadorTipoPublicacao=2&other=1",
])
def test_official_feed_must_be_exact_public_json_route(url):
    with pytest.raises(obs.ObservationalError):
        obs._checked_feed_url(url)


def test_official_non_json_response_and_redirect_rejected(tmp_path):
    catalog = tmp_path / "catalog.json"
    catalog.write_text('{"publications":[]}', encoding="utf-8")
    records = [_official("2026-10", "20261100000011098")]
    for session in (
        _OfficialSession(records, type_="text/html"),
        _OfficialSession(records, redirect=True),
    ):
        with pytest.raises(obs.ObservationalError):
            obs.read_catalog(catalog, feed_url=obs.OFFICIAL_CATALOG_URL,
                             http=session)


def test_official_publication_same_month_as_pinned_different_code_rejected(tmp_path):
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps({"publications": [
        {"month": "2026-10", "url": URL}
    ]}), encoding="utf-8")
    session = _OfficialSession([_official("2026-10", "20261100000011999")])
    with pytest.raises(obs.ObservationalError, match="diverge"):
        obs.read_catalog(catalog, feed_url=obs.OFFICIAL_CATALOG_URL, http=session)


def test_official_revision_signaled_for_metadata_size_mismatch(tmp_path, monkeypatch):
    monkeypatch.setattr(obs, "MIN_CSV_BYTES", 0)
    source = _zip(tmp_path / "source.zip", [_row()])
    source_hash = obs.sha256(source)
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps({"publications": [
        {"month": "2026-10", "url": URL}
    ]}), encoding="utf-8")

    def download(_url, dest, http=None):
        shutil.copyfile(source, dest)
        return source_hash

    def catalog_reader(_path, feed_url=None):
        return [{
            "month": "2026-10", "url": URL, "expected_sha256": "",
            "publication_code": "20261100000011098",
            "announced_bytes": 15000000,
            "discovery": "official_publicacoes_json",
        }]

    monkeypatch.setattr(obs, "download_direct", download)
    monkeypatch.setattr(obs, "read_catalog", catalog_reader)
    output = tmp_path / "isolated"
    assert obs.main(["--catalog", str(catalog), "--output-dir", str(output)]) == 0
    manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["revisions"] == ["2026-10"]
    assert manifest["months"]["2026-10"]["status"] == "requires_manual_review"



def test_september_2026_legacy_schema_iso_date_and_unreported_resolution(tmp_path, monkeypatch):
    """Reproduz os dois esquemas oficiais, preservando a ausência de resolução."""
    monkeypatch.setattr(obs, "MIN_CSV_BYTES", 0)
    rows = [
        _row(data_finalizacao="2026-09-09", respondida="S",
             avaliacao_reclamacao="", nota_do_consumidor="1"),
        _row(data_finalizacao="2026-09-14", respondida="N",
             situacao="Finalizada não avaliada", avaliacao_reclamacao="Não Avaliada",
             nota_do_consumidor=""),
    ]
    translated = [
        {legacy: row[canonical] for canonical, legacy in zip(
            obs.COLUMNS, obs.LEGACY_COLUMNS, strict=True
        )}
        for row in rows
    ]
    src = _zip(
        tmp_path / "september.zip", translated,
        columns=list(obs.LEGACY_COLUMNS),
        member="finalizadas_2026-09.csv",
    )
    aggregate, evidence = obs.aggregate_zip(src, "2026-09", URL)
    assert evidence["source_schema"] == "legacy_portuguese_19"
    assert evidence["rows_segment"] == 2
    assert evidence["responded"] == 1
    assert evidence["evaluated"] == 1
    assert evidence["resolved"] == 0
    assert evidence["unresolved"] == 0
    assert evidence["resolution_unknown"] == 1
    assert evidence["status"] == "requires_manual_review"
    counts = aggregate["by_name_key_raw"]["alfa seguros"]["statistics"]
    assert counts["complaintsCount"] == 2
    assert counts["satisfactionCount"] == 1
    assert counts["scoreSum"] == 1
    assert counts["resolvedCount"] == 0
    assert aggregate["meta"]["resolution_unknown_count"] == 1


def test_only_evaluated_legacy_rows_can_lack_resolution(tmp_path, monkeypatch):
    monkeypatch.setattr(obs, "MIN_CSV_BYTES", 0)
    row = _row(
        data_finalizacao="2026-09-09", respondida="N",
        situacao="Finalizada não avaliada", avaliacao_reclamacao="",
        nota_do_consumidor="",
    )
    translated = {
        legacy: row[canonical] for canonical, legacy in zip(
            obs.COLUMNS, obs.LEGACY_COLUMNS, strict=True
        )
    }
    src = _zip(
        tmp_path / "september.zip", [translated],
        columns=list(obs.LEGACY_COLUMNS),
        member="finalizadas_2026-09.csv",
    )
    with pytest.raises(obs.ObservationalError, match="Avaliação inconsistente"):
        obs.aggregate_zip(src, "2026-09", URL)
