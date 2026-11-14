"""Contrato OBSERVACIONAL: download oficial, schema e não publicação."""
from __future__ import annotations

import csv
import io
import json
import shutil
import zipfile
from pathlib import Path

import pytest
import requests

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
    previous.write_text(json.dumps({
        "schema": "finalizadas-observational-1",
        "months": {"2026-10": {"zip_sha256": "a" * 64}},
    }), encoding="utf-8")

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


def test_review_remains_pending_on_second_identical_observation(tmp_path, monkeypatch):
    """A revisão não pode aprovar a si mesma na execução seguinte."""
    monkeypatch.setattr(obs, "MIN_CSV_BYTES", 0)
    archive = _zip(tmp_path / "candidate.zip", [_row()])
    digest = obs.sha256(archive)
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps({"publications": [
        {"month": "2026-10", "url": URL}
    ]}), encoding="utf-8")
    previous = tmp_path / "before.json"
    previous.write_text(json.dumps({
        "schema": "finalizadas-observational-1",
        "months": {"2026-10": {"zip_sha256": "a" * 64}},
    }), encoding="utf-8")

    def download(_url, dest, http=None):
        shutil.copyfile(archive, dest)
        return digest

    monkeypatch.setattr(obs, "download_direct", download)
    first = tmp_path / "first"
    second = tmp_path / "second"
    assert obs.main(["--catalog", str(catalog), "--output-dir", str(first),
                     "--previous-manifest", str(previous)]) == 0
    assert obs.main(["--catalog", str(catalog), "--output-dir", str(second),
                     "--previous-manifest", str(first / "manifest.json")]) == 0
    state = json.loads((second / "manifest.json").read_text(encoding="utf-8"))
    assert state["months"]["2026-10"]["status"] == "requires_manual_review"
    assert state["months"]["2026-10"]["review_origin_sha256"] == "a" * 64
    assert state["revisions"] == ["2026-10"]


def test_catalog_same_code_conflicting_metadata_rejected(tmp_path):
    catalog = tmp_path / "catalog.json"
    catalog.write_text('{"publications":[]}', encoding="utf-8")
    first = _official("2026-10", "20261100000011098", size=15_692_004)
    second = _official("2026-10", "20261100000011098", size=15_692_005)
    with pytest.raises(obs.ObservationalError, match="Metadados oficiais conflitantes"):
        obs.read_catalog(
            catalog, feed_url=obs.OFFICIAL_CATALOG_URL,
            http=_OfficialSession([first, second])
        )


def test_pinned_month_withdrawn_is_not_downloaded(tmp_path, monkeypatch):
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps({"publications": [
        {"month": "2026-10", "url": URL}
    ]}), encoding="utf-8")
    monkeypatch.setattr(obs, "download_direct", lambda *a, **k:
                        pytest.fail("Withdrawn month must never be downloaded"))
    monkeypatch.setattr(obs, "_fetch_official_feed", lambda *a, **k: [
        {"month": "2026-10", "withdrawn": True}
    ])
    output = tmp_path / "out"
    assert obs.main(["--catalog", str(catalog),
                     "--feed-url", obs.OFFICIAL_CATALOG_URL,
                     "--output-dir", str(output)]) == 0
    state = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
    assert state["months"]["2026-10"]["catalog_status"] == "withdrawn"
    assert state["months"]["2026-10"]["status"] == "requires_manual_review"


def test_pinned_month_absent_from_partial_feed_is_not_accepted(tmp_path, monkeypatch):
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps({"publications": [
        {"month": "2026-10", "url": URL}
    ]}), encoding="utf-8")
    monkeypatch.setattr(obs, "_fetch_official_feed", lambda *a, **k: [
        {"month": "2026-09", "url": URL, "expected_sha256": ""}
    ])
    selection = obs.read_catalog(catalog, feed_url=obs.OFFICIAL_CATALOG_URL)
    assert selection[-1]["catalog_status"] == "absent_from_feed"


def test_invalid_previous_manifest_fails_closed(tmp_path, monkeypatch):
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps({"publications": [
        {"month": "2026-10", "url": URL}
    ]}), encoding="utf-8")
    previous = tmp_path / "bad.json"
    previous.write_text('{"months":{"2026-10":{"zip_sha256":"bad"}}}', encoding="utf-8")
    with pytest.raises(obs.ObservationalError, match="inválido"):
        obs.main(["--catalog", str(catalog), "--output-dir", str(tmp_path / "out"),
                  "--previous-manifest", str(previous)])


def test_quality_alert_is_not_mislabeled_as_source_revision(tmp_path, monkeypatch):
    """Setembro: resolução não informada exige revisão, não mudança de ZIP."""
    monkeypatch.setattr(obs, "MIN_CSV_BYTES", 0)
    row = _row(data_finalizacao="2026-09-09", respondida="S",
               avaliacao_reclamacao="", nota_do_consumidor="1")
    legacy = {older: row[canonical] for canonical, older in zip(
        obs.COLUMNS, obs.LEGACY_COLUMNS, strict=True
    )}
    archive = _zip(tmp_path / "september.zip", [legacy],
                   columns=list(obs.LEGACY_COLUMNS),
                   member="finalizadas_2026-09.csv")
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps({"publications": [
        {"month": "2026-09", "url": URL}
    ]}), encoding="utf-8")

    def download(_url, target, http=None):
        shutil.copyfile(archive, target)
        return obs.sha256(archive)

    monkeypatch.setattr(obs, "download_direct", download)
    first = tmp_path / "first"
    second = tmp_path / "second"
    assert obs.main(["--catalog", str(catalog), "--output-dir", str(first)]) == 0
    assert obs.main(["--catalog", str(catalog), "--output-dir", str(second),
                     "--previous-manifest", str(first / "manifest.json")]) == 0
    for folder in (first, second):
        state = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
        assert state["revisions"] == []
        assert state["quality_alerts"] == ["2026-09"]
        assert state["months"]["2026-09"]["revision"] is False
        assert state["months"]["2026-09"]["status"] == "requires_manual_review"


def test_metadata_only_change_requires_manual_review(tmp_path, monkeypatch):
    monkeypatch.setattr(obs, "MIN_CSV_BYTES", 0)
    archive = _zip(tmp_path / "october.zip", [_row()])
    file_hash = obs.sha256(archive)
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps({"publications": [
        {"month": "2026-10", "url": URL}
    ]}), encoding="utf-8")
    previous = tmp_path / "previous.json"
    previous.write_text(json.dumps({
        "schema": "finalizadas-observational-1", "months": {
            "2026-10": {"zip_sha256": file_hash,
                        "status": "validated_experimental",
                        "source_url": URL,
                        "modified_at": "01/11/2026 às 14:00"}
        }
    }), encoding="utf-8")

    def download(_url, target, http=None):
        shutil.copyfile(archive, target)
        return file_hash

    monkeypatch.setattr(obs, "download_direct", download)
    monkeypatch.setattr(obs, "read_catalog", lambda _path, feed_url=None: [
        {"month": "2026-10", "url": URL, "expected_sha256": "",
         "modified_at": "08/11/2026 às 14:00",
         "publication_code": "20261100000011098",
         "announced_bytes": archive.stat().st_size}
    ])
    output = tmp_path / "out"
    assert obs.main(["--catalog", str(catalog), "--output-dir", str(output),
                     "--previous-manifest", str(previous)]) == 0
    manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
    state = manifest["months"]["2026-10"]
    assert state["revision"] is True
    assert state["status"] == "requires_manual_review"
    assert manifest["revisions"] == ["2026-10"]


class _TransientOfficialSession(_OfficialSession):
    def __init__(self, records, outcomes):
        super().__init__(records)
        self.outcomes = list(outcomes)

    def get(self, url, **kwargs):
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            self.calls.append((url, kwargs))
            raise outcome
        return super().get(url, **kwargs)


def test_official_feed_504_retry_then_fresh_json(tmp_path, monkeypatch):
    """Only a successful NEW official GET unlocks publication discovery."""
    catalog = tmp_path / "catalog.json"
    catalog.write_text('{"publications":[]}', encoding="utf-8")
    session = _TransientOfficialSession(
        [_official("2026-10", "20261100000011098")],
        [requests.exceptions.RetryError("too many 504 error responses"), "ok"],
    )
    sleeps = []
    monkeypatch.setattr(obs.time, "sleep", sleeps.append)
    discovered = obs.read_catalog(
        catalog, feed_url=obs.OFFICIAL_CATALOG_URL, http=session,
    )
    assert [item["month"] for item in discovered] == ["2026-10"]
    assert len(session.calls) == 2
    assert sleeps == [obs.CATALOG_RETRY_PAUSE_SECONDS]
    assert all(call[1]["allow_redirects"] is False for call in session.calls)


def test_official_feed_504_exhaustion_does_not_use_pinned_stale_data(
    tmp_path, monkeypatch,
):
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps({"publications": [
        {"month": "2026-10", "url": URL},
    ]}), encoding="utf-8")
    session = _TransientOfficialSession(
        [],
        [requests.exceptions.RetryError("504"),
         requests.exceptions.RetryError("504")],
    )
    sleeps = []
    monkeypatch.setattr(obs.time, "sleep", sleeps.append)
    with pytest.raises(obs.ObservationalError, match="indisponível"):
        obs.read_catalog(
            catalog, feed_url=obs.OFFICIAL_CATALOG_URL, http=session,
        )
    assert len(session.calls) == 2
    assert sleeps == [obs.CATALOG_RETRY_PAUSE_SECONDS]


@pytest.mark.parametrize("code,expected_calls", [(504, 2), (403, 1)])
def test_direct_http_error_only_504_is_retried(
    tmp_path, monkeypatch, code, expected_calls,
):
    catalog = tmp_path / "catalog.json"
    catalog.write_text('{"publications":[]}', encoding="utf-8")
    response = requests.Response()
    response.status_code = code
    session = _TransientOfficialSession(
        [], [requests.exceptions.HTTPError(
            f"HTTP {code}", response=response
        ) for _ in range(expected_calls)],
    )
    monkeypatch.setattr(obs.time, "sleep", lambda _seconds: None)
    with pytest.raises(
        obs.ObservationalError if code == 504 else requests.exceptions.HTTPError
    ):
        obs.read_catalog(
            catalog, feed_url=obs.OFFICIAL_CATALOG_URL, http=session,
        )
    assert len(session.calls) == expected_calls


def test_invalid_official_json_is_not_retried(tmp_path, monkeypatch):
    catalog = tmp_path / "catalog.json"
    catalog.write_text('{"publications":[]}', encoding="utf-8")
    session = _OfficialSession([_official("2026-10", "20261100000011098")],
                               type_="text/html")
    monkeypatch.setattr(obs.time, "sleep", lambda _seconds:
                        pytest.fail("Non-JSON must fail immediately"))
    with pytest.raises(obs.ObservationalError, match="Content-Type"):
        obs.read_catalog(
            catalog, feed_url=obs.OFFICIAL_CATALOG_URL, http=session,
        )
    assert len(session.calls) == 1


def test_a07_second_month_failure_does_not_promote_partial_bundle(tmp_path, monkeypatch):
    """Month one must not leak into the accepted output when month two fails."""
    monkeypatch.setattr(obs, "MIN_CSV_BYTES", 0)
    september = _zip(
        tmp_path / "sep.zip", [_row(data_finalizacao="17/09/2026")],
        member="finalizadas_2026-09.csv",
    )
    october = _zip(tmp_path / "oct.zip", [_row()])
    catalog = tmp_path / "catalog.json"
    catalog.write_text('{"publications":[]}', encoding="utf-8")
    monkeypatch.setattr(obs, "read_catalog", lambda *_args, **_kwargs: [
        {"month": "2026-09", "url": URL, "expected_sha256": ""},
        {"month": "2026-10", "url": URL, "expected_sha256": ""},
    ])
    calls = []

    def download(_url, target, http=None):
        calls.append(target.parent.name)
        if len(calls) == 2:
            raise obs.ObservationalError("partial second month")
        shutil.copyfile(september, target)
        return obs.sha256(september)

    monkeypatch.setattr(obs, "download_direct", download)
    output = tmp_path / "accepted"
    with pytest.raises(obs.ObservationalError, match="partial second month"):
        obs.main(["--catalog", str(catalog), "--output-dir", str(output)])
    assert calls == ["2026-09", "2026-10"]
    assert not output.exists()
    assert not list(tmp_path.glob(".finalizadas-stage-*"))
    assert october.exists()


def test_a07_accepted_output_cannot_be_replaced(tmp_path, monkeypatch):
    catalog = tmp_path / "catalog.json"
    catalog.write_text('{"publications":[]}', encoding="utf-8")
    output = tmp_path / "accepted"
    output.mkdir()
    sentinel = output / "manifest.json"
    sentinel.write_bytes(b"previous valid manifest")
    monkeypatch.setattr(obs, "read_catalog", lambda *_a, **_k:
                        pytest.fail("Existing bundle must be rejected before acquisition"))
    with pytest.raises(obs.ObservationalError, match="sobrescrita proibida"):
        obs.main(["--catalog", str(catalog), "--output-dir", str(output)])
    assert sentinel.read_bytes() == b"previous valid manifest"
    assert not list(tmp_path.glob(".finalizadas-stage-*"))


def test_a07_manifest_write_failure_leaves_no_accepted_bundle(tmp_path, monkeypatch):
    monkeypatch.setattr(obs, "MIN_CSV_BYTES", 0)
    archive = _zip(tmp_path / "src.zip", [_row()])
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps({"publications": [{"month": "2026-10", "url": URL}]}))
    monkeypatch.setattr(obs, "download_direct", lambda _url, path, http=None: (
        shutil.copyfile(archive, path), obs.sha256(archive)
    )[1])
    original = obs._write_json

    def interrupted_write(path, data):
        if path.name == "manifest.json":
            raise OSError("simulated interruption before commit")
        return original(path, data)

    monkeypatch.setattr(obs, "_write_json", interrupted_write)
    output = tmp_path / "accepted"
    with pytest.raises(OSError, match="simulated interruption"):
        obs.main(["--catalog", str(catalog), "--output-dir", str(output)])
    assert not output.exists()
    assert not list(tmp_path.glob(".finalizadas-stage-*"))


def test_a07_protection_anchored_to_repository_not_current_cwd(tmp_path, monkeypatch):
    checkout = Path(obs.__file__).resolve().parents[2]
    monkeypatch.chdir(tmp_path)
    with pytest.raises(obs.ObservationalError, match="protegida"):
        obs._safe_output(checkout / "data" / "derived" / "untrusted.json")
    (tmp_path / "data" / "derived").mkdir(parents=True)
    (tmp_path / "link").symlink_to(tmp_path / "data" / "derived", target_is_directory=True)
    with pytest.raises(obs.ObservationalError, match="protegida"):
        obs._safe_output(tmp_path / "link" / "bypass.json")


def test_a07_direct_download_guard_blocks_protected_checkout(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    checkout = Path(obs.__file__).resolve().parents[2]
    with pytest.raises(obs.ObservationalError, match="protegida"):
        obs.download_direct(URL, checkout / "data" / "derived" / "malicious.zip")
