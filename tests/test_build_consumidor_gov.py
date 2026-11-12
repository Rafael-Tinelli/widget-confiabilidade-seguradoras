from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any

import pytest
import requests

import api.build_consumidor_gov as build


def _write_gz(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False)


def _read_gz(path: Path) -> dict[str, Any]:
    with gzip.open(path, "rt", encoding="utf-8") as file:
        return json.load(file)


def _entry(name: str = "Seguradora Teste") -> dict[str, Any]:
    return {
        "name": name,
        "display_name": name,
        "cnpj": "",
        "statistics": {
            "complaintsCount": 1,
            "respondedCount": 1,
            "resolvedCount": 1,
            "finalizedCount": 1,
            "scoreSum": 5.0,
            "satisfactionCount": 1,
            "total_claims": 1,
            "responded_claims": 1,
            "resolved_claims": 1,
            "finalized_claims": 1,
            "averageScore": 5.0,
        },
    }


def _monthly(month: str = "2026-06") -> dict[str, Any]:
    entries = {"seguradora teste": _entry()}
    return {
        "meta": {
            "status": "ok",
            "month": month,
            "companies": 1,
            "lines_total": 1,
            "lines_kept": 1,
        },
        "by_name_key_raw": entries,
        "by_name_key": entries,
        "by_name": entries,
        "by_cnpj_key_raw": {},
        "by_cnpj_key": {},
    }


def _aggregate(month: str = "2026-06") -> dict[str, Any]:
    root = _monthly(month)
    root["meta"] = {
        "status": "ok",
        "months": [month],
        "companies": 1,
        "semantics": {"has_reliable_cnpj": False},
    }
    return root


def _configure_paths(monkeypatch, tmp_path: Path) -> tuple[Path, Path, Path]:
    raw = tmp_path / "raw"
    derived = tmp_path / "derived"
    monthly = derived / "monthly"
    monkeypatch.setattr(build, "RAW_DIR", raw)
    monkeypatch.setattr(build, "DERIVED_DIR", derived)
    monkeypatch.setattr(build, "MONTHLY_DIR", monthly)
    monkeypatch.setattr(build, "MONTHS_BACK", 1)
    monkeypatch.setattr(build, "FORCE_MONTH", None)
    monkeypatch.setattr(build, "FORCE_DOWNLOAD", False)
    monkeypatch.setattr(build, "CG_MIN_MONTH_BYTES", 1)
    monkeypatch.setattr(build, "CG_MAX_COMPANY_DROP_PCT", 0.25)
    return raw, derived, monthly


def test_ckan_failure_uses_existing_valid_aggregate(monkeypatch, tmp_path: Path) -> None:
    _, derived, _ = _configure_paths(monkeypatch, tmp_path)
    aggregate_path = derived / "consumidor_gov_agg.json.gz"
    _write_gz(aggregate_path, _aggregate())
    before = aggregate_path.read_bytes()

    def fail_ckan():
        raise requests.ConnectionError("DNS indisponível")

    monkeypatch.setattr(build, "_list_basecompleta_resources", fail_ckan)

    assert build.main() == 0
    assert aggregate_path.read_bytes() == before


def test_ckan_failure_without_cache_fails(monkeypatch, tmp_path: Path) -> None:
    _configure_paths(monkeypatch, tmp_path)

    def fail_ckan():
        raise requests.ConnectionError("DNS indisponível")

    monkeypatch.setattr(build, "_list_basecompleta_resources", fail_ckan)

    assert build.main() == 1


def test_invalid_monthly_marker_is_ignored_and_rebuilt(
    monkeypatch,
    tmp_path: Path,
) -> None:
    raw, derived, monthly = _configure_paths(monkeypatch, tmp_path)
    month = "2026-06"
    monthly_path = monthly / f"consumidor_gov_{month}.json.gz"
    _write_gz(
        monthly_path,
        {"meta": {"month": month, "invalid": True, "companies": 0}},
    )

    monkeypatch.setattr(
        build,
        "_list_basecompleta_resources",
        lambda: {
            month: build.ResourceInfo(
                month=month,
                name=f"BaseCompleta{month}",
                url="https://example.test/base.csv",
                format="CSV",
            )
        },
    )

    def fake_download(_url: str, out_path: Path) -> None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            "Nome Fantasia;Segmento de Mercado;Respondida;Situação;"
            "Avaliação Reclamação;Nota do Consumidor;Tempo Resposta;"
            "Procurou Empresa;Área;Assunto;Grupo Problema;Problema;"
            "Como Comprou Contratou;Ano Abertura;Mês Abertura\n"
            "Seguradora Teste;Seguros, Capitalização e Previdência;S;"
            "Finalizada avaliada;Resolvida;5;2;S;Serviços Financeiros;"
            "Seguro;Contrato;Cobertura;Internet;2026;6\n",
            encoding="utf-8",
        )

    monkeypatch.setattr(build, "_download", fake_download)

    assert build.main() == 0

    from api.v2.consumer_gov_window import load_active_window

    window = load_active_window(derived, monthly, required=1)
    rebuilt = window.months[0][2]
    assert rebuilt["meta"].get("invalid") is not True
    assert rebuilt["meta"]["companies"] == 1
    assert window.aggregate["meta"]["companies"] == 1
    assert window.aggregate["meta"]["status"] == "ok"
    assert (derived / "consumidor_gov_window_manifest.json").is_file()
    # The invalid legacy file was never promoted or overwritten.
    assert _read_gz(monthly_path)["meta"]["invalid"] is True
    assert not (raw / f"basecompleta_{month}.csv").exists()


def test_all_invalid_months_do_not_overwrite_valid_aggregate(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _, derived, _ = _configure_paths(monkeypatch, tmp_path)
    monkeypatch.setattr(build, "CG_MIN_MONTH_BYTES", 1000)
    month = "2026-06"
    aggregate_path = derived / "consumidor_gov_agg.json.gz"
    _write_gz(aggregate_path, _aggregate(month))
    before = aggregate_path.read_bytes()

    monkeypatch.setattr(
        build,
        "_list_basecompleta_resources",
        lambda: {
            month: build.ResourceInfo(
                month=month,
                name=f"BaseCompleta{month}",
                url="https://example.test/base.csv",
                format="CSV",
            )
        },
    )

    def tiny_download(_url: str, out_path: Path) -> None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_bytes(b"tiny")

    monkeypatch.setattr(build, "_download", tiny_download)

    assert build.main() == 0
    assert aggregate_path.read_bytes() == before


def test_partial_download_never_replaces_existing_raw_cache(
    monkeypatch,
    tmp_path: Path,
) -> None:
    destination = tmp_path / "base.csv"
    destination.write_bytes(b"previous-valid-cache")

    class BrokenResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def raise_for_status(self) -> None:
            return None

        def iter_content(self, chunk_size: int):
            del chunk_size
            yield b"partial"
            raise requests.ConnectionError("connection interrupted")

    monkeypatch.setattr(build.HTTP, "get", lambda *args, **kwargs: BrokenResponse())

    with pytest.raises(requests.ConnectionError, match="connection interrupted"):
        build._download("https://example.test/base.csv", destination)

    assert destination.read_bytes() == b"previous-valid-cache"
    assert not destination.with_suffix(destination.suffix + ".tmp").exists()


def test_direct_consumer_source_lists_only_explicit_basecompleta(
    monkeypatch,
) -> None:
    base = build.consumer_gov_direct.Publication(
        code="20260800000010944",
        title="Base Completa Consumidor.gov.br - Junho_2026",
        filename="basecompleta2026-06.zip",
        published_at="2026-08-23",
        month="2026-06",
        discovery_method="test",
    )
    generic = build.consumer_gov_direct.Publication(
        code="20260800000010945",
        title="Dados - Julho de 2026",
        filename="dados_2026-07.zip",
        published_at="2026-08-24",
        month="2026-07",
        discovery_method="test",
    )
    finalizadas = build.consumer_gov_direct.Publication(
        code="20260800000010946",
        title="Reclamações finalizadas em Julho de 2026",
        filename="finalizadas_2026-07.zip",
        published_at="2026-08-24",
        month="2026-07",
        discovery_method="test",
    )
    monkeypatch.setattr(
        build.consumer_gov_direct,
        "discover_publications",
        lambda _wanted: [generic, finalizadas, base],
    )

    resources = build._list_basecompleta_resources()

    assert list(resources) == ["2026-06"]
    assert resources["2026-06"].url == base.download_url
    assert resources["2026-06"].publication == base


def test_direct_resource_download_uses_schema_validating_client(
    monkeypatch,
    tmp_path: Path,
) -> None:
    publication = build.consumer_gov_direct.Publication(
        code="20260800000010944",
        title="Base Completa Consumidor.gov.br - Junho_2026",
        filename="basecompleta2026-06.zip",
        published_at="2026-08-23",
        month="2026-06",
        discovery_method="test",
    )
    resource = build.ResourceInfo(
        month="2026-06",
        name=publication.title,
        url=publication.download_url,
        format="ZIP",
        publication=publication,
    )
    destination = tmp_path / "basecompleta_2026-06.csv"
    calls = []

    def fake_download(selected, out_path, month):
        calls.append((selected, out_path, month))
        out_path.write_text("validated", encoding="utf-8")

    monkeypatch.setattr(
        build.consumer_gov_direct,
        "download_publication",
        fake_download,
    )

    build._download_resource(resource, destination)

    assert calls == [(publication, destination, "2026-06")]


def test_legacy_http_declared_length_mismatch_preserves_original(
    monkeypatch, tmp_path: Path
) -> None:
    destination = tmp_path / "base.csv"
    destination.write_bytes(b"valid-previous-cache")

    class ShortResponse:
        headers = {"Content-Length": "100"}

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def raise_for_status(self):
            return None

        def iter_content(self, chunk_size):
            assert chunk_size > 0
            yield b"short"

    monkeypatch.setattr(
        build.HTTP, "get", lambda *args, **kwargs: ShortResponse()
    )
    with pytest.raises(RuntimeError, match="incomplete"):
        build._download("https://example.test/data.csv", destination)
    assert destination.read_bytes() == b"valid-previous-cache"
