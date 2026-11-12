"""Failure-injection tests for Consumer.gov direct raw cache integrity."""

from __future__ import annotations

import hashlib
import io
import json
import zipfile
from pathlib import Path

import pytest
import requests

from api.sources import consumer_gov_direct as direct


MONTH = "2026-03"


def _publication() -> direct.Publication:
    return direct.Publication(
        code="20260400000012345",
        title="Base Completa março 2026",
        filename="basecompleta2026-03.zip",
        published_at="2026-04-01",
        month=MONTH,
        discovery_method="test",
    )


def _csv_bytes(name: str = "Empresa Alfa", month: str = MONTH) -> bytes:
    year, number = month.split("-")
    header = (
        "Nome Fantasia;Segmento de Mercado;Respondida;Situação;"
        "Avaliação Reclamação;Nota do Consumidor;Tempo Resposta;"
        "Procurou Empresa;Área;Assunto;Grupo Problema;Problema;"
        "Como Comprou Contratou;Ano Abertura;Mês Abertura\n"
    )
    row = (
        f"{name};Seguros, Capitalização e Previdência;S;"
        f"Finalizada avaliada;Resolvida;4;2;S;Serviços Financeiros;"
        f"Seguro;Contrato;Cobertura;Internet;{year};{int(number)}\n"
    )
    return (header + row).encode("utf-8-sig")


def _zip_bytes(csv_payload: bytes) -> bytes:
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("basecompleta2026-03.csv", csv_payload)
    return out.getvalue()


class _Response:
    def __init__(self, chunks: list[bytes], *, interrupt: bool = False):
        self.chunks = chunks
        self.interrupt = interrupt

    def __enter__(self):
        return self

    def __exit__(self, _type, _value, _traceback):
        return False

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int):
        assert chunk_size > 0
        yield from self.chunks
        if self.interrupt:
            raise requests.ConnectionError("download interrupted")


class _Session:
    def __init__(self, response: _Response):
        self.response = response

    def get(self, *args, **kwargs):
        assert kwargs["stream"] is True
        return self.response


def _initialized_cache(tmp_path: Path, monkeypatch) -> tuple[Path, Path, bytes]:
    monkeypatch.setattr(direct, "MIN_MONTH_BYTES", 1)
    raw = tmp_path / "raw"
    raw.mkdir()
    path = raw / f"basecompleta_{MONTH}.csv"
    payload = _csv_bytes()
    path.write_bytes(payload)
    manifest = raw / "manifest.json"
    first = direct.ensure_months([MONTH], raw_dir=raw, manifest_path=manifest)
    assert first[MONTH]["acquisition"] == "cache_schema_validated"
    return path, manifest, payload


def test_same_size_tampering_is_not_a_cache_hit_or_legacy_rebootstrap(
    tmp_path: Path, monkeypatch
) -> None:
    path, manifest, original = _initialized_cache(tmp_path, monkeypatch)
    original_manifest = manifest.read_bytes()
    altered = original.replace(b"Empresa Alfa", b"Empresa Beta")
    assert len(altered) == len(original)
    assert altered != original
    path.write_bytes(altered)
    assert not direct._manifest_cache_hit(
        path, MONTH, json.loads(original_manifest)["months"][MONTH]
    )

    monkeypatch.setattr(direct, "discover_publications", lambda _: [_publication()])

    def offline(_publication, _destination, _month):
        raise direct.ConsumerGovDownloadFailed("official source offline")

    monkeypatch.setattr(direct, "download_publication", offline)
    with pytest.raises(direct.ConsumerGovDownloadFailed, match="offline"):
        direct.ensure_months([MONTH], raw_dir=path.parent, manifest_path=manifest)

    # No re-baselining of tampered bytes, even though the CSV is plausible.
    assert path.read_bytes() == altered
    assert manifest.read_bytes() == original_manifest


def test_tampered_cache_is_replaced_only_after_successful_revalidation(
    tmp_path: Path, monkeypatch
) -> None:
    path, manifest, original = _initialized_cache(tmp_path, monkeypatch)
    path.write_bytes(original.replace(b"Empresa Alfa", b"Empresa Beta"))
    monkeypatch.setattr(direct, "discover_publications", lambda _: [_publication()])

    def restored(_publication, dest, month):
        dest.write_bytes(original)
        return direct.validate_month_csv(dest, month)

    monkeypatch.setattr(direct, "download_publication", restored)
    result = direct.ensure_months(
        [MONTH], raw_dir=path.parent, manifest_path=manifest
    )

    assert result[MONTH]["acquisition"] == "download"
    assert path.read_bytes() == original
    entry = json.loads(manifest.read_text(encoding="utf-8"))["months"][MONTH]
    assert entry["sha256"] == hashlib.sha256(original).hexdigest()
    assert direct._manifest_cache_hit(path, MONTH, entry)


def test_network_interruption_never_deletes_prior_destination(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(direct, "MIN_MONTH_BYTES", 1)
    destination = tmp_path / f"basecompleta_{MONTH}.csv"
    original = _csv_bytes()
    destination.write_bytes(original)
    http = _Session(_Response([b"partial"], interrupt=True))

    with pytest.raises(direct.ConsumerGovDownloadFailed, match="interrupted"):
        direct.download_publication(_publication(), destination, MONTH, session=http)

    assert destination.read_bytes() == original
    assert sorted(p.name for p in tmp_path.iterdir()) == [destination.name]


def test_invalid_download_does_not_delete_prior_destination(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(direct, "MIN_MONTH_BYTES", 1)
    destination = tmp_path / f"basecompleta_{MONTH}.csv"
    original = _csv_bytes()
    destination.write_bytes(original)

    with pytest.raises(direct.ConsumerGovSchemaMismatch):
        direct.download_publication(
            _publication(), destination, MONTH,
            session=_Session(_Response([b"not a CSV"])),
        )

    assert destination.read_bytes() == original
    assert sorted(p.name for p in tmp_path.iterdir()) == [destination.name]


def test_valid_csv_for_wrong_month_does_not_replace_destination(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(direct, "MIN_MONTH_BYTES", 1)
    destination = tmp_path / f"basecompleta_{MONTH}.csv"
    original = _csv_bytes()
    destination.write_bytes(original)

    with pytest.raises(direct.ConsumerGovMonthMismatch):
        direct.download_publication(
            _publication(), destination, MONTH,
            session=_Session(_Response([_zip_bytes(_csv_bytes(month="2026-04"))])),
        )

    assert destination.read_bytes() == original
    assert sorted(p.name for p in tmp_path.iterdir()) == [destination.name]


def test_successful_download_validates_stage_before_replacing_old_bytes(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(direct, "MIN_MONTH_BYTES", 1)
    destination = tmp_path / f"basecompleta_{MONTH}.csv"
    old = _csv_bytes("Empresa Alfa")
    updated = _csv_bytes("Empresa Beta")
    destination.write_bytes(old)

    original_validator = direct.validate_month_csv

    def verify_staging(path: Path, month: str):
        assert path != destination
        assert path.parent == destination.parent
        assert destination.read_bytes() == old
        return original_validator(path, month)

    monkeypatch.setattr(direct, "validate_month_csv", verify_staging)
    result = direct.download_publication(
        _publication(), destination, MONTH,
        session=_Session(_Response([_zip_bytes(updated)])),
    )
    assert destination.read_bytes() == updated
    assert result.sha256 == hashlib.sha256(updated).hexdigest()
    assert sorted(p.name for p in tmp_path.iterdir()) == [destination.name]


def test_extraction_failure_leaves_prior_bytes_and_removes_temps(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(direct, "MIN_MONTH_BYTES", 1)
    destination = tmp_path / f"basecompleta_{MONTH}.csv"
    original = _csv_bytes()
    destination.write_bytes(original)

    def broken_extraction(_archive, staged):
        staged.write_bytes(b"partial")
        raise OSError("disk write interrupted")

    monkeypatch.setattr(direct, "_materialize_download", broken_extraction)
    with pytest.raises(OSError, match="disk write interrupted"):
        direct.download_publication(
            _publication(), destination, MONTH,
            session=_Session(_Response([_zip_bytes(_csv_bytes("Empresa Beta"))])),
        )

    assert destination.read_bytes() == original
    assert sorted(p.name for p in tmp_path.iterdir()) == [destination.name]


def test_manifest_without_hash_is_not_trusted_for_rebootstrap(
    tmp_path: Path, monkeypatch
) -> None:
    path, manifest, original = _initialized_cache(tmp_path, monkeypatch)
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    del payload["months"][MONTH]["sha256"]
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(direct, "discover_publications", lambda _: [_publication()])

    def offline(*_args):
        raise direct.ConsumerGovDownloadFailed("offline")

    monkeypatch.setattr(direct, "download_publication", offline)
    with pytest.raises(direct.ConsumerGovDownloadFailed, match="offline"):
        direct.ensure_months([MONTH], raw_dir=path.parent, manifest_path=manifest)
    assert path.read_bytes() == original
    assert "sha256" not in json.loads(manifest.read_text())["months"][MONTH]


def test_unreadable_existing_manifest_fails_closed(
    tmp_path: Path, monkeypatch
) -> None:
    path, manifest, original = _initialized_cache(tmp_path, monkeypatch)
    manifest.write_text("{broken json", encoding="utf-8")
    with pytest.raises(direct.ConsumerGovArchiveInvalid, match="unreadable"):
        direct.ensure_months([MONTH], raw_dir=path.parent, manifest_path=manifest)
    assert path.read_bytes() == original


def test_repeat_valid_cache_hash_matches_manifest_and_does_not_discover(
    tmp_path: Path, monkeypatch
) -> None:
    path, manifest, original = _initialized_cache(tmp_path, monkeypatch)

    def unexpected_discovery(_wanted):
        raise AssertionError("valid hash must allow cache reuse")

    monkeypatch.setattr(direct, "discover_publications", unexpected_discovery)
    result = direct.ensure_months(
        [MONTH], raw_dir=path.parent, manifest_path=manifest
    )
    assert result[MONTH]["acquisition"] == "cache_manifest"
    assert path.read_bytes() == original
