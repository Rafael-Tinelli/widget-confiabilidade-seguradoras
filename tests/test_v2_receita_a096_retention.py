"""A09.6 retention is same-run, hardlink-only and fails without all 20 ZIPs."""
from __future__ import annotations

import hashlib
import io
import json
import os
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from api.sources import receita_cnpj_a096_retention as keep
from api.sources import receita_cnpj_run_zip_cache as a094

OFFICIAL = "https://arquivos.receitafederal.gov.br/public.php/dav/files/token/2026-11/"


def _zip() -> bytes:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("data.csv", "12345678;DEMO\n")
    return stream.getvalue()


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _configure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv(keep.ENV_NAME, "1")
    monkeypatch.setenv("V2_BUILD_ID", "a096-proof-local-1")
    monkeypatch.setenv("RUNNER_TEMP", str(tmp_path / "runner"))
    monkeypatch.setenv("V2_SOURCE_HEAD_SHA", "c" * 40)


def _reference_metadata(name: str, size: int) -> dict:
    return {
        "name": name, "size": size,
        "etag": f'"etag-{name}"',
        "last_modified": "Mon, 16 Nov 2026 10:04:47 GMT",
    }


def _sample_snapshot(group: str) -> dict:
    return {
        "artifact": "v2_receita_cnpj_identity" if group == "identity"
        else "v2_receita_cnpj_lifecycle",
        "source": {"reference_period": "2026-11"},
    }


def _prepare_full_capture(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> tuple[Path, Path]:
    _configure(monkeypatch, tmp_path)
    raw = _zip()
    base = tmp_path / "work"
    base.mkdir()
    release = SimpleNamespace(period="2026-11", release_url=OFFICIAL)
    a094_dir = a094._cache_dir()
    assert a094_dir is not None
    a094_dir.mkdir(parents=True)
    for index in range(10):
        estab = f"Estabelecimentos{index}.zip"
        source = base / estab
        source.write_bytes(raw)
        receipt = {
            "schema": "receita-run-establishment-zip-v1",
            "build_id": os.environ["V2_BUILD_ID"],
            "reference_period": "2026-11",
            "release_url": OFFICIAL,
            "resource": _reference_metadata(estab, len(raw)),
            "sha256": _sha(raw),
        }
        os.link(source, a094_dir / estab)
        (a094_dir / (estab + ".json")).write_text(json.dumps(receipt), encoding="utf-8")
        company = f"Empresas{index}.zip"
        company_source = base / company
        company_source.write_bytes(raw)
        keep.capture_company(
            company_source, name=company,
            metadata=_reference_metadata(company, len(raw)),
            release=release,
        )
    source_root = tmp_path / "source"
    conduct_root = tmp_path / "conduct"
    for label, root, source_id in (
        ("lifecycle", source_root, "receita_cnpj_lifecycle"),
        ("identity", conduct_root, "receita_consumer_identity"),
    ):
        root.mkdir()
        data = json.dumps(_sample_snapshot(label)).encode()
        (root / f"{source_id}.json").write_bytes(data)
        (root / f"{source_id}.meta.json").write_text(json.dumps({
            "source_id": source_id, "sha256": _sha(data),
            "fetched_at": "2026-11-16T22:00:00Z",
        }))
    return source_root, conduct_root


def test_a096_hardlinks_capture_only_after_real_acquisition(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    roots = _prepare_full_capture(monkeypatch, tmp_path)
    manifest = keep.local_input_manifest(source_root=roots[0], conduct_root=roots[1])
    assert manifest["zip_files_verified"] == 20
    assert manifest["reference_period"] == "2026-11"
    assert manifest["input_mode"] == "same_exact_local_zip_bytes"
    assert manifest["source_head_sha"] == "c" * 40
    assert len(manifest["zip_partitions"]["Empresas"]) == 10
    assert len(manifest["zip_partitions"]["Estabelecimentos"]) == 10
    a = tmp_path / "work/Empresas0.zip"
    b = keep._company_root() / "Empresas0.zip"
    assert os.stat(a).st_ino == os.stat(b).st_ino
    assert b.stat().st_size == a.stat().st_size


def test_a096_retention_disabled_creates_nothing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    _configure(monkeypatch, tmp_path)
    monkeypatch.delenv(keep.ENV_NAME)
    blob = _zip()
    src = tmp_path / "Empresas0.zip"
    src.write_bytes(blob)
    keep.capture_company(
        src, name=src.name, metadata=_reference_metadata(src.name, len(blob)),
        release=SimpleNamespace(period="2026-11", release_url=OFFICIAL),
    )
    assert not (tmp_path / "runner/a096-company-run-zips").exists()


def test_a096_missing_partition_fails_closed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    source, conduct = _prepare_full_capture(monkeypatch, tmp_path)
    keep._company_root().joinpath("Empresas9.zip").unlink()
    with pytest.raises(ValueError, match="incomplete"):
        keep.local_input_manifest(source_root=source, conduct_root=conduct)


def test_a096_corrupt_same_size_zip_fails_closed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    source, conduct = _prepare_full_capture(monkeypatch, tmp_path)
    path = keep._company_root() / "Empresas8.zip"
    data = bytearray(path.read_bytes())
    data[8] ^= 1
    # The original source is hardlinked; this is a deliberate adversarial
    # mutation and the SHA-256 seal must reject it.
    path.write_bytes(data)
    with pytest.raises(ValueError, match="invalid"):
        keep.local_input_manifest(source_root=source, conduct_root=conduct)


@pytest.mark.parametrize("change", ["period", "release_url", "build_id"])
def test_a096_changed_release_or_build_cannot_mix_zips(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, change: str,
) -> None:
    source, conduct = _prepare_full_capture(monkeypatch, tmp_path)
    path = keep._company_root() / "Empresas1.zip.json"
    data = json.loads(path.read_text())
    if change == "period":
        data["reference_period"] = "2026-10"
    elif change == "release_url":
        data["release_url_sha256"] = "0" * 64
    else:
        data["build_id"] = "other-build"
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="invalid|sources_do_not_share|build"):
        keep.local_input_manifest(source_root=source, conduct_root=conduct)


def test_a096_retention_rejects_foreign_release_without_side_effects(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    _configure(monkeypatch, tmp_path)
    raw = _zip()
    src = tmp_path / "Empresas0.zip"
    src.write_bytes(raw)
    keep.capture_company(
        src, name=src.name, metadata=_reference_metadata(src.name, len(raw)),
        release=SimpleNamespace(period="2026-11", release_url="https://example.com/a/"),
    )
    root = keep._company_root()
    assert root is not None
    assert not root.exists()
    assert src.read_bytes() == raw
