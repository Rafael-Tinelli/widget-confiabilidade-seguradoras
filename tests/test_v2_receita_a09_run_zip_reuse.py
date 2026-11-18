"""A09.4: one-generation establishment ZIP reuse; strictly offline tests."""
from __future__ import annotations

import io
import os
import subprocess
import sys
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from api.sources import receita_cnpj_run_zip_cache as shared


def _archive() -> bytes:
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("dados.csv", "10000000;DADOS VALIDOS\n")
    return out.getvalue()


def _release(url: str = "https://official.example/2026-10/") -> SimpleNamespace:
    return SimpleNamespace(period="2026-10", release_url=url)


def _meta(raw: bytes, *, etag: str = "etag-2026-10") -> dict:
    return {
        "name": "Estabelecimentos0.zip", "size": len(raw),
        "etag": etag, "last_modified": "2026-11-01T00:00:00Z",
    }


def _configure(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("V2_BUILD_ID", "v2-gate4-full-123-a1")
    monkeypatch.setenv("RUNNER_TEMP", str(tmp_path / "runner-tmp"))


def _downloader(dest: Path, contents: bytes, calls: list[str]):
    def fetch() -> Path:
        calls.append(str(dest))
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(contents)
        return dest
    return fetch


def _acquire(
    release: SimpleNamespace, meta: dict, dest: Path, fetch,
) -> Path:
    return shared.acquire_run_establishment_zip(
        release=release, name="Estabelecimentos0.zip",
        metadata=meta, destination=dest, download=fetch,
    )


def test_a094_lifecycle_then_identity_uses_same_verified_zip_without_second_get(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    _configure(monkeypatch, tmp_path)
    raw = _archive()
    meta = _meta(raw)
    calls: list[str] = []
    lifecycle = tmp_path / "lifecycle" / meta["name"]
    identity = tmp_path / "identity" / meta["name"]
    assert _acquire(_release(), meta, lifecycle, _downloader(lifecycle, raw, calls)) == lifecycle
    assert lifecycle.read_bytes() == raw
    lifecycle.unlink()  # Exactly what the first consuming stage does.
    assert _acquire(_release(), meta, identity, lambda: pytest.fail(
        "second official HTTP GET is forbidden for an intact current-run ZIP"
    )) == identity
    assert identity.read_bytes() == raw
    assert calls == [str(lifecycle)]


def test_a094_reuse_survives_separate_python_process(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    _configure(monkeypatch, tmp_path)
    raw = _archive()
    name = "Estabelecimentos0.zip"
    first = tmp_path / "first" / name
    _acquire(_release(), _meta(raw), first, _downloader(first, raw, []))
    first.unlink()
    output = tmp_path / "subprocess" / name
    script = """
from pathlib import Path
from types import SimpleNamespace
from api.sources.receita_cnpj_run_zip_cache import acquire_run_establishment_zip
target = Path(__import__('sys').argv[1])
release = SimpleNamespace(period="2026-10", release_url="https://official.example/2026-10/")
data = {"name": "Estabelecimentos0.zip", "size": int(__import__('sys').argv[2]),
        "etag": "etag-2026-10", "last_modified": "2026-11-01T00:00:00Z"}
acquire_run_establishment_zip(release=release, name=data["name"], metadata=data,
                              destination=target,
                              download=lambda: (_ for _ in ()).throw(
                                  AssertionError("unexpected second download")))
"""
    subprocess.run(
        [sys.executable, "-c", script, str(output), str(len(raw))],
        check=True, env=os.environ.copy(), capture_output=True, text=True,
    )
    assert output.read_bytes() == raw


@pytest.mark.parametrize("change", ["etag", "release_url", "build_id", "period"])
def test_a094_changed_source_or_run_forces_official_download(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, change: str,
) -> None:
    _configure(monkeypatch, tmp_path)
    raw = _archive()
    metadata = _meta(raw)
    first = tmp_path / "first.zip"
    calls: list[str] = []
    _acquire(_release(), metadata, first, _downloader(first, raw, calls))
    first.unlink()
    next_meta = dict(metadata)
    next_release = _release()
    if change == "etag":
        next_meta["etag"] = "republished"
    elif change == "release_url":
        next_release = _release("https://official.example/different/")
    elif change == "period":
        next_release.period = "2026-11"
    else:
        monkeypatch.setenv("V2_BUILD_ID", "v2-gate4-full-124-a1")
    next_path = tmp_path / "next.zip"
    _acquire(next_release, next_meta, next_path, _downloader(next_path, raw, calls))
    assert len(calls) == 2
    assert next_path.read_bytes() == raw


def test_a094_same_size_corrupted_cache_never_reused(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    _configure(monkeypatch, tmp_path)
    raw = _archive()
    meta = _meta(raw)
    first = tmp_path / "first.zip"
    calls: list[str] = []
    _acquire(_release(), meta, first, _downloader(first, raw, calls))
    root = shared._cache_dir()
    assert root is not None
    cached = root / meta["name"]
    corrupted = bytearray(raw)
    corrupted[10] ^= 1
    cached.write_bytes(corrupted)
    assert len(corrupted) == len(raw)
    first.unlink()
    second = tmp_path / "second.zip"
    _acquire(_release(), meta, second, _downloader(second, raw, calls))
    assert len(calls) == 2
    assert second.read_bytes() == raw


@pytest.mark.parametrize("missing", [
    {"etag": None, "last_modified": None},
    {"size": None},
    {"size": 0},
    {"size": True},
    {"name": "../Estabelecimentos0.zip"},
])
def test_a094_invalid_official_metadata_prevents_reuse(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, missing: dict,
) -> None:
    _configure(monkeypatch, tmp_path)
    raw = _archive()
    bad = {**_meta(raw), **missing}
    calls: list[str] = []
    for i in range(2):
        dest = tmp_path / f"attempt-{i}.zip"
        with pytest.raises(shared.UnverifiedRunZipError, match="metadata_missing"):
            _acquire(_release(), bad, dest, _downloader(dest, raw, calls))
        assert not dest.exists()
    assert not calls


def test_a094_absent_build_scope_disables_reuse(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.delenv("V2_BUILD_ID", raising=False)
    monkeypatch.setenv("RUNNER_TEMP", str(tmp_path))
    raw = _archive()
    calls: list[str] = []
    for i in range(2):
        dest = tmp_path / f"no-scope-{i}.zip"
        _acquire(_release(), _meta(raw), dest, _downloader(dest, raw, calls))
    assert len(calls) == 2
    assert shared._cache_dir() is None


def test_a094_wrong_size_is_not_stored(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    _configure(monkeypatch, tmp_path)
    raw = _archive()
    wrong = {**_meta(raw), "size": len(raw) + 1}
    calls: list[str] = []
    for i in range(2):
        dest = tmp_path / f"wrong-{i}.zip"
        with pytest.raises(shared.UnverifiedRunZipError, match="unverifiable"):
            _acquire(_release(), wrong, dest, _downloader(dest, raw, calls))
        assert not dest.exists()
    assert len(calls) == 2


def test_a094_production_lifecycle_and_identity_reuse_one_official_zip(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    """Integration: actual consuming functions, mocked HTTP with small ZIPs."""
    from collections import Counter

    from api.sources import receita_cnpj_bulk as bulk
    from api.sources import receita_cnpj_identity as identity

    _configure(monkeypatch, tmp_path)

    def zip_data(rows: str) -> bytes:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("data.csv", rows.encode("latin1"))
        return buf.getvalue()

    blobs = {
        "Motivos.zip": zip_data("01;Motivo\n"),
        "Empresas0.zip": zip_data("12345678;DEMO SEGURADORA SA;2054\n"),
        "Cnaes.zip": zip_data("6511101;Seguradoras\n"),
        "Naturezas.zip": zip_data("2054;Sociedade anonima\n"),
        "Estabelecimentos0.zip": zip_data(
            "12345678;0001;00;1;DEMO SEGURADORA;02;20260110"
            ";;;;20010101;6511101;\n"
        ),
    }
    metadata = {
        name: {"name": name, "size": len(blob), "etag": f"etag-{name}",
               "last_modified": "2026-11-01"}
        for name, blob in blobs.items()
    }
    release = bulk.ReceitaOpenDataRelease(
        base_url="https://arquivos.receitafederal.gov.br/index.php/s/token",
        release_url="https://arquivos.receitafederal.gov.br/public.php/dav/files/token/2026-10/",
        period="2026-10",
        establishment_files=("Estabelecimentos0.zip",),
        resource_metadata=(
            metadata["Motivos.zip"], metadata["Estabelecimentos0.zip"],
        ),
    )
    calls = Counter()

    def download(_session, _url, destination: Path, **_kwargs) -> Path:
        calls[destination.name] += 1
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(blobs[destination.name])
        return destination

    monkeypatch.setattr(bulk, "_download_zip", download)
    monkeypatch.setattr(bulk, "load_reason_map", lambda _path: {"01": "Demo"})
    monkeypatch.setattr(
        bulk, "extract_target_lifecycle_from_zip",
        lambda *_a, **_k: [{"cnpj": "12345678000100"}],
    )
    targets = [{"cnpj": "12345678000100", "entity_id": "e1",
                "legal_name": "DEMO SEGURADORA SA"}]
    lifecycle = bulk.refresh_filtered_lifecycle(
        targets, release=release, work_dir=tmp_path / "lifecycle",
    )
    assert lifecycle["status"] == "ok"
    assert calls["Estabelecimentos0.zip"] == 1

    monkeypatch.setattr(identity, "_download_zip", download)
    monkeypatch.setattr(identity, "_release_files", lambda *_a, **_k: {
        "companies": ["Empresas0.zip"], "cnaes": "Cnaes.zip",
        "legal_natures": "Naturezas.zip", "metadata": metadata,
    })
    result = identity.build_filtered_identity_snapshot(
        targets, ["DEMO SEGURADORA"], release=release,
        work_dir=tmp_path / "identity",
    )
    assert len(result["canonical_records"]) == 1
    assert calls["Empresas0.zip"] == 1
    assert calls["Estabelecimentos0.zip"] == 1
