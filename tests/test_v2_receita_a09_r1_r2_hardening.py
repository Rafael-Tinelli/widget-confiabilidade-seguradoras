"""R1/R2 regression: all acquisition routes and bounded speculative ownership.

All transfers are mocked in-process. No national Receita ZIP, Gate 4,
HostGator publication or GitHub Actions workload is triggered by this suite.
"""
from __future__ import annotations

import hashlib
import io
import threading
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest
import requests

from api.sources import receita_cnpj_bulk as bulk
from api.sources import receita_cnpj_prefetch as prefetch
from api.sources import receita_cnpj_run_zip_cache as a094

ROOT = "https://arquivos.receitafederal.gov.br/public.php/dav/files/token/2026-11/"
NAME = "Empresas0.zip"


def _zip(data: str = "12345678;EMPRESA DEMO;2054\n") -> bytes:
    target = io.BytesIO()
    with zipfile.ZipFile(target, "w") as archive:
        archive.writestr("dados.csv", data.encode("latin1"))
    return target.getvalue()


def _spec(raw: bytes, name: str = NAME) -> prefetch.Partition:
    return prefetch.Partition(
        name=name, url=ROOT + name, size=len(raw),
        etag='"official-etag"',
        last_modified="Mon, 16 Nov 2026 10:04:47 GMT",
    )


class FakeResponse:
    def __init__(self, spec, raw, **changes):
        self.status_code = changes.get("status", 200)
        self.url = changes.get("final_url", spec.url)
        self.headers = {
            "ETag": changes.get("etag", spec.etag),
            "Last-Modified": changes.get("modified", spec.last_modified),
            "Content-Length": changes.get("length", str(len(raw))),
        }
        if "content_range" in changes:
            self.headers["Content-Range"] = changes["content_range"]
        if "encoding" in changes:
            self.headers["Content-Encoding"] = changes["encoding"]
        self.raw = raw
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.closed = True

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError("simulated transient HTTP error")

    def iter_content(self, chunk_size):
        for index in range(0, len(self.raw), chunk_size):
            yield self.raw[index:index + chunk_size]


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.calls = []
        self.headers = {}
        self.auth = None

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.response

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None


@pytest.mark.parametrize("scenario", [
    "etag", "last_modified", "cross_host_redirect", "http_downgrade",
    "partial_status", "content_range", "gzip_encoding", "content_length",
    "size_short", "invalid_zip",
])
def test_r1_invalid_reply_rejected_by_both_routes(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, scenario: str,
) -> None:
    original = _zip()
    raw = original
    spec = _spec(original)
    changes = {}
    if scenario == "etag":
        changes["etag"] = '"unexpected-revision"'
    elif scenario == "last_modified":
        changes["modified"] = "Sun, 15 Nov 2026 10:04:47 GMT"
    elif scenario == "cross_host_redirect":
        changes["final_url"] = "https://untrusted.example/Empresas0.zip"
    elif scenario == "http_downgrade":
        changes["final_url"] = "http://arquivos.receitafederal.gov.br/Empresas0.zip"
    elif scenario == "partial_status":
        changes["status"] = 206
    elif scenario == "content_range":
        changes["content_range"] = "bytes 0-10/100"
    elif scenario == "gzip_encoding":
        changes["encoding"] = "gzip"
    elif scenario == "content_length":
        changes["length"] = str(len(original) + 3)
    elif scenario == "size_short":
        raw = original[:-5]
        changes["length"] = str(len(original))
    elif scenario == "invalid_zip":
        raw = b"not-a-ZIP" + b"x" * (len(original) - 9)
    response = FakeResponse(spec, raw, **changes)
    serial = FakeSession(response)
    destination = tmp_path / "serial" / NAME
    with pytest.raises(bulk.ReceitaOpenDataError):
        bulk._download_zip(serial, spec.url, destination, retries=1, expected=spec)
    assert len(serial.calls) == 1
    assert not destination.exists()
    assert not destination.with_suffix(".zip.part").exists()

    other_response = FakeResponse(spec, raw, **changes)
    prefetch_session = FakeSession(other_response)
    monkeypatch.setattr(prefetch.requests, "Session", lambda: prefetch_session)
    with pytest.raises(prefetch.PrefetchRejected):
        prefetch._fetch_stage(spec, tmp_path, threading.Event())
    assert not (tmp_path / NAME).exists()
    assert not (tmp_path / (NAME + ".part")).exists()
    assert response.closed and other_response.closed


def test_r1_valid_serial_keeps_local_digest_and_requires_official_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw = _zip()
    spec = _spec(raw)
    session = FakeSession(FakeResponse(spec, raw))
    observed = []
    monkeypatch.setattr(bulk, "emit", lambda event, **fields: observed.append((event, fields)))
    output = tmp_path / NAME
    assert bulk._download_zip(session, spec.url, output, retries=1, expected=spec) == output
    assert output.read_bytes() == raw
    assert observed[-1][1]["local_sha256"] == hashlib.sha256(raw).hexdigest()
    assert observed[-1][1]["state"] == "success"
    meta = {"name": NAME, "size": len(raw), "etag": spec.etag}
    assert prefetch.official_resource(ROOT, NAME, meta).size == len(raw)
    with pytest.raises(prefetch.PrefetchRejected):
        prefetch.official_resource(ROOT, NAME, {"name": NAME, "etag": spec.etag})
    with pytest.raises(bulk.ReceitaOpenDataError):
        bulk._required_official_resource(ROOT, NAME, {"name": NAME, "size": len(raw)})


def test_r1_run_cache_never_returns_unverifiable_miss_even_without_run_scope(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.delenv("V2_BUILD_ID", raising=False)
    monkeypatch.setenv("RUNNER_TEMP", str(tmp_path))
    raw = _zip()
    destination = tmp_path / "Estabelecimentos0.zip"
    spec = _spec(raw, name="Estabelecimentos0.zip")
    metadata = {"name": spec.name, "size": len(raw) + 100, "etag": spec.etag}
    def bad_download():
        destination.write_bytes(raw)
        return destination
    with pytest.raises(a094.UnverifiedRunZipError, match="unverifiable"):
        a094.acquire_run_establishment_zip(
            release=SimpleNamespace(period="2026-11", release_url=ROOT),
            name=spec.name, metadata=metadata,
            destination=destination, download=bad_download,
        )
    assert not destination.exists()


def test_r2_six_warm_a094_hits_never_accumulate_six_staged_zips(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setenv("V2_BUILD_ID", "a09-warm-cache-reentry")
    monkeypatch.setenv("RUNNER_TEMP", str(tmp_path / "runner"))
    monkeypatch.setattr(prefetch, "MIN_FREE_RESERVE_BYTES", 0)
    raw = _zip()
    release = SimpleNamespace(period="2026-11", release_url=ROOT)
    specs = [_spec(raw, f"Estabelecimentos{i}.zip") for i in range(6)]
    work = tmp_path / "work"
    work.mkdir()
    # Populate the true run-scoped cache BEFORE the speculative re-entry.
    for spec in specs:
        metadata = {
            "name": spec.name, "size": spec.size,
            "etag": spec.etag, "last_modified": spec.last_modified,
        }
        destination = work / ("seed-" + spec.name)
        a094.acquire_run_establishment_zip(
            release=release, name=spec.name, metadata=metadata,
            destination=destination,
            download=lambda destination=destination: _write(destination, raw),
        )
        destination.unlink()

    started = []
    def staged_worker(spec, root, cancel):
        assert not cancel.is_set()
        started.append(spec.name)
        file = root / spec.name
        file.write_bytes(raw)
        return prefetch.Staged(path=file, sha256=hashlib.sha256(raw).hexdigest())

    monkeypatch.setattr(prefetch, "_fetch_stage", staged_worker)
    max_files = 0
    max_bytes = 0
    seen = []
    with prefetch.PartitionPrefetch(specs, root=work, enabled=True) as queue:
        for spec, staged in queue:
            # A09.4 skips adopt_staged when the ZIP is already cached.
            destination = work / spec.name
            result = a094.acquire_run_establishment_zip(
                release=release, name=spec.name,
                metadata={
                    "name": spec.name, "size": spec.size,
                    "etag": spec.etag, "last_modified": spec.last_modified,
                },
                destination=destination,
                download=lambda: pytest.fail("unexpected GET with warm A09.4"),
            )
            assert result.read_bytes() == raw
            seen.append(spec.name)
            stage = queue._stage
            assert stage is not None
            candidates = list(stage.glob("*.zip"))
            max_files = max(max_files, len(candidates))
            max_bytes = max(max_bytes, sum(p.stat().st_size for p in candidates))
            destination.unlink()
    assert seen == [x.name for x in specs]
    assert max_files <= 2
    assert max_bytes <= prefetch.MAX_STAGED_BYTES
    assert started
    assert not list(work.glob("a096-prefetch-*"))


def _write(path: Path, raw: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    return path
