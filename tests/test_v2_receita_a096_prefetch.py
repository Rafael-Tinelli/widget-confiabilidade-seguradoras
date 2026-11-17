"""A09.6: bounded, OFF-BY-DEFAULT prefetch invariants and semantic equivalence.

All HTTP is mocked. No Receita ZIP is acquired in these tests and no Gate 4
generation is triggered. Production activates only with an explicit flag.
"""
from __future__ import annotations

import copy
import hashlib
import io
import threading
import time
import zipfile
from collections import Counter
from pathlib import Path
from types import SimpleNamespace

import pytest

from api.sources import receita_cnpj_bulk as bulk
from api.sources import receita_cnpj_identity as identity
from api.sources import receita_cnpj_prefetch as prefetch
from api.sources import receita_cnpj_run_zip_cache as a094

OFFICIAL = "https://arquivos.receitafederal.gov.br/public.php/dav/files/token/2026-11/"


def _zip(rows: str) -> bytes:
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("data.csv", rows.encode("latin1"))
    return out.getvalue()


def _partition_files() -> dict[str, bytes]:
    return {
        "Motivos.zip": _zip("01;Motivo valido\n"),
        "Cnaes.zip": _zip("6511101;Seguradoras\n"),
        "Naturezas.zip": _zip("2054;Sociedade anonima\n"),
        "Empresas0.zip": _zip("87654321;OUTRA EMPRESA SA;2054\n"),
        "Empresas1.zip": _zip("12345678;DEMO SEGURADORA SA;2054\n"),
        "Estabelecimentos0.zip": _zip(
            "87654321;0001;00;1;OUTRA EMPRESA;02;20260110;01;;"
            ";20010101;6511101;\n"
        ),
        "Estabelecimentos1.zip": _zip(
            "12345678;0001;00;1;DEMO SEGURADORA;02;20260110;01;;"
            ";20010101;6511101;\n"
        ),
    }


def _meta(blobs: dict[str, bytes]) -> dict[str, dict]:
    return {
        name: {"name": name, "size": len(data), "etag": f'"etag-{name}"',
               "last_modified": "Mon, 16 Nov 2026 10:04:47 GMT"}
        for name, data in blobs.items()
    }


def _specs(group: str = "Empresas", n: int = 3) -> list[prefetch.Partition]:
    return [
        prefetch.Partition(
            name=f"{group}{i}.zip", url=OFFICIAL + f"{group}{i}.zip",
            size=len(_zip(f"row{i};{'X' * 65}\n")),
            etag=f'"etag-{i}"',
        )
        for i in range(n)
    ]


def _worker_from_blobs(blobs: dict[str, bytes], state: dict | None = None):
    lock = threading.Lock()
    if state is None:
        state = {"active": 0, "maximum": 0, "started": [], "cancelled": []}

    def worker(
        spec: prefetch.Partition, root: Path, cancel: threading.Event,
    ) -> prefetch.Staged:
        with lock:
            state["active"] += 1
            state["maximum"] = max(state["maximum"], state["active"])
            state["started"].append(spec.name)
        try:
            for _ in range(2):
                time.sleep(0.005)
                if cancel.is_set():
                    state["cancelled"].append(spec.name)
                    raise prefetch.PrefetchRejected("cancelled")
            path = root / spec.name
            data = blobs.get(spec.name, _zip(spec.name + ";DATA\n"))
            path.write_bytes(data)
            return prefetch.Staged(path=path, sha256=hashlib.sha256(data).hexdigest())
        finally:
            with lock:
                state["active"] -= 1

    return worker, state


def test_a096_flag_is_off_by_default_and_does_not_start_workers(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.delenv(prefetch.ENV_NAME, raising=False)
    assert not prefetch.requested()
    monkeypatch.setattr(prefetch, "_fetch_stage", lambda *_a: pytest.fail("unexpected GET"))
    with prefetch.PartitionPrefetch(_specs(n=3), root=tmp_path) as q:
        assert [staged for _spec, staged in q] == [None, None, None]
    assert not list(tmp_path.glob("a096-prefetch-*"))


def test_a096_only_explicit_one_enables_prefetch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for value in ("true", "yes", "on", "0", ""):
        monkeypatch.setenv(prefetch.ENV_NAME, value)
        assert not prefetch.requested()
    monkeypatch.setenv(prefetch.ENV_NAME, "1")
    assert prefetch.requested()


@pytest.mark.parametrize("bad_name", ["../Empresas0.zip", "Empresas01.zip", "Socios0.zip"])
def test_a096_untrusted_metadata_disables_without_skipping_any_file(
    monkeypatch: pytest.MonkeyPatch, bad_name: str,
) -> None:
    names = ("Empresas0.zip", bad_name)
    meta = {
        name: {"name": name, "size": 100_000_000, "etag": "official"}
        for name in names
    }
    assert prefetch.official_partitions(OFFICIAL, names, meta) is None
    assert prefetch.official_partitions(
        "https://untrusted.example/2026-11/", ("Empresas0.zip",),
        {"Empresas0.zip": meta["Empresas0.zip"]},
    ) is None


def test_a096_metadata_requires_bounded_complete_order() -> None:
    specs = _specs(n=2)
    meta = {
        x.name: {"name": x.name, "size": x.size, "etag": x.etag} for x in specs
    }
    assert prefetch.official_partitions(OFFICIAL, [x.name for x in specs], meta) is None
    # Tiny test ZIPs are rejected under real 1-MiB minimum; this is a
    # deliberate fail-open to the original serial processing.
    for item in meta.values():
        item["size"] = 100_000_000
    assert [x.index for x in prefetch.official_partitions(
        OFFICIAL, list(meta), meta,
    )] == [0, 1]
    assert prefetch.official_partitions(
        OFFICIAL, ["Empresas1.zip", "Empresas0.zip"], meta,
    ) is None
    meta["Empresas1.zip"]["etag"] = None
    assert prefetch.official_partitions(
        OFFICIAL, list(meta), meta,
    ) is None


def test_a096_two_worker_bound_integrity_and_strict_order(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    raw = {spec.name: _zip(spec.name + ";DATA\n") for spec in _specs()}
    specs = [
        prefetch.Partition(
            name=spec.name, url=spec.url, size=len(raw[spec.name]),
            etag=spec.etag,
        )
        for spec in _specs()
    ]
    worker, state = _worker_from_blobs(raw)
    monkeypatch.setattr(prefetch, "_fetch_stage", worker)
    monkeypatch.setattr(prefetch, "MIN_FREE_RESERVE_BYTES", 0)
    seen = []
    with prefetch.PartitionPrefetch(specs, root=tmp_path, enabled=True) as queue:
        for spec, staged in queue:
            assert staged is not None
            assert staged.read_bytes() == raw[spec.name]
            seen.append(spec.name)
            local = prefetch.adopt_or_download(
                staged, tmp_path / spec.name,
                lambda: pytest.fail("unexpected serial acquisition"),
            )
            assert local.read_bytes() == raw[spec.name]
            local.unlink()
    assert seen == [f"Empresas{i}.zip" for i in range(3)]
    assert state["maximum"] == 2
    assert not list(tmp_path.glob("a096-prefetch-*"))


def test_a096_corrupt_staged_zip_falls_back_to_original_serial(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    specs = _specs(n=2)
    raw = {spec.name: _zip(spec.name + ";DATA\n") for spec in specs}
    specs = [
        prefetch.Partition(name=s.name, url=s.url, size=len(raw[s.name]))
        for s in specs
    ]
    worker, state = _worker_from_blobs(raw)
    def corrupted_worker(spec, stage, cancel):
        value = worker(spec, stage, cancel)
        if spec.index == 1:
            value.path.write_bytes(b"NOT A ZIP" + b"X" * 100)
        return value
    monkeypatch.setattr(prefetch, "_fetch_stage", corrupted_worker)
    monkeypatch.setattr(prefetch, "MIN_FREE_RESERVE_BYTES", 0)
    fallback = []
    with prefetch.PartitionPrefetch(specs, root=tmp_path, enabled=True) as queue:
        for spec, staged in queue:
            def serial(spec=spec):
                fallback.append(spec.name)
                path = tmp_path / spec.name
                path.write_bytes(raw[spec.name])
                return path
            local = prefetch.adopt_or_download(staged, tmp_path / spec.name, serial)
            assert local.read_bytes() == raw[spec.name]
            local.unlink()
    assert fallback == ["Empresas1.zip"]
    assert state["maximum"] <= 2


def test_a096_missing_disk_space_uses_serial_for_every_partition(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setattr(
        prefetch.shutil, "disk_usage",
        lambda *_a: SimpleNamespace(free=0),
    )
    monkeypatch.setattr(prefetch, "_fetch_stage", lambda *_a: pytest.fail("unexpected GET"))
    with prefetch.PartitionPrefetch(_specs(), root=tmp_path, enabled=True) as q:
        assert [staged for _spec, staged in q] == [None] * 3
    assert not list(tmp_path.glob("a096-prefetch-*"))


def test_a096_early_exit_cancels_pending_and_deletes_stage(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    specs = _specs(n=3)
    raw = {spec.name: _zip(spec.name + ";DATA\n") for spec in specs}
    specs = [
        prefetch.Partition(name=s.name, url=s.url, size=len(raw[s.name]))
        for s in specs
    ]
    worker, state = _worker_from_blobs(raw)
    monkeypatch.setattr(prefetch, "_fetch_stage", worker)
    monkeypatch.setattr(prefetch, "MIN_FREE_RESERVE_BYTES", 0)
    with prefetch.PartitionPrefetch(specs, root=tmp_path, enabled=True) as q:
        for _spec, _stage in q:
            break  # lifecycle may resolve all requested CNPJs early
    assert q._cancel.is_set()
    assert state["maximum"] <= 2
    assert "Empresas2.zip" not in state["started"]
    assert not list(tmp_path.glob("a096-prefetch-*"))


def test_a096_staging_promotion_io_error_falls_back(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    staged = tmp_path / "source.zip"
    staged.write_bytes(b"sample")
    monkeypatch.setattr(prefetch, "adopt_staged", lambda *_: (_ for _ in ()).throw(
        OSError("temporary I/O failure")
    ))
    dest = tmp_path / "destination.zip"
    calls = []
    def serial():
        calls.append(1)
        dest.write_bytes(b"ok")
        return dest
    assert prefetch.adopt_or_download(staged, dest, serial) == dest
    assert calls == [1]
    assert dest.read_bytes() == b"ok"


def _run_real_consumers(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, enabled: bool,
) -> tuple[dict, dict, Counter, list[str]]:
    blobs = _partition_files()
    metadata = _meta(blobs)
    release = bulk.ReceitaOpenDataRelease(
        base_url="https://arquivos.receitafederal.gov.br/index.php/s/token",
        release_url=OFFICIAL,
        period="2026-11",
        establishment_files=("Estabelecimentos0.zip", "Estabelecimentos1.zip"),
        resource_metadata=tuple(metadata[n] for n in (
            "Motivos.zip", "Estabelecimentos0.zip", "Estabelecimentos1.zip",
        )),
    )
    monkeypatch.setenv(prefetch.ENV_NAME, "1" if enabled else "0")
    monkeypatch.setenv("V2_BUILD_ID", f"a096-proof-{'prefetch' if enabled else 'serial'}")
    monkeypatch.setenv("RUNNER_TEMP", str(tmp_path / "runner"))
    monkeypatch.setattr(prefetch, "READ_CHUNK", 64)
    monkeypatch.setattr(prefetch, "MIN_FREE_RESERVE_BYTES", 0)

    worker, state = _worker_from_blobs(blobs)
    monkeypatch.setattr(prefetch, "_fetch_stage", worker)
    calls = Counter()

    def download(_session, _url, dest: Path):
        calls[dest.name] += 1
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(blobs[dest.name])
        return dest
    monkeypatch.setattr(bulk, "_download_zip", download)
    monkeypatch.setattr(identity, "_download_zip", download)
    monkeypatch.setattr(identity, "_release_files", lambda _s, _r: {
        "companies": ["Empresas0.zip", "Empresas1.zip"],
        "cnaes": "Cnaes.zip",
        "legal_natures": "Naturezas.zip",
        "metadata": metadata,
    })
    targets = [{
        "cnpj": "12345678000100", "entity_id": "id123",
        "legal_name": "DEMO SEGURADORA SA",
    }]
    lifecycle = bulk.refresh_filtered_lifecycle(
        targets, release=release, work_dir=tmp_path / "lifecycle",
    )
    identity_snapshot = identity.build_filtered_identity_snapshot(
        targets, ["DEMO SEGURADORA"], release=release,
        work_dir=tmp_path / "identity",
    )
    assert not list((tmp_path / "lifecycle").glob("a096-prefetch-*"))
    assert not list((tmp_path / "identity").glob("a096-prefetch-*"))
    assert a094._cache_dir() is not None
    return lifecycle, identity_snapshot, calls, state["started"]


def _semantic(data: dict) -> dict:
    clone = copy.deepcopy(data)
    clone.pop("generated_at", None)
    source = clone.get("source", {})
    for name in ("retrieved_at", "release_checked_at"):
        source.pop(name, None)
    return clone


def test_a096_end_to_end_lifecycle_identity_and_a094_equivalence(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    serial = _run_real_consumers(monkeypatch, tmp_path / "serial", False)
    parallel = _run_real_consumers(monkeypatch, tmp_path / "parallel", True)
    assert _semantic(serial[0]) == _semantic(parallel[0])
    assert _semantic(serial[1]) == _semantic(parallel[1])
    # A09.4 must save after lifecycle and avoid second Estabelecimentos GET.
    assert serial[2]["Estabelecimentos0.zip"] == 1
    assert serial[2]["Estabelecimentos1.zip"] == 1
    assert parallel[2]["Estabelecimentos0.zip"] == 0
    assert parallel[2]["Estabelecimentos1.zip"] == 0
    assert set(parallel[3]) == {
        "Empresas0.zip", "Empresas1.zip",
        "Estabelecimentos0.zip", "Estabelecimentos1.zip",
    }
    assert serial[1]["meta"]["establishment_files_scanned"] == [
        "Estabelecimentos0.zip", "Estabelecimentos1.zip",
    ]
    assert parallel[1]["meta"]["company_files_scanned"] == [
        "Empresas0.zip", "Empresas1.zip",
    ]


def test_a096_production_workflows_do_not_enable_flag() -> None:
    root = Path(".github/workflows")
    canonical = (root / "v2-gate4-full-generation-proof.yml").read_text()
    scheduled = (root / "v2-production-generation-schedule.yml").read_text()
    publish = (root / "v2-hostgator-publication.yml").read_text()
    assert prefetch.ENV_NAME not in canonical
    assert prefetch.ENV_NAME not in scheduled
    assert prefetch.ENV_NAME not in publish
    assert prefetch.MAX_WORKERS == 2
    assert prefetch.MAX_STAGED_BYTES == 4 * 1024**3


class _HTTPResponse:
    def __init__(
        self, spec: prefetch.Partition, raw: bytes, *,
        status: int = 200, length: str | None = None,
        etag: str | None = None, modified: str | None = None,
        redirected: str | None = None,
        cancel: threading.Event | None = None,
    ) -> None:
        self.status_code = status
        self.url = redirected or spec.url
        self.headers = {
            "Content-Length": length if length is not None else str(len(raw)),
            "ETag": etag if etag is not None else str(spec.etag),
            "Last-Modified": modified if modified is not None else str(spec.last_modified),
        }
        self.raw = raw
        self.closed = False
        self.cancel = cancel
        self.read_bytes = 0

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        self.closed = True

    def iter_content(self, *, chunk_size: int):
        for i in range(0, len(self.raw), chunk_size):
            chunk = self.raw[i:i + chunk_size]
            self.read_bytes += len(chunk)
            if self.cancel is not None:
                self.cancel.set()
            yield chunk


def _http_spec(raw: bytes) -> prefetch.Partition:
    return prefetch.Partition(
        name="Empresas0.zip", url=OFFICIAL + "Empresas0.zip",
        size=len(raw), etag='"etag-0"',
        last_modified="Mon, 16 Nov 2026 10:04:47 GMT",
    )


def _fake_http(
    monkeypatch: pytest.MonkeyPatch, response: _HTTPResponse,
) -> None:
    class Session:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def get(self, url, **kwargs):
            assert url == OFFICIAL + "Empresas0.zip"
            assert kwargs["stream"] is True
            assert kwargs["allow_redirects"] is True
            assert kwargs["headers"]["Accept-Encoding"] == "identity"
            assert kwargs["timeout"] == (15, 20)
            return response

    monkeypatch.setattr(prefetch.requests, "Session", Session)
    monkeypatch.setattr(bulk, "_configure_official_session", lambda _s: None)


def test_a096_real_stream_worker_verifies_zip_sha_and_closes_response(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    raw = _zip("12345678;EMPRESA DEMO;2054\n")
    spec = _http_spec(raw)
    response = _HTTPResponse(spec, raw)
    _fake_http(monkeypatch, response)
    staged = prefetch._fetch_stage(spec, tmp_path, threading.Event())
    assert staged.path.read_bytes() == raw
    assert staged.sha256 == hashlib.sha256(raw).hexdigest()
    prefetch._verify(staged.path, spec, staged.sha256)
    assert response.closed
    assert response.read_bytes == len(raw)
    assert not (tmp_path / (spec.name + ".part")).exists()


@pytest.mark.parametrize("failure", [
    "status", "content_length", "etag", "last_modified",
    "redirect", "invalid_zip", "truncated", "overflow",
])
def test_a096_untrusted_http_transfer_never_reaches_parser(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, failure: str,
) -> None:
    raw = _zip("12345678;EMPRESA DEMO;2054\n")
    spec = _http_spec(raw)
    data = raw
    options = {}
    if failure == "status":
        options["status"] = 206
    elif failure == "content_length":
        options["length"] = str(len(raw) + 9)
    elif failure == "etag":
        options["etag"] = '"different"'
    elif failure == "last_modified":
        options["modified"] = "Mon, 15 Nov 2026 10:04:47 GMT"
    elif failure == "redirect":
        options["redirected"] = "https://other.example/download.zip"
    elif failure == "invalid_zip":
        data = b"NOT A ZIP!" + b"x" * (len(raw) - 10)
    elif failure == "truncated":
        data = raw[:-10]
        options["length"] = str(len(raw))
    elif failure == "overflow":
        data = raw + b"extra"
        options["length"] = str(len(raw))
    response = _HTTPResponse(spec, data, **options)
    _fake_http(monkeypatch, response)
    with pytest.raises(prefetch.PrefetchRejected):
        prefetch._fetch_stage(spec, tmp_path, threading.Event())
    assert response.closed
    assert not (tmp_path / spec.name).exists()
    assert not (tmp_path / (spec.name + ".part")).exists()


def test_a096_cooperative_cancel_during_stream_removes_partial_zip(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    raw = _zip("12345678;EMPRESA DEMO;2054\n")
    spec = _http_spec(raw)
    cancel = threading.Event()
    response = _HTTPResponse(spec, raw, cancel=cancel)
    _fake_http(monkeypatch, response)
    with pytest.raises(prefetch.PrefetchRejected, match="cancelled"):
        prefetch._fetch_stage(spec, tmp_path, cancel)
    assert response.closed
    assert not (tmp_path / (spec.name + ".part")).exists()


def test_a096_serial_fallback_rejects_incomplete_even_if_zip_valid(
    tmp_path: Path,
) -> None:
    raw = _zip("12345678;EMPRESA DEMO;2054\n")
    spec = prefetch.Partition(
        name="Empresas0.zip", url=OFFICIAL + "Empresas0.zip",
        size=len(raw) + 2,
    )
    output = tmp_path / spec.name
    def serial() -> Path:
        output.write_bytes(raw)
        return output
    with pytest.raises(prefetch.PrefetchRejected, match="serial_fallback"):
        prefetch.adopt_or_download(None, output, serial, expected=spec)
    assert not output.exists()
