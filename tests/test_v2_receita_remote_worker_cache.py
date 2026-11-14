from __future__ import annotations

import importlib.util
import zipfile
from io import BytesIO
from pathlib import Path


def _load_worker():
    path = Path("ops/hostgator/receita_remote_worker.py")
    spec = importlib.util.spec_from_file_location("receita_remote_worker_cache_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _zip_bytes() -> bytes:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr("Motivos.csv", b"HELLO;WORLD\n")
    return buffer.getvalue()


class _Response:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int):
        del chunk_size
        yield self.payload


class _Session:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.calls = 0
        self.auth = None

        class _Cookies:
            @staticmethod
            def clear() -> None:
                return None

        self.cookies = _Cookies()

    def get(self, url, **kwargs):
        del url, kwargs
        self.calls += 1
        return _Response(self.payload)


def _release(worker, payload_size: int):
    names = [
        "Motivos.zip",
        "Cnaes.zip",
        "Naturezas.zip",
        "Empresas0.zip",
        "Estabelecimentos0.zip",
    ]
    resources = []
    for index, name in enumerate(names):
        resources.append(
            {
                "name": name,
                "size": payload_size if name == "Motivos.zip" else index + 1,
                "etag": f'"etag-{index}"',
                "last_modified": "Fri, 18 Sep 2026 00:00:00 GMT",
                "is_collection": False,
            }
        )
    return {
        "period": "2026-09",
        "release_url": "https://arquivos.receitafederal.gov.br/release/",
        "company_files": ["Empresas0.zip"],
        "establishment_files": ["Estabelecimentos0.zip"],
        "resources": resources,
    }


def test_remote_worker_cache_reuses_and_invalidates_same_size_tamper(tmp_path: Path) -> None:
    worker = _load_worker()
    payload = _zip_bytes()
    session = _Session(payload)
    release = _release(worker, len(payload))
    cache_root = tmp_path / "2026-09"

    first, first_status = worker.acquire_cached_zip(
        session,
        release,
        "Motivos.zip",
        cache_root,
    )
    assert first_status == "downloaded"
    assert session.calls == 1
    original_sha = worker.sha256_file(first)

    second, second_status = worker.acquire_cached_zip(
        session,
        release,
        "Motivos.zip",
        cache_root,
    )
    assert second_status == "reused_unchanged"
    assert session.calls == 1
    assert worker.sha256_file(second) == original_sha

    data = bytearray(first.read_bytes())
    marker = data.find(b"HELLO")
    assert marker >= 0
    data[marker] = ord("J")
    first.write_bytes(data)
    assert first.stat().st_size == len(payload)
    assert zipfile.is_zipfile(first)
    assert worker.sha256_file(first) != original_sha

    third, third_status = worker.acquire_cached_zip(
        session,
        release,
        "Motivos.zip",
        cache_root,
    )
    assert third_status == "downloaded"
    assert session.calls == 2
    assert worker.sha256_file(third) == original_sha

    manifest = worker.load_cache_manifest(cache_root)
    entry = manifest["files"]["Motivos.zip"]
    assert entry["remote_fingerprint"]["period"] == "2026-09"
    assert entry["remote_fingerprint"]["filename"] == "Motivos.zip"
    assert entry["sha256"] == original_sha
    assert manifest["release_fingerprint"] == worker.release_fingerprint(release)


def test_remote_worker_cache_invalidates_same_period_etag_change(tmp_path: Path) -> None:
    worker = _load_worker()
    payload = _zip_bytes()
    session = _Session(payload)
    release = _release(worker, len(payload))
    cache_root = tmp_path / "2026-09"

    worker.acquire_cached_zip(session, release, "Motivos.zip", cache_root)
    assert session.calls == 1

    changed = dict(release)
    changed["resources"] = [dict(row) for row in release["resources"]]
    changed["resources"][0]["etag"] = '"etag-changed"'

    _, status = worker.acquire_cached_zip(
        session,
        changed,
        "Motivos.zip",
        cache_root,
    )
    assert status == "downloaded"
    assert session.calls == 2
