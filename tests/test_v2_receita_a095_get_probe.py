"""Offline tests for the second A09.5 ordinary-GET bounded transport proof."""
from __future__ import annotations

import hashlib
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import a095_bounded_get_probe as get_probe


class FakeResponse:
    def __init__(
        self, *,
        content: bytes,
        size: int = 100_000_000,
        status: int = 200,
        content_encoding: str = "identity",
        content_length: str | None = None,
        content_range: str | None = None,
        etag: str | None = '"etag-1"',
        oversized_chunk: bool = False,
    ) -> None:
        self.content = content
        self.status_code = status
        self.headers: dict[str, str] = {"Content-Encoding": content_encoding}
        if content_length is not None:
            self.headers["Content-Length"] = content_length
        else:
            self.headers["Content-Length"] = str(size)
        if content_range is not None:
            self.headers["Content-Range"] = content_range
        if etag is not None:
            self.headers["ETag"] = etag
        self.closed = False
        self.yielded = 0
        self.oversized_chunk = oversized_chunk

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.closed = True

    def iter_content(self, chunk_size: int):
        if self.oversized_chunk:
            yield self.content
            return
        for start in range(0, len(self.content), chunk_size):
            self.yielded += 1
            yield self.content[start:start + chunk_size]


@pytest.mark.parametrize("changes", [
    {"status": 206},
    {"status": 404},
    {"content_encoding": "gzip"},
    {"content_length": "200"},
    {"content_length": "-1"},
    {"content_range": "bytes 0-1023/100000000"},
])
def test_a095_get_rejects_unexpected_representation(changes: dict) -> None:
    resp = FakeResponse(content=b"PK\x03\x04" + b"x" * 1020, **changes)
    with pytest.raises(get_probe.GetProbeError):
        get_probe._check_get_headers(resp, 100_000_000)


def test_a095_get_checks_official_size_and_accepts_chunked_without_length() -> None:
    resp = FakeResponse(content=b"PK\x03\x04" + b"x" * 1020)
    assert get_probe._check_get_headers(resp, 100_000_000)["etag"] == '"etag-1"'
    resp.headers.pop("Content-Length")
    assert get_probe._check_get_headers(resp, 100_000_000)["content_length_present"] is False


def _fake_session(monkeypatch: pytest.MonkeyPatch, response: FakeResponse):
    class Session:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def get(self, *_args, **kwargs):
            assert "Range" not in kwargs["headers"]
            assert kwargs["headers"]["Accept-Encoding"] == "identity"
            assert kwargs["stream"] is True
            assert kwargs["allow_redirects"] is True
            assert kwargs["timeout"] == (10, 20)
            return response

    monkeypatch.setattr(get_probe.requests, "Session", Session)
    monkeypatch.setattr(get_probe, "_configure_official_session", lambda _: None)
    monkeypatch.setattr(get_probe, "READ_LIMIT", 1024)
    monkeypatch.setattr(get_probe, "READ_CHUNK", 128)


def _resource(index: int = 0) -> dict:
    return {
        "url": f"https://official.example/Estabelecimentos{index}.zip",
        "size": 100_000_000, "file_index": index,
    }


def test_a095_get_stops_at_limit_and_closes_even_with_more_server_bytes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    data = b"PK\x03\x04" + b"x" * 2044
    resp = FakeResponse(content=data)
    _fake_session(monkeypatch, resp)
    item = get_probe._get_prefix(_resource())
    assert item["read_bytes"] == 1024
    assert item["sha256_prefix"] == hashlib.sha256(data[:1024]).hexdigest()
    assert item["http_status"] == 200
    assert resp.closed
    assert resp.yielded == 8  # Not all sixteen chunks were consumed.


@pytest.mark.parametrize("payload,reason", [
    (b"<html>" + b"x" * 1018, "unexpected_zip_local_header"),
    (b"PK\x03\x04" + b"x" * 20, "truncated_get_prefix"),
])
def test_a095_get_rejects_html_or_short_reads(
    monkeypatch: pytest.MonkeyPatch, payload: bytes, reason: str,
) -> None:
    resp = FakeResponse(content=payload)
    _fake_session(monkeypatch, resp)
    with pytest.raises(get_probe.GetProbeError, match=reason):
        get_probe._get_prefix(_resource())
    assert resp.closed


def test_a095_get_rejects_badly_sized_stream_chunks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resp = FakeResponse(
        content=b"PK\x03\x04" + b"x" * 1020,
        oversized_chunk=True,
    )
    _fake_session(monkeypatch, resp)
    with pytest.raises(get_probe.GetProbeError, match="prefix_read_limit_exceeded"):
        get_probe._get_prefix(_resource())
    assert resp.closed


def test_a095_get_preflight_rejects_invalid_resource_without_http(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(get_probe.requests, "Session", lambda: pytest.fail("unexpected HTTP"))
    with pytest.raises(get_probe.GetProbeError):
        get_probe._get_prefix({"size": 1, "file_index": 0})
    with pytest.raises(get_probe.GetProbeError):
        get_probe._get_prefix({"size": 1025, "file_index": True})


def _fake_run(monkeypatch: pytest.MonkeyPatch, mismatch: str | None = None):
    monkeypatch.setattr(get_probe, "READ_LIMIT", 1024)
    resources = [_resource(0), _resource(1)]
    release = SimpleNamespace(period="2026-10", release_url="https://official.example/2026-10/")
    versions = iter(["a" * 64, "b" * 64]) if mismatch == "release" else None

    def manifest():
        return release, next(versions) if versions is not None else "a" * 64, resources

    monkeypatch.setattr(get_probe, "_release_resources", manifest)
    count = {}
    active = 0
    maximum = 0
    lock = threading.Lock()

    def fetch(resource: dict):
        nonlocal active, maximum
        index = resource["file_index"]
        with lock:
            count[index] = count.get(index, 0) + 1
            active += 1
            maximum = max(maximum, active)
            number = count[index]
        time.sleep(0.02)
        with lock:
            active -= 1
        digest = ("a" if index == 0 else "b") * 64
        if mismatch == "digest" and index == 1 and number == 2:
            digest = "f" * 64
        return {
            "file_index": index,
            "read_bytes": get_probe.READ_LIMIT,
            "sha256_prefix": digest,
            "etag": '"same"',
            "elapsed_seconds": 0.02,
        }

    monkeypatch.setattr(get_probe, "_get_prefix", fetch)
    return count, lambda: maximum


def test_a095_get_compares_one_vs_two_without_extra_requests(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    counts, maximum = _fake_run(monkeypatch)
    report = get_probe.run_get_probe()
    assert report["status"] == "measured"
    assert report["get_requests"] == 4
    assert report["observed_application_read_bytes"] == 4096
    assert report["max_application_read_bytes"] == 4096
    assert set(counts) == {0, 1}
    assert counts == {0: 2, 1: 2}
    assert maximum() == 2
    assert report["observed_speedup"] > 1


@pytest.mark.parametrize("mismatch", ["digest", "release"])
def test_a095_get_rejects_mutated_content_or_manifest(
    monkeypatch: pytest.MonkeyPatch, mismatch: str,
) -> None:
    _fake_run(monkeypatch, mismatch=mismatch)
    with pytest.raises(get_probe.GetProbeError):
        get_probe.run_get_probe()


def test_a095_get_workflow_manual_read_only_and_not_hostgator() -> None:
    workflow = Path(".github/workflows/v2-a095-get-transport-probe.yml").read_text()
    publisher = Path(".github/workflows/v2-hostgator-publication.yml").read_text()
    code = Path("scripts/a095_bounded_get_probe.py").read_text()
    assert "name: V2 A09.5 Bounded GET Probe" in workflow
    assert "workflow_dispatch:" in workflow
    assert "if: inputs.confirm_probe == 'GET'" in workflow
    assert "timeout-minutes: 8" in workflow
    assert "actions/cache/restore" not in workflow
    assert "actions/cache/save" not in workflow
    assert "gate4_pipeline" not in workflow
    assert "v2-hostgator" not in workflow
    assert "data/derived/v2/public" not in workflow
    assert "V2 A09.5 Bounded GET Probe" not in publisher
    assert "Range" not in code.split('headers={"Accept-Encoding": "identity"}')[1].split("stream=True")[0]
    assert "MAX_CONCURRENT = 2" in code
    assert "READ_LIMIT = 8 * 1024 * 1024" in code
