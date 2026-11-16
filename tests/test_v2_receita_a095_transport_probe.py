"""A09.5: bounded, offline proof of safe two-connection transport probing."""
from __future__ import annotations

import hashlib
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import a095_bounded_transport_probe as probe


class FakeResponse:
    def __init__(
        self, *,
        status: int = 206, content: bytes = b"0123456789",
        content_range: str = "bytes 0-9/100",
        content_length: str = "10",
    ) -> None:
        self.status_code = status
        self.headers = {
            "Content-Range": content_range,
            "Content-Length": content_length,
            "Content-Encoding": "identity",
        }
        self.content = content

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None

    def iter_content(self, chunk_size: int):
        assert chunk_size <= 256 * 1024
        yield self.content


@pytest.mark.parametrize("bad", [
    {"status": 200},
    {"status": 404},
    {"content_range": "bytes 0-8/100"},
    {"content_range": "bytes 0-9/*"},
    {"content_range": "bytes 10-19/100"},
    {"content_length": "11"},
    {"content_length": "-1"},
])
def test_a095_rejects_missing_or_invalid_range_headers(bad) -> None:
    response = FakeResponse(**bad)
    with pytest.raises(probe.RangeUnsupported):
        probe._require_range_response(response, size=100, count=10)


def test_a095_accepts_exact_range_and_size() -> None:
    probe._require_range_response(FakeResponse(), size=100, count=10)


def test_a095_rejects_unexpected_content_encoding() -> None:
    response = FakeResponse()
    response.headers["Content-Encoding"] = "gzip"
    with pytest.raises(probe.RangeUnsupported, match="encoding"):
        probe._require_range_response(response, size=100, count=10)


@pytest.mark.parametrize("payload", [b"012345678", b"0123456789X"])
def test_a095_rejects_truncated_or_oversized_stream(
    monkeypatch: pytest.MonkeyPatch, payload: bytes,
) -> None:
    class Session:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def get(self, *_args, **_kwargs):
            assert _kwargs["headers"]["Range"] == "bytes=0-9"
            assert _kwargs["stream"] is True
            return FakeResponse(content=payload)

    monkeypatch.setattr(probe.requests, "Session", Session)
    monkeypatch.setattr(probe, "_configure_official_session", lambda _s: None)
    with pytest.raises(probe.ProbeError):
        probe._transfer_range(
            url="https://test.invalid/Estabelecimentos0.zip",
            size=100, count=10, file_index=0,
        )


def test_a095_local_stream_hash_and_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    class Session:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def get(self, *_args, **_kwargs):
            assert _kwargs["timeout"] == (10, 30)
            assert _kwargs["stream"] is True
            assert _kwargs["headers"]["Accept-Encoding"] == "identity"
            return FakeResponse()

    monkeypatch.setattr(probe.requests, "Session", Session)
    monkeypatch.setattr(probe, "_configure_official_session", lambda _s: None)
    got = probe._transfer_range(
        url="https://test.invalid/Estabelecimentos0.zip",
        size=100, count=10, file_index=0,
    )
    assert got["bytes"] == 10
    assert got["sha256_prefix"] == hashlib.sha256(b"0123456789").hexdigest()[:12]
    assert got["elapsed_seconds"] >= 0


def test_a095_refuses_more_than_eight_mib(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(probe.requests, "Session", lambda: pytest.fail("unexpected HTTP"))
    with pytest.raises(probe.ProbeError, match="probe bound"):
        probe._transfer_range(
            url="https://test.invalid/foo", size=10**9,
            count=probe.RANGE_BYTES + 1, file_index=0,
        )


def test_a095_two_connections_only_and_no_full_download(monkeypatch: pytest.MonkeyPatch) -> None:
    item = [
        {"name": f"Estabelecimentos{i}.zip", "size": 100_000_000,
         "file_index": i, "url": f"https://test.invalid/{i}"}
        for i in (0, 1)
    ]
    release = SimpleNamespace(period="2026-10", release_url="https://test.invalid/")
    monkeypatch.setattr(probe, "_release_metadata", lambda: (release, "a" * 64, item))
    calls: list[tuple[int, int]] = []

    def fake_transfer(*, url, size, count, file_index):
        calls.append((count, file_index))
        time.sleep(0.015)
        return {
            "bytes": count, "file_index": file_index,
            "sha256_prefix": str(file_index) * 12,
            "elapsed_seconds": 0.015,
        }

    monkeypatch.setattr(probe, "_transfer_range", fake_transfer)
    result = probe.run_transport_probe()
    assert result["status"] == "measured"
    assert calls.count((1024, 0)) == 1
    assert calls.count((probe.RANGE_BYTES, 0)) == 2
    assert calls.count((probe.RANGE_BYTES, 1)) == 2
    assert result["max_concurrent_connections"] == 2
    assert result["total_range_bytes_requested"] == 1024 + 4 * probe.RANGE_BYTES
    assert result["observed_speedup"] > 1.0


def test_a095_rejects_publication_change(monkeypatch: pytest.MonkeyPatch) -> None:
    items = [
        {"name": f"Estabelecimentos{i}.zip", "size": 10**9,
         "file_index": i, "url": f"https://test.invalid/{i}"}
        for i in (0, 1)
    ]
    release = SimpleNamespace(period="2026-10", release_url="https://test.invalid/")
    fingerprint = iter(["a" * 64, "b" * 64])
    monkeypatch.setattr(
        probe, "_release_metadata", lambda: (release, next(fingerprint), items)
    )
    monkeypatch.setattr(
        probe, "_transfer_range",
        lambda *, url, size, count, file_index: {
            "bytes": count, "file_index": file_index,
            "sha256_prefix": "a" * 12, "elapsed_seconds": 0.001,
        },
    )
    with pytest.raises(probe.ProbeError, match="publication changed"):
        probe.run_transport_probe()


def test_a095_workflow_manual_and_nonpublishing() -> None:
    workflow = Path(".github/workflows/v2-a095-transport-probe.yml").read_text()
    deploy = Path(".github/workflows/v2-hostgator-publication.yml").read_text()
    assert "name: V2 A09.5 Bounded Transport Probe" in workflow
    assert "workflow_dispatch:" in workflow
    assert "if: inputs.confirm_probe == 'PROBE'" in workflow
    assert "timeout-minutes: 8" in workflow
    assert "actions/cache/restore" not in workflow
    assert "actions/cache/save" not in workflow
    assert "gate4_pipeline" not in workflow
    assert "v2-hostgator" not in workflow
    assert "data/derived/v2/public" not in workflow
    assert "V2 A09.5 Bounded Transport Probe" not in deploy
