from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pytest

from api.v2.generation import BuildContext
from api.v2.source_cache import (
    CachedSource,
    acquire_with_validated_cache,
    sha256_file,
)

HEAD = "d" * 40
BUILD_ID = "v2-dddddddddddd-789-a1"


def _context() -> BuildContext:
    return BuildContext.from_env(
        {
            "V2_SOURCE_HEAD_SHA": HEAD,
            "GITHUB_RUN_ID": "789",
            "GITHUB_RUN_ATTEMPT": "1",
            "V2_BUILD_ID": BUILD_ID,
            "V2_GENERATED_AT": "2026-08-30T20:00:00Z",
        }
    )


def _cache(tmp_path: Path) -> CachedSource:
    return CachedSource(
        source_id="official_test_source",
        source_url="https://example.test/source",
        content_path=tmp_path / "cache" / "source.bin",
        metadata_path=tmp_path / "cache" / "source.meta.json",
    )


def _susep_cache(tmp_path: Path) -> CachedSource:
    return CachedSource(
        source_id="susep_test_source",
        source_url="https://www2.susep.gov.br/example/source",
        content_path=tmp_path / "cache" / "susep.bin",
        metadata_path=tmp_path / "cache" / "susep.meta.json",
    )


def _validate_nonempty(path: Path) -> None:
    if not path.is_file() or path.stat().st_size == 0:
        raise ValueError("empty source")


def test_fresh_acquisition_updates_cache_and_lineage(tmp_path: Path, capsys):
    destination = tmp_path / "work" / "source.bin"
    cache = _cache(tmp_path)

    def fetch(path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"fresh-content")

    result = acquire_with_validated_cache(
        source_id=cache.source_id,
        source_url=cache.source_url,
        destination=destination,
        cache=cache,
        fetch_to_path=fetch,
        validate_path=_validate_nonempty,
        context=_context(),
    )

    lineage = result.observation.to_lineage(_context())
    metadata = json.loads(cache.metadata_path.read_text(encoding="utf-8"))
    stderr = capsys.readouterr().err
    assert lineage.state == "fresh"
    assert result.used_cache is False
    assert destination.read_bytes() == b"fresh-content"
    assert cache.content_path.read_bytes() == b"fresh-content"
    assert metadata["sha256"] == sha256_file(cache.content_path)
    assert metadata["fetched_at"] == lineage.fetched_at
    assert '"event": "start"' in stderr
    assert '"event": "end"' in stderr
    assert '"source_id": "official_test_source"' in stderr
    assert '"state": "fresh"' in stderr
    assert '"duration_seconds":' in stderr


def test_failed_current_fetch_reuses_valid_cache_with_original_timestamp(tmp_path: Path):
    destination = tmp_path / "work" / "source.bin"
    cache = _cache(tmp_path)
    seed = tmp_path / "seed.bin"
    seed.write_bytes(b"validated-previous-content")
    cache.store(seed, fetched_at="2026-08-29T10:00:00Z")

    def fail_fetch(_path: Path) -> None:
        raise TimeoutError("official endpoint timed out")

    result = acquire_with_validated_cache(
        source_id=cache.source_id,
        source_url=cache.source_url,
        destination=destination,
        cache=cache,
        fetch_to_path=fail_fetch,
        validate_path=_validate_nonempty,
        context=_context(),
    )

    lineage = result.observation.to_lineage(_context())
    assert lineage.state == "stale"
    assert lineage.fetched_at == "2026-08-29T10:00:00Z"
    assert result.used_cache is True
    assert "TimeoutError" in str(result.current_error)
    assert destination.read_bytes() == b"validated-previous-content"


def test_failed_fetch_and_invalid_cache_becomes_unavailable(tmp_path: Path):
    destination = tmp_path / "work" / "source.bin"
    cache = _cache(tmp_path)
    cache.content_path.parent.mkdir(parents=True, exist_ok=True)
    cache.content_path.write_bytes(b"tampered")
    cache.metadata_path.write_text(
        json.dumps(
            {
                "source_id": cache.source_id,
                "source_url": cache.source_url,
                "fetched_at": "2026-08-29T10:00:00Z",
                "sha256": "0" * 64,
            }
        ),
        encoding="utf-8",
    )

    def fail_fetch(_path: Path) -> None:
        raise OSError("network unavailable")

    result = acquire_with_validated_cache(
        source_id=cache.source_id,
        source_url=cache.source_url,
        destination=destination,
        cache=cache,
        fetch_to_path=fail_fetch,
        validate_path=_validate_nonempty,
        context=_context(),
    )

    lineage = result.observation.to_lineage(_context())
    assert lineage.state == "unavailable"
    assert lineage.sha256 is None
    assert destination.exists() is False


@pytest.mark.skipif(os.name != "posix", reason="SIGALRM hard deadline is POSIX-only")
def test_susep_wall_clock_deadline_falls_back_to_valid_cache(
    tmp_path: Path,
    monkeypatch,
    capsys,
):
    destination = tmp_path / "work" / "susep.bin"
    cache = _susep_cache(tmp_path)
    seed = tmp_path / "seed.bin"
    seed.write_bytes(b"validated-susep-cache")
    cache.store(seed, fetched_at="2026-08-29T10:00:00Z")
    monkeypatch.setenv("V2_SUSEP_FETCH_DEADLINE_SECONDS", "0.05")

    def slow_fetch(path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        time.sleep(0.25)
        path.write_bytes(b"too-late")

    started = time.monotonic()
    result = acquire_with_validated_cache(
        source_id=cache.source_id,
        source_url=cache.source_url,
        destination=destination,
        cache=cache,
        fetch_to_path=slow_fetch,
        validate_path=_validate_nonempty,
        context=_context(),
    )
    elapsed = time.monotonic() - started

    lineage = result.observation.to_lineage(_context())
    stderr = capsys.readouterr().err
    assert elapsed < 0.2
    assert lineage.state == "stale"
    assert result.used_cache is True
    assert destination.read_bytes() == b"validated-susep-cache"
    assert "SourceFetchDeadlineExceeded" in str(result.current_error)
    assert '"deadline_seconds": 0.05' in stderr
    assert '"state": "stale"' in stderr
