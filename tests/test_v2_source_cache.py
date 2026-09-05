from __future__ import annotations

import json
from pathlib import Path

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


def _validate_nonempty(path: Path) -> None:
    if not path.is_file() or path.stat().st_size == 0:
        raise ValueError("empty source")


def test_fresh_acquisition_updates_cache_and_lineage(tmp_path: Path):
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
    assert lineage.state == "fresh"
    assert result.used_cache is False
    assert destination.read_bytes() == b"fresh-content"
    assert cache.content_path.read_bytes() == b"fresh-content"
    assert metadata["sha256"] == sha256_file(cache.content_path)
    assert metadata["fetched_at"] == lineage.fetched_at


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
