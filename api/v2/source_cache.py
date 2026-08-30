from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from api.v2.generation import BuildContext
from api.v2.source_snapshot import SourceObservation


class SourceCacheError(RuntimeError):
    """Raised when a cached source cannot be trusted or materialized safely."""


def utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_copy(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp = destination.with_suffix(destination.suffix + ".tmp")
    try:
        shutil.copy2(source, temp)
        temp.replace(destination)
    finally:
        if temp.exists():
            temp.unlink()


def _atomic_json(payload: dict, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp = destination.with_suffix(destination.suffix + ".tmp")
    try:
        temp.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temp.replace(destination)
    finally:
        if temp.exists():
            temp.unlink()


@dataclass(frozen=True)
class CachedSource:
    source_id: str
    source_url: str
    content_path: Path
    metadata_path: Path

    def store(self, fresh_path: Path, *, fetched_at: str | None = None) -> None:
        if not fresh_path.is_file():
            raise SourceCacheError(f"fresh source file not found: {fresh_path}")
        timestamp = fetched_at or utc_now()
        digest = sha256_file(fresh_path)
        _atomic_copy(fresh_path, self.content_path)
        _atomic_json(
            {
                "source_id": self.source_id,
                "source_url": self.source_url,
                "fetched_at": timestamp,
                "sha256": digest,
            },
            self.metadata_path,
        )

    def materialize(self, destination: Path) -> str:
        if not self.content_path.is_file() or not self.metadata_path.is_file():
            raise SourceCacheError(f"cache unavailable for {self.source_id}")
        metadata = json.loads(self.metadata_path.read_text(encoding="utf-8"))
        if metadata.get("source_id") != self.source_id:
            raise SourceCacheError(f"cache source_id mismatch for {self.source_id}")
        if metadata.get("source_url") != self.source_url:
            raise SourceCacheError(f"cache source_url mismatch for {self.source_id}")
        expected = str(metadata.get("sha256") or "")
        actual = sha256_file(self.content_path)
        if len(expected) != 64 or actual != expected:
            raise SourceCacheError(f"cache hash mismatch for {self.source_id}")
        fetched_at = str(metadata.get("fetched_at") or "").strip()
        if not fetched_at:
            raise SourceCacheError(f"cache fetched_at missing for {self.source_id}")
        _atomic_copy(self.content_path, destination)
        return fetched_at


@dataclass(frozen=True)
class AcquisitionResult:
    observation: SourceObservation
    used_cache: bool
    current_error: str | None = None


def acquire_with_validated_cache(
    *,
    source_id: str,
    source_url: str,
    destination: Path,
    cache: CachedSource,
    fetch_to_path: Callable[[Path], None],
    validate_path: Callable[[Path], None],
    context: BuildContext,
) -> AcquisitionResult:
    """Acquire one source, falling back only to a hash-validated prior snapshot.

    A failed current fetch never rewrites cache metadata. When fallback is used,
    the original fetched_at timestamp is preserved in lineage.
    """
    if cache.source_id != source_id or cache.source_url != source_url:
        raise SourceCacheError("cache identity differs from requested source")

    destination.parent.mkdir(parents=True, exist_ok=True)
    temp = destination.with_suffix(destination.suffix + ".incoming")
    if temp.exists():
        temp.unlink()

    current_error: str | None = None
    try:
        fetch_to_path(temp)
        validate_path(temp)
        fetched_at = utc_now()
        temp.replace(destination)
        cache.store(destination, fetched_at=fetched_at)
        observation = SourceObservation(
            source_id=source_id,
            source_url=source_url,
            snapshot_path=destination,
            fetched_at=fetched_at,
            current_fetch_succeeded=True,
        )
        return AcquisitionResult(observation=observation, used_cache=False)
    except Exception as exc:
        current_error = f"{type(exc).__name__}: {exc}"
        if temp.exists():
            temp.unlink()

    try:
        fetched_at = cache.materialize(destination)
        validate_path(destination)
    except Exception:
        if destination.exists():
            destination.unlink()
        observation = SourceObservation(
            source_id=source_id,
            source_url=source_url,
            snapshot_path=destination,
            fetched_at=utc_now(),
            current_fetch_succeeded=False,
        )
        return AcquisitionResult(
            observation=observation,
            used_cache=False,
            current_error=current_error,
        )

    observation = SourceObservation(
        source_id=source_id,
        source_url=source_url,
        snapshot_path=destination,
        fetched_at=fetched_at,
        current_fetch_succeeded=False,
    )
    return AcquisitionResult(
        observation=observation,
        used_cache=True,
        current_error=current_error,
    )
