from __future__ import annotations

import hashlib
import json
import os
import signal
import shutil
import sys
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from api.v2.generation import BuildContext
from api.v2.source_snapshot import SourceObservation


DEFAULT_SUSEP_FETCH_DEADLINE_SECONDS = 900.0


class SourceCacheError(RuntimeError):
    """Raised when a cached source cannot be trusted or materialized safely."""


class SourceFetchDeadlineExceeded(TimeoutError):
    """Raised when a current SUSEP acquisition exceeds its hard wall-clock budget."""


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


def _susep_fetch_deadline_seconds(source_url: str) -> float | None:
    """Return the hard wall-clock budget for current SUSEP fetches.

    The deadline is intentionally enforced outside requests/urllib3 timeout
    semantics. A streaming response can otherwise stay alive indefinitely by
    yielding a small amount of data before each socket read timeout. The Gate 4
    runner is Linux, so SIGALRM gives the current fetch a true wall-clock cap and
    lets the existing validated-cache fallback run normally after expiry.
    """
    if "susep.gov.br" not in source_url.casefold():
        return None

    raw = os.getenv("V2_SUSEP_FETCH_DEADLINE_SECONDS", "").strip()
    if not raw:
        return DEFAULT_SUSEP_FETCH_DEADLINE_SECONDS
    try:
        seconds = float(raw)
    except ValueError as exc:
        raise SourceCacheError(
            "V2_SUSEP_FETCH_DEADLINE_SECONDS must be a positive number"
        ) from exc
    if seconds <= 0:
        raise SourceCacheError(
            "V2_SUSEP_FETCH_DEADLINE_SECONDS must be greater than zero"
        )
    return seconds


@contextmanager
def _hard_wall_clock_deadline(
    *,
    source_id: str,
    seconds: float | None,
) -> Iterator[None]:
    if seconds is None:
        yield
        return

    if (
        not hasattr(signal, "SIGALRM")
        or not hasattr(signal, "setitimer")
        or not hasattr(signal, "ITIMER_REAL")
    ):
        raise SourceCacheError(
            f"hard wall-clock deadline is unsupported for {source_id} on this platform"
        )
    if threading.current_thread() is not threading.main_thread():
        raise SourceCacheError(
            f"hard wall-clock deadline for {source_id} requires the main thread"
        )

    active_delay, active_interval = signal.getitimer(signal.ITIMER_REAL)
    if active_delay > 0 or active_interval > 0:
        raise SourceCacheError(
            f"cannot install source deadline for {source_id}: ITIMER_REAL is already active"
        )

    previous_handler = signal.getsignal(signal.SIGALRM)

    def _deadline_handler(_signum: int, _frame: object) -> None:
        raise SourceFetchDeadlineExceeded(
            f"current fetch for {source_id} exceeded {seconds:g}s wall-clock deadline"
        )

    signal.signal(signal.SIGALRM, _deadline_handler)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)


def _log_acquisition_event(event: str, **fields: object) -> None:
    payload = {"event": event, **fields}
    print(
        "V2_SOURCE_ACQUISITION "
        + json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str),
        file=sys.stderr,
        flush=True,
    )


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
    the original fetched_at timestamp is preserved in lineage. The broad catches
    are intentional boundaries: arbitrary source adapters and validators are
    allowed to fail, but every failure must be converted into stale/unavailable
    state rather than escaping before lineage can be recorded.

    Every acquisition emits start/end records to stderr so GitHub Actions keeps
    source-level progress visible even when pipeline stdout is redirected to a
    file. Current SUSEP fetches additionally receive a true wall-clock deadline;
    expiry is treated like any other current-source failure and therefore enters
    the existing hash-validated cache fallback without weakening lineage rules.
    """
    if cache.source_id != source_id or cache.source_url != source_url:
        raise SourceCacheError("cache identity differs from requested source")

    deadline_seconds = _susep_fetch_deadline_seconds(source_url)
    started = time.monotonic()
    _log_acquisition_event(
        "start",
        source_id=source_id,
        source_url=source_url,
        deadline_seconds=deadline_seconds,
    )

    def finish(result: AcquisitionResult) -> AcquisitionResult:
        lineage = result.observation.to_lineage(context)
        _log_acquisition_event(
            "end",
            source_id=source_id,
            state=lineage.state,
            used_cache=result.used_cache,
            duration_seconds=round(time.monotonic() - started, 3),
            current_error=result.current_error,
        )
        return result

    destination.parent.mkdir(parents=True, exist_ok=True)
    temp = destination.with_suffix(destination.suffix + ".incoming")
    if temp.exists():
        temp.unlink()

    current_error: str | None = None
    try:
        with _hard_wall_clock_deadline(
            source_id=source_id,
            seconds=deadline_seconds,
        ):
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
        return finish(AcquisitionResult(observation=observation, used_cache=False))
    except Exception as exc:  # noqa: BLE001
        current_error = f"{type(exc).__name__}: {exc}"
        if temp.exists():
            temp.unlink()

    try:
        fetched_at = cache.materialize(destination)
        validate_path(destination)
    except Exception:  # noqa: BLE001
        if destination.exists():
            destination.unlink()
        observation = SourceObservation(
            source_id=source_id,
            source_url=source_url,
            snapshot_path=destination,
            fetched_at=utc_now(),
            current_fetch_succeeded=False,
            state_reason=current_error,
        )
        return finish(
            AcquisitionResult(
                observation=observation,
                used_cache=False,
                current_error=current_error,
            )
        )

    observation = SourceObservation(
        source_id=source_id,
        source_url=source_url,
        snapshot_path=destination,
        fetched_at=fetched_at,
        current_fetch_succeeded=False,
        state_reason=current_error,
    )
    return finish(
        AcquisitionResult(
            observation=observation,
            used_cache=True,
            current_error=current_error,
        )
    )
