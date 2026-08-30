from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from api.v2.generation import BuildContext, SourceLineage


def _normalize_timestamp(value: str) -> str:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("source fetched_at must include timezone")
    return parsed.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class SourceObservation:
    source_id: str
    source_url: str
    snapshot_path: Path
    fetched_at: str
    current_fetch_succeeded: bool

    def to_lineage(self, context: BuildContext) -> SourceLineage:
        fetched_at = _normalize_timestamp(self.fetched_at)
        exists = self.snapshot_path.is_file()
        if self.current_fetch_succeeded and not exists:
            raise ValueError(
                f"fresh source {self.source_id!r} has no materialized snapshot"
            )

        if self.current_fetch_succeeded:
            state = "fresh"
        elif exists:
            state = "stale"
        else:
            state = "unavailable"

        return SourceLineage.from_mapping(
            {
                "source_id": self.source_id,
                "source_url": self.source_url,
                "fetched_at": fetched_at,
                "state": state,
                "build_id": context.build_id,
                "sha256": _sha256(self.snapshot_path) if exists else None,
                "snapshot_path": str(self.snapshot_path) if exists else None,
            }
        )


def build_source_lineage(
    observations: list[SourceObservation],
    context: BuildContext,
) -> list[SourceLineage]:
    if not observations:
        raise ValueError("at least one source observation is required")
    ids = [observation.source_id for observation in observations]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate source observation")
    return sorted(
        (observation.to_lineage(context) for observation in observations),
        key=lambda source: source.source_id,
    )
