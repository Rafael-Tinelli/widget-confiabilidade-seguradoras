from __future__ import annotations

import hashlib
import json
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
    current_fetch_succeeded: bool = False
    current_validation_succeeded: bool = False
    state_reason: str | None = None

    def to_lineage(self, context: BuildContext) -> SourceLineage:
        fetched_at = _normalize_timestamp(self.fetched_at)
        exists = self.snapshot_path.is_file()
        if (
            self.current_fetch_succeeded
            and self.current_validation_succeeded
        ):
            raise ValueError(
                f"source {self.source_id!r} cannot be both freshly fetched and cache-validated"
            )
        current_is_fresh = (
            self.current_fetch_succeeded or self.current_validation_succeeded
        )
        if current_is_fresh and not exists:
            raise ValueError(
                f"fresh source {self.source_id!r} has no materialized snapshot"
            )

        if current_is_fresh:
            state = "fresh"
            freshness_method = (
                "current_fetch"
                if self.current_fetch_succeeded
                else "current_validation"
            )
        elif exists:
            state = "stale"
            freshness_method = "validated_cache_fallback"
        else:
            state = "unavailable"
            freshness_method = "unavailable"

        return SourceLineage.from_mapping(
            {
                "source_id": self.source_id,
                "source_url": self.source_url,
                "fetched_at": fetched_at,
                "state": state,
                "build_id": context.build_id,
                "sha256": _sha256(self.snapshot_path) if exists else None,
                "snapshot_path": str(self.snapshot_path) if exists else None,
                "freshness_method": freshness_method,
                "state_reason": self.state_reason,
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


def source_lineage_payload(
    observations: list[SourceObservation],
    context: BuildContext,
) -> dict:
    sources = build_source_lineage(observations, context)
    states = {"fresh": 0, "stale": 0, "unavailable": 0}
    methods: dict[str, int] = {}
    for source in sources:
        states[source.state] += 1
        method = source.freshness_method or "unspecified"
        methods[method] = methods.get(method, 0) + 1
    return {
        "artifact": "v2_source_lineage",
        "version": 2,
        "build": context.as_dict(),
        "state_counts": states,
        "freshness_method_counts": dict(sorted(methods.items())),
        "sources": [source.as_dict() for source in sources],
    }


def write_source_lineage(
    path: Path,
    observations: list[SourceObservation],
    context: BuildContext,
) -> Path:
    payload = source_lineage_payload(observations, context)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path
