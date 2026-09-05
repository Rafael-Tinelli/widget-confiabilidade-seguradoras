from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ALLOWED_SOURCE_STATES = {"fresh", "stale", "unavailable"}
_ALLOWED_FRESHNESS_METHODS = {
    "current_fetch",
    "current_validation",
    "validated_cache_fallback",
    "unavailable",
}
_REQUIRED_PUBLIC_FILES = {
    "search_index.json",
    "profile_manifest.json",
    "insurer_explorer.json",
    "explore_index.json",
}
_REQUIRED_PUBLIC_DIRS = {"profiles", "leaderboards", "collections"}


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_utc_timestamp(value: str) -> str:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamp must include timezone")
    return parsed.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z"
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class BuildContext:
    build_id: str
    source_head_sha: str
    generated_at: str
    workflow_run_id: str
    workflow_run_attempt: int

    @classmethod
    def from_env(
        cls,
        env: Mapping[str, str] | None = None,
        *,
        generated_at: str | None = None,
    ) -> BuildContext:
        values = dict(os.environ if env is None else env)
        source_head_sha = values.get("V2_SOURCE_HEAD_SHA") or values.get("GITHUB_SHA") or ""
        workflow_run_id = values.get("GITHUB_RUN_ID") or "local"
        attempt_raw = values.get("GITHUB_RUN_ATTEMPT") or "1"

        if not _SHA_RE.fullmatch(source_head_sha):
            raise ValueError("V2_SOURCE_HEAD_SHA/GITHUB_SHA must be a full lowercase Git SHA")
        try:
            workflow_run_attempt = int(attempt_raw)
        except ValueError as exc:
            raise ValueError("GITHUB_RUN_ATTEMPT must be an integer") from exc
        if workflow_run_attempt < 1:
            raise ValueError("GITHUB_RUN_ATTEMPT must be >= 1")

        build_id = values.get("V2_BUILD_ID") or (
            f"v2-{source_head_sha[:12]}-{workflow_run_id}-a{workflow_run_attempt}"
        )
        if not re.fullmatch(r"[A-Za-z0-9._-]+", build_id):
            raise ValueError("V2_BUILD_ID contains unsupported characters")

        timestamp = _normalize_utc_timestamp(
            values.get("V2_GENERATED_AT") or generated_at or _utc_now()
        )
        return cls(
            build_id=build_id,
            source_head_sha=source_head_sha,
            generated_at=timestamp,
            workflow_run_id=workflow_run_id,
            workflow_run_attempt=workflow_run_attempt,
        )

    def as_dict(self) -> dict:
        return {
            "build_id": self.build_id,
            "source_head_sha": self.source_head_sha,
            "generated_at": self.generated_at,
            "workflow_run_id": self.workflow_run_id,
            "workflow_run_attempt": self.workflow_run_attempt,
        }


@dataclass(frozen=True)
class SourceLineage:
    source_id: str
    source_url: str
    fetched_at: str
    state: str
    build_id: str
    sha256: str | None = None
    snapshot_path: str | None = None
    freshness_method: str | None = None
    state_reason: str | None = None

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> SourceLineage:
        source = cls(
            source_id=str(value.get("source_id") or ""),
            source_url=str(value.get("source_url") or ""),
            fetched_at=_normalize_utc_timestamp(str(value.get("fetched_at") or "")),
            state=str(value.get("state") or ""),
            build_id=str(value.get("build_id") or ""),
            sha256=(str(value["sha256"]) if value.get("sha256") else None),
            snapshot_path=(
                str(value["snapshot_path"]) if value.get("snapshot_path") else None
            ),
            freshness_method=(
                str(value["freshness_method"])
                if value.get("freshness_method")
                else None
            ),
            state_reason=(str(value["state_reason"]) if value.get("state_reason") else None),
        )
        source.validate()
        return source

    def validate(self) -> None:
        if not self.source_id:
            raise ValueError("source_id is required")
        if not self.source_url:
            raise ValueError(f"source_url is required for {self.source_id}")
        if self.state not in _ALLOWED_SOURCE_STATES:
            raise ValueError(f"invalid source state for {self.source_id}: {self.state}")
        if not self.build_id:
            raise ValueError(f"build_id is required for {self.source_id}")
        if self.sha256 is not None and not _SHA256_RE.fullmatch(self.sha256):
            raise ValueError(f"invalid sha256 for {self.source_id}")
        if self.state == "unavailable" and self.sha256 is not None:
            raise ValueError(f"unavailable source cannot claim content hash: {self.source_id}")
        if self.state in {"fresh", "stale"} and self.sha256 is None:
            raise ValueError(f"{self.state} source requires content hash: {self.source_id}")
        if self.freshness_method is not None and self.freshness_method not in _ALLOWED_FRESHNESS_METHODS:
            raise ValueError(
                f"invalid freshness method for {self.source_id}: {self.freshness_method}"
            )
        if self.state == "fresh" and self.freshness_method not in {
            None,
            "current_fetch",
            "current_validation",
        }:
            raise ValueError(f"fresh source has incompatible freshness method: {self.source_id}")
        if self.state == "stale" and self.freshness_method not in {
            None,
            "validated_cache_fallback",
        }:
            raise ValueError(f"stale source has incompatible freshness method: {self.source_id}")
        if self.state == "unavailable" and self.freshness_method not in {None, "unavailable"}:
            raise ValueError(
                f"unavailable source has incompatible freshness method: {self.source_id}"
            )

    def as_dict(self) -> dict:
        return {
            "source_id": self.source_id,
            "source_url": self.source_url,
            "fetched_at": self.fetched_at,
            "state": self.state,
            "build_id": self.build_id,
            "sha256": self.sha256,
            "snapshot_path": self.snapshot_path,
            "freshness_method": self.freshness_method,
            "state_reason": self.state_reason,
        }


def load_source_lineage(path: Path, context: BuildContext) -> list[SourceLineage]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    raw_sources = payload.get("sources") if isinstance(payload, dict) else None
    if not isinstance(raw_sources, list) or not raw_sources:
        raise ValueError("source lineage must contain a non-empty sources list")

    sources = [SourceLineage.from_mapping(item) for item in raw_sources]
    ids = [source.source_id for source in sources]
    if len(ids) != len(set(ids)):
        raise ValueError("source lineage contains duplicate source_id values")
    mismatched = [source.source_id for source in sources if source.build_id != context.build_id]
    if mismatched:
        raise ValueError(f"source lineage build_id mismatch: {mismatched}")
    return sorted(sources, key=lambda source: source.source_id)


def load_source_lineages(
    paths: Iterable[Path],
    context: BuildContext,
) -> list[SourceLineage]:
    sources = [
        source
        for path in paths
        for source in load_source_lineage(path, context)
    ]
    ids = [source.source_id for source in sources]
    duplicates = sorted({source_id for source_id in ids if ids.count(source_id) > 1})
    if duplicates:
        raise ValueError(f"combined source lineage contains duplicate source_id values: {duplicates}")
    return sorted(sources, key=lambda source: source.source_id)


def _public_files(public_dir: Path) -> list[Path]:
    if not public_dir.is_dir():
        raise ValueError(f"public package directory not found: {public_dir}")
    files = [
        path
        for path in public_dir.rglob("*")
        if path.is_file() and path.name != "distribution_manifest.json"
    ]
    non_json = [path for path in files if path.suffix != ".json"]
    if non_json:
        raise ValueError(f"public package contains non-JSON files: {non_json[:5]}")
    return sorted(files, key=lambda path: path.relative_to(public_dir).as_posix())


def _validate_public_shape(public_dir: Path, files: Iterable[Path]) -> None:
    relative = {path.relative_to(public_dir).as_posix() for path in files}
    missing_files = sorted(_REQUIRED_PUBLIC_FILES - relative)
    if missing_files:
        raise ValueError(f"public package missing required files: {missing_files}")
    for directory in sorted(_REQUIRED_PUBLIC_DIRS):
        prefix = f"{directory}/"
        if not any(path.startswith(prefix) for path in relative):
            raise ValueError(f"public package missing required directory content: {directory}")


def build_distribution_manifest(
    *,
    public_dir: Path,
    context: BuildContext,
    sources: Iterable[SourceLineage],
) -> dict:
    source_list = sorted(sources, key=lambda source: source.source_id)
    if not source_list:
        raise ValueError("at least one source lineage record is required")
    ids = [source.source_id for source in source_list]
    duplicates = sorted({source_id for source_id in ids if ids.count(source_id) > 1})
    if duplicates:
        raise ValueError(f"distribution manifest has duplicate source_id values: {duplicates}")
    if any(source.build_id != context.build_id for source in source_list):
        raise ValueError("all source lineage records must match the current build_id")

    files = _public_files(public_dir)
    _validate_public_shape(public_dir, files)

    file_rows = []
    package_digest = hashlib.sha256()
    for path in files:
        relative = path.relative_to(public_dir).as_posix()
        digest = _sha256_file(path)
        size = path.stat().st_size
        file_rows.append({"path": relative, "sha256": digest, "bytes": size})
        package_digest.update(relative.encode("utf-8"))
        package_digest.update(b"\0")
        package_digest.update(digest.encode("ascii"))
        package_digest.update(b"\0")

    states: dict[str, int] = {state: 0 for state in sorted(_ALLOWED_SOURCE_STATES)}
    for source in source_list:
        source.validate()
        states[source.state] += 1

    return {
        "artifact": "v2_public_distribution_manifest",
        "version": 1,
        "build": context.as_dict(),
        "source_lineage": {
            "count": len(source_list),
            "state_counts": states,
            "sources": [source.as_dict() for source in source_list],
        },
        "public_package": {
            "files_count": len(file_rows),
            "package_sha256": package_digest.hexdigest(),
            "files": file_rows,
        },
        "publication_policy": {
            "atomic_publish_required": True,
            "partial_publish_forbidden": True,
            "manifest_hash_verification_required": True,
            "retain_previous_generation_for_rollback": True,
        },
    }


def write_distribution_manifest(
    *,
    public_dir: Path,
    context: BuildContext,
    sources: Iterable[SourceLineage],
) -> Path:
    payload = build_distribution_manifest(
        public_dir=public_dir,
        context=context,
        sources=sources,
    )
    output = public_dir / "distribution_manifest.json"
    output.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return output
