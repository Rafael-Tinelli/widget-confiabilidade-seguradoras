from __future__ import annotations

import argparse
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from api import build_consumidor_gov as consumer_gov_build
from api.v2.build_conduct_source_snapshot import (
    CONSUMER_MANIFEST,
    DEFAULT_CACHE_DIR,
    ConductSourceSnapshotError,
    _cache_consumer_manifest,
    _consumer_manifest_payload,
    _read_gzip_json,
    _validate_consumer_manifest,
    _write_json,
)


class ConductConsumerBootstrapError(RuntimeError):
    """Raised when the versioned Consumer.gov core cannot seed Gate 4 safely."""


def _normalize_timestamp(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ConductConsumerBootstrapError(
            f"invalid versioned Consumer.gov generated_at: {value!r}"
        ) from exc
    if parsed.tzinfo is None:
        raise ConductConsumerBootstrapError(
            "versioned Consumer.gov generated_at must include timezone"
        )
    return (
        parsed.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _git(
    repo_root: Path,
    *args: str,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(repo_root), *args],
        check=check,
        capture_output=True,
        text=True,
    )


def _git_head(repo_root: Path) -> str:
    head = _git(repo_root, "rev-parse", "HEAD").stdout.strip()
    if len(head) != 40:
        raise ConductConsumerBootstrapError(f"invalid Git HEAD: {head!r}")
    expected = os.getenv("V2_SOURCE_HEAD_SHA") or os.getenv("GITHUB_SHA") or ""
    if expected and expected != head:
        raise ConductConsumerBootstrapError(
            f"bootstrap Git HEAD mismatch: workspace={head} expected={expected}"
        )
    return head


def _relative_to_repo(path: Path, repo_root: Path) -> str:
    try:
        return path.resolve().relative_to(repo_root.resolve()).as_posix()
    except ValueError as exc:
        raise ConductConsumerBootstrapError(
            f"Consumer.gov bootstrap path escapes repository: {path}"
        ) from exc


def _require_versioned_pristine(paths: list[Path], repo_root: Path) -> list[str]:
    relative = [_relative_to_repo(path, repo_root) for path in paths]
    for item in relative:
        result = _git(repo_root, "ls-files", "--error-unmatch", "--", item, check=False)
        if result.returncode != 0:
            raise ConductConsumerBootstrapError(
                f"Consumer.gov bootstrap file is not versioned: {item}"
            )
    diff = _git(repo_root, "diff", "--quiet", "HEAD", "--", *relative, check=False)
    if diff.returncode != 0:
        if diff.returncode == 1:
            raise ConductConsumerBootstrapError(
                "Consumer.gov bootstrap files differ from Git HEAD"
            )
        raise ConductConsumerBootstrapError(
            f"git diff failed while validating Consumer.gov bootstrap: {diff.stderr.strip()}"
        )
    return relative


def _validate_versioned_materialization(
    *,
    repo_root: Path,
) -> tuple[dict[str, Any], str, str, list[str]]:
    try:
        materialized = _consumer_manifest_payload()
    except (OSError, ValueError, ConductSourceSnapshotError) as exc:
        raise ConductConsumerBootstrapError(
            f"versioned Consumer.gov core is not structurally valid: {exc}"
        ) from exc

    aggregate_row = materialized.get("aggregate") or {}
    aggregate_path = Path(str(aggregate_row.get("path") or ""))
    try:
        aggregate = _read_gzip_json(aggregate_path)
    except (OSError, UnicodeError, ValueError, ConductSourceSnapshotError) as exc:
        raise ConductConsumerBootstrapError(
            f"cannot read versioned Consumer.gov aggregate: {exc}"
        ) from exc
    meta = aggregate.get("meta") if isinstance(aggregate.get("meta"), dict) else {}
    status = str(meta.get("status") or "").strip().lower()
    invalid_months = list(meta.get("invalid_months") or [])
    if status != "ok" or invalid_months:
        raise ConductConsumerBootstrapError(
            "versioned Consumer.gov aggregate is not a complete validated snapshot: "
            f"status={status!r} invalid_months={invalid_months}"
        )

    selected_months = [
        str(item.get("month") or "") for item in materialized.get("months") or []
    ]
    declared_months = sorted(
        {str(month) for month in (meta.get("months") or []) if str(month).strip()}
    )
    if declared_months != selected_months:
        raise ConductConsumerBootstrapError(
            "versioned Consumer.gov aggregate/monthly window mismatch: "
            f"aggregate={declared_months} selected={selected_months}"
        )

    paths = [aggregate_path]
    for item in materialized.get("months") or []:
        month = str(item.get("month") or "")
        path = Path(str(item.get("path") or ""))
        payload = _read_gzip_json(path)
        month_meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
        embedded_month = str(month_meta.get("month") or month_meta.get("ym") or "")
        if embedded_month != month or str(month_meta.get("status") or "ok").lower() != "ok":
            raise ConductConsumerBootstrapError(
                f"versioned Consumer.gov monthly snapshot is incoherent: {path}"
            )
        paths.append(path)

    tracked_paths = _require_versioned_pristine(paths, repo_root)
    fetched_at = _normalize_timestamp(str(meta.get("generated_at") or ""))
    return materialized, fetched_at, _git_head(repo_root), tracked_paths


def bootstrap_consumer_cache(
    *,
    cache_dir: Path = DEFAULT_CACHE_DIR,
    repo_root: Path = Path("."),
) -> dict[str, Any]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = cache_dir / CONSUMER_MANIFEST
    if manifest_path.exists():
        try:
            cached = _validate_consumer_manifest(manifest_path)
        except (OSError, ValueError, ConductSourceSnapshotError) as exc:
            raise ConductConsumerBootstrapError(
                f"existing Gate 4 Consumer.gov cache is invalid; refusing bootstrap overwrite: {exc}"
            ) from exc
        return {
            "action": "existing_cache_preserved",
            "manifest": str(manifest_path),
            "months": [str(item.get("month") or "") for item in cached["months"]],
            "fetched_at": str(cached["fetched_at"]),
            "bootstrap_provenance": cached.get("bootstrap_provenance"),
        }

    materialized, fetched_at, head, tracked_paths = _validate_versioned_materialization(
        repo_root=repo_root
    )
    cached = _cache_consumer_manifest(
        materialized,
        cache_dir=cache_dir,
        fetched_at=fetched_at,
    )
    cached["bootstrap_provenance"] = {
        "method": "versioned_repository_snapshot",
        "source_head_sha": head,
        "tracked_paths": tracked_paths,
        "policy": "bootstrap_only_when_gate4_cache_absent",
    }
    _write_json(manifest_path, cached)
    validated = _validate_consumer_manifest(manifest_path)
    return {
        "action": "versioned_repository_bootstrap",
        "manifest": str(manifest_path),
        "months": [str(item.get("month") or "") for item in validated["months"]],
        "fetched_at": str(validated["fetched_at"]),
        "bootstrap_provenance": validated.get("bootstrap_provenance"),
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Seed the Gate 4 Consumer.gov cache from the exact versioned core in Git HEAD. "
            "This is a fail-closed bootstrap path, not a current-source refresh."
        )
    )
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--repo-root", type=Path, default=Path("."))
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    result = bootstrap_consumer_cache(cache_dir=args.cache_dir, repo_root=args.repo_root)
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
