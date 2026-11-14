"""Optional, hash-verified reuse of the *previous successful run's* full artifact.

Never asserts that remote bytes are unchanged. The observational runner still
reads the live official JSON catalog, and falls back to direct official ZIP
downloads when reuse is absent, expired, inconsistent, or unverifiable.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

from api.probes import finalizadas_history as history
from api.probes import finalizadas_observational as obs

REUSE_MAX_AGE_SECONDS = 7 * 24 * 3600
MAX_FULL_ARTIFACT_BYTES = 150 * 1024 * 1024


def _fresh_candidate(month: str, state: dict, now: datetime) -> bool:
    if (state.get("parser_version") != obs.PARSER_CONTRACT_VERSION
            or state.get("revision") or state.get("history_gap")
            or state.get("catalog_status") != "listed"
            or not isinstance(state.get("source_fetched_at"), str)
            or not all(isinstance(state.get(k), str)
                       and len(state[k]) == 64
                       for k in ("zip_sha256", "aggregate_json_sha256",
                                 "validation_json_sha256"))):
        return False
    try:
        acquired = datetime.fromisoformat(
            state["source_fetched_at"].replace("Z", "+00:00")
        )
        if acquired.tzinfo is None:
            return False
        age = (now - acquired.astimezone(timezone.utc)).total_seconds()
        return 0 <= age <= REUSE_MAX_AGE_SECONDS
    except ValueError:
        return False


def _verified_bundle(folder: Path, month: str, state: dict) -> bool:
    month_dir = folder / month
    if not month_dir.is_dir() or month_dir.is_symlink():
        return False
    archive = month_dir / f"finalizadas_{month}.zip"
    aggregate = month_dir / "agregado-experimental.json"
    validation = month_dir / "validacao.json"
    if any(not path.is_file() or path.is_symlink()
           for path in (archive, aggregate, validation)):
        return False
    if (archive.stat().st_size != state.get("zip_bytes")
            or archive.stat().st_size > obs.MAX_ZIP_BYTES
            or history.digest(archive) != state["zip_sha256"]
            or history.digest(aggregate) != state["aggregate_json_sha256"]
            or history.digest(validation) != state["validation_json_sha256"]):
        return False
    try:
        agg = json.loads(aggregate.read_text(encoding="utf-8"))
        val = json.loads(validation.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    meta = agg.get("meta") if isinstance(agg, dict) else None
    return bool(
        isinstance(meta, dict) and isinstance(val, dict)
        and meta.get("source_family") == "finalizadas"
        and meta.get("reference_event") == "finalizacao"
        and meta.get("publication_status") == "isolated_not_for_publication"
        and meta.get("month") == month
        and meta.get("source_zip_sha256") == state["zip_sha256"]
        and meta.get("parser_version") == obs.PARSER_CONTRACT_VERSION
        and val.get("month") == month
        and val.get("zip_sha256") == state["zip_sha256"]
        and val.get("parser_version") == obs.PARSER_CONTRACT_VERSION
    )


def prepare_cache(repository: str, recovery: Path, manifest_path: Path,
                  output_dir: Path, *, now: datetime | None = None) -> list[str]:
    """Return only authenticated, verified local candidates; empty means fetch."""
    if not history.REPO.fullmatch(repository) or not recovery.is_file():
        return []
    report = json.loads(recovery.read_text(encoding="utf-8"))
    if report.get("status") != "restored" or not manifest_path.is_file():
        return []
    manifest = history.validate_manifest(manifest_path)
    if history.digest(manifest_path) != report.get("manifest_sha256"):
        raise history.HistoryError("Reuse recovery manifest digest mismatch")
    run_id = report.get("source_run_id")
    attempt = report.get("source_attempt")
    if type(run_id) is not int or type(attempt) is not int or run_id <= 0 or attempt <= 0:
        raise history.HistoryError("Reuse source run identity invalid")
    now = now or datetime.now(timezone.utc)
    selected = [
        month for month, state in manifest["months"].items()
        if _fresh_candidate(month, state, now)
    ]
    if not selected:
        return []

    output_dir = obs._safe_output(output_dir)
    if output_dir.exists() or output_dir.is_symlink():
        raise history.HistoryError("Reuse output already exists")

    full_name = f"finalizadas-observational-{run_id}-a{attempt}"
    artifacts = history._gh_json(
        f"repos/{repository}/actions/runs/{run_id}/artifacts?per_page=100"
    ).get("artifacts")
    if not isinstance(artifacts, list):
        raise history.HistoryError("Invalid full artifact listing")
    matches = [
        item for item in artifacts
        if isinstance(item, dict) and item.get("name") == full_name
        and item.get("expired") is False
        and type(item.get("size_in_bytes")) is int
        and 0 < item["size_in_bytes"] <= MAX_FULL_ARTIFACT_BYTES
    ]
    if len(matches) != 1:
        return []

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix=".finalizadas-reuse-check-", dir=output_dir.parent
    ) as tmp:
        root = Path(tmp)
        downloaded = root / "downloaded"
        downloaded.mkdir()
        history._gh_download(run_id, full_name, repository, downloaded)
        full_manifest = downloaded / "manifest.json"
        if (not full_manifest.is_file() or full_manifest.is_symlink()
                or history.digest(full_manifest) != history.digest(manifest_path)):
            raise history.HistoryError("Full artifact manifest differs from sealed history")
        verified = root / "verified"
        verified.mkdir()
        valid: list[str] = []
        for month in selected:
            if not _verified_bundle(downloaded, month, manifest["months"][month]):
                continue
            target = verified / month
            target.mkdir()
            for filename in (f"finalizadas_{month}.zip",
                             "agregado-experimental.json", "validacao.json"):
                shutil.copyfile(downloaded / month / filename, target / filename)
            valid.append(month)
        if valid:
            if output_dir.exists() or output_dir.is_symlink():
                raise history.HistoryError("Reuse output appeared before promotion")
            os.replace(verified, output_dir)
        return valid


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--recovery-report", type=Path, required=True)
    parser.add_argument("--previous-manifest", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    try:
        months = prepare_cache(
            args.repository, args.recovery_report, args.previous_manifest,
            args.output_dir,
        )
    except (OSError, ValueError, history.HistoryError, shutil.Error,
            subprocess.CalledProcessError, subprocess.TimeoutExpired,
            zipfile.BadZipFile) as exc:
        # The cache is an optimization, never an alternative truth source.
        print(f"REUSE_UNAVAILABLE: {type(exc).__name__}: {exc}")
        months = []
    print(json.dumps({"reuse_verified_months": months,
                      "state": "local_verified_only" if months else "no_reuse"}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
