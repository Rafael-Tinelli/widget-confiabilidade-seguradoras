"""Restore an authenticated, complete observational manifest from GitHub Actions.

This is not a cache for ZIP/CSV data and never changes productive paths.
Succeeded runs are searched newest-first. Inaccessible or invalid artifacts are
skipped but recorded as a history gap; no recoverable history fails closed.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import subprocess
import tempfile
import time
import zipfile
from pathlib import Path

WORKFLOW = "v2-finalizadas-observational.yml"
SCHEMA = "finalizadas-observational-1"
STATE_SCHEMA = "finalizadas-observational-state-1"
MONTH = re.compile(r"20\d{2}-(?:0[1-9]|1[0-2])\Z")
SHA = re.compile(r"[0-9a-f]{64}\Z")
REPO = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+\Z")
ALLOWED_STATUS = {"validated_experimental", "requires_manual_review"}
A05_PARSER_CONTRACT = "finalizadas-parser-2026-11-14-v2-a05"


class HistoryError(RuntimeError):
    """No trustworthy, complete revision reference can be restored."""


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _json_file(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise HistoryError("Evidence is not a JSON object")
    return value


def validate_manifest(path: Path) -> dict:
    manifest = _json_file(path)
    months = manifest.get("months")
    if (manifest.get("schema") != SCHEMA
            or manifest.get("status") != "experimental_no_publication"
            or not isinstance(months, dict) or not months
            or not isinstance(manifest.get("revisions"), list)
            or not isinstance(manifest.get("quality_alerts"), list)):
        raise HistoryError("Incomplete observational manifest")
    for month, state in months.items():
        if not isinstance(month, str) or not MONTH.fullmatch(month):
            raise HistoryError("Invalid manifest month")
        if not isinstance(state, dict) or state.get("status") not in ALLOWED_STATUS:
            raise HistoryError(f"Invalid manifest state: {month}")
        source_hash = state.get("zip_sha256")
        if source_hash is None and (state.get("catalog_status") in
                                    ("withdrawn", "absent_from_feed")):
            if state["status"] != "requires_manual_review":
                raise HistoryError("Catalog withdrawal without alert")
        elif not isinstance(source_hash, str) or not SHA.fullmatch(source_hash):
            raise HistoryError(f"Missing/invalid source SHA: {month}")
        if (state["status"] == "requires_manual_review"
                and month not in manifest["revisions"]
                and month not in manifest["quality_alerts"]
                and state.get("history_gap") is not True):
            raise HistoryError(f"Unrecorded manual review: {month}")
        # Historic v1 evidence remains recoverable. The new A05 parser
        # contract cannot silently omit or upgrade its completeness status.
        if state.get("parser_version") == A05_PARSER_CONTRACT:
            population = state.get("population_completeness")
            if (not isinstance(population, dict)
                    or population.get("status") != "not_demonstrated"
                    or population.get("official_monthly_rows") is not None
                    or not isinstance(population.get("warnings"), list)
                    or any(not isinstance(item, str)
                           for item in population["warnings"])):
                raise HistoryError(f"A05 completeness claim invalid: {month}")
            if population["warnings"] and (
                state["status"] != "requires_manual_review"
                or month not in manifest["quality_alerts"]
            ):
                raise HistoryError(f"Unrecorded A05 completeness alert: {month}")
    for field in ("revisions", "quality_alerts"):
        values = manifest[field]
        if any(not isinstance(x, str) or x not in months for x in values):
            raise HistoryError(f"Invalid {field} reference")
    return manifest


def seal(manifest_path: Path, target: Path, run_id: int, attempt: int,
         head_sha: str) -> None:
    if run_id < 1 or attempt < 1 or not re.fullmatch(r"[0-9a-f]{40}", head_sha):
        raise HistoryError("Invalid run identity")
    validate_manifest(manifest_path)
    target.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(manifest_path, target / "manifest.json")
    identity = {
        "schema": STATE_SCHEMA, "run_id": run_id,
        "run_attempt": attempt, "head_sha": head_sha,
        "manifest_sha256": digest(target / "manifest.json"),
    }
    (target / "origin.json").write_text(
        json.dumps(identity, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )


def validate_artifact(folder: Path, run: dict, *, state_only: bool) -> dict:
    manifest_path = folder / "manifest.json"
    if not manifest_path.is_file() or manifest_path.is_symlink():
        raise HistoryError("Missing manifest")
    manifest = validate_manifest(manifest_path)
    if state_only:
        origin_path = folder / "origin.json"
        if not origin_path.is_file() or origin_path.is_symlink():
            raise HistoryError("Missing state provenance")
        origin = _json_file(origin_path)
        if origin != {
            "schema": STATE_SCHEMA,
            "run_id": run["id"], "run_attempt": run["run_attempt"],
            "head_sha": run["head_sha"],
            "manifest_sha256": digest(manifest_path),
        }:
            raise HistoryError("Wrong run or invalid manifest digest")
    else:
        # Historical full artifacts do not have a sealed compact manifest.
        # Verify the actual source ZIP and both JSON derivatives for each month.
        for month, evidence in manifest["months"].items():
            folder_month = folder / month
            archive = folder_month / f"finalizadas_{month}.zip"
            validation = folder_month / "validacao.json"
            aggregate = folder_month / "agregado-experimental.json"
            if any(not p.is_file() or p.is_symlink()
                   for p in (archive, validation, aggregate)):
                raise HistoryError(f"Incomplete legacy bundle: {month}")
            if digest(archive) != evidence["zip_sha256"]:
                raise HistoryError(f"Legacy source hash mismatch: {month}")
            checks = _json_file(validation)
            agg = _json_file(aggregate)
            metadata = agg.get("meta") if isinstance(agg.get("meta"), dict) else {}
            if (checks.get("zip_sha256") != evidence["zip_sha256"]
                    or checks.get("month") != month
                    or metadata.get("source_family") != "finalizadas"
                    or metadata.get("month") != month
                    or metadata.get("source_zip_sha256") != evidence["zip_sha256"]):
                raise HistoryError(f"Legacy provenance mismatch: {month}")
            with zipfile.ZipFile(archive) as z:
                members = z.namelist()
                if members != [f"finalizadas_{month}.csv"] or z.testzip() is not None:
                    raise HistoryError(f"Invalid legacy ZIP: {month}")
                with z.open(members[0]) as source:
                    h = hashlib.sha256()
                    for chunk in iter(lambda: source.read(1024 * 1024), b""):
                        h.update(chunk)
                    if h.hexdigest() != evidence.get("csv_sha256"):
                        raise HistoryError(f"Legacy CSV hash mismatch: {month}")
    return manifest


def _gh_json(endpoint: str) -> dict:
    result = subprocess.run(
        ["gh", "api", endpoint], check=True, capture_output=True,
        text=True, timeout=35,
    )
    payload = json.loads(result.stdout)
    if not isinstance(payload, dict):
        raise HistoryError("GitHub API did not return an object")
    return payload


def _gh_download(run_id: int, name: str, repo: str, target: Path) -> None:
    subprocess.run(
        ["gh", "run", "download", str(run_id), "--repo", repo,
         "--name", name, "--dir", str(target)],
        check=True, capture_output=True, text=True, timeout=150,
    )


def restore(repository: str, output: Path) -> dict:
    if not REPO.fullmatch(repository):
        raise HistoryError("Invalid repository identifier")
    endpoint = (
        f"repos/{repository}/actions/workflows/{WORKFLOW}/runs"
        "?branch=main&status=success&per_page=100"
    )
    runs = _gh_json(endpoint).get("workflow_runs")
    if not isinstance(runs, list):
        raise HistoryError("Invalid workflow-run listing")
    # Bounded search: do not spend the entire observational job recovering
    # artifacts, even when GitHub is slow or many historical runs exist.
    deadline = time.monotonic() + 240
    failures: list[int] = []
    output.mkdir(parents=True, exist_ok=True)
    for run in runs:
        if time.monotonic() >= deadline:
            raise HistoryError("History recovery time budget exhausted")
        if (not isinstance(run, dict) or run.get("conclusion") != "success"
                or run.get("status") != "completed"
                or run.get("head_branch") != "main"):
            continue
        run_id = run.get("id")
        attempt = run.get("run_attempt")
        head_sha = run.get("head_sha")
        if (type(run_id) is not int or type(attempt) is not int
                or not isinstance(head_sha, str)
                or not re.fullmatch(r"[0-9a-f]{40}", head_sha)):
            raise HistoryError("Run has invalid provenance")
        payload = _gh_json(
            f"repos/{repository}/actions/runs/{run_id}/artifacts?per_page=100"
        )
        artifacts = payload.get("artifacts")
        if not isinstance(artifacts, list):
            raise HistoryError("Invalid run artifact listing")
        expected = (
            (f"finalizadas-observational-state-{run_id}-a{attempt}", True),
            (f"finalizadas-observational-{run_id}-a{attempt}", False),
        )
        for name, state_only in expected:
            if time.monotonic() >= deadline:
                raise HistoryError("History recovery time budget exhausted")
            max_bytes = 2 * 1024 * 1024 if state_only else 150 * 1024 * 1024
            matches = [
                a for a in artifacts if isinstance(a, dict)
                and a.get("name") == name and a.get("expired") is False
                and type(a.get("size_in_bytes")) is int
                and 0 < a["size_in_bytes"] <= max_bytes
            ]
            if len(matches) != 1:
                continue
            with tempfile.TemporaryDirectory(prefix="finalizadas-restore-") as tmp:
                folder = Path(tmp)
                try:
                    _gh_download(run_id, name, repository, folder)
                    validate_artifact(folder, run, state_only=state_only)
                except (OSError, ValueError, HistoryError,
                        subprocess.CalledProcessError, subprocess.TimeoutExpired,
                        zipfile.BadZipFile, json.JSONDecodeError) as exc:
                    print(f"REJECTED_HISTORY run={run_id} artifact={name}: {exc}")
                    continue
                shutil.copyfile(folder / "manifest.json", output / "manifest.json")
                report = {
                    "status": "restored_with_gap" if failures else "restored",
                    "source_run_id": run_id, "source_attempt": attempt,
                    "source_artifact": name, "skipped_successful_runs": failures,
                    "manifest_sha256": digest(output / "manifest.json"),
                }
                (output / "recovery.json").write_text(
                    json.dumps(report, sort_keys=True, indent=2) + "\n",
                    encoding="utf-8",
                )
                return report
        failures.append(run_id)
    if failures:
        raise HistoryError(
            f"No intact baseline among {len(failures)} successful runs; "
            "do not silently reset revision history"
        )
    report = {"status": "bootstrap", "skipped_successful_runs": []}
    (output / "recovery.json").write_text(
        json.dumps(report, sort_keys=True, indent=2) + "\n", encoding="utf-8"
    )
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    r = commands.add_parser("restore")
    r.add_argument("--repository", required=True)
    r.add_argument("--output-dir", type=Path, required=True)
    s = commands.add_parser("seal")
    s.add_argument("--manifest", type=Path, required=True)
    s.add_argument("--output-dir", type=Path, required=True)
    s.add_argument("--run-id", type=int, required=True)
    s.add_argument("--attempt", type=int, required=True)
    s.add_argument("--head-sha", required=True)
    args = parser.parse_args()
    if args.command == "restore":
        print(json.dumps(restore(args.repository, args.output_dir)))
    else:
        seal(args.manifest, args.output_dir, args.run_id,
             args.attempt, args.head_sha)
        print("SEALED_OBSERVATIONAL_STATE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
