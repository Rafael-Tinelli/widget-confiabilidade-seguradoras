"""A09.6 one-shot isolated real-data proof (never publishes or writes repo data).

Execution on a disposable GitHub runner:
  preflight -- pin and verify exact November 2026 source and conduct caches.
  validate  -- prove real prefetch, A09.4 and 20 captured ZIPs after Gate 4.
  replay    -- reparse the *same* 20 ZIP bytes locally using the serial reader
               and compare them with the live prefetch output.

Only compact, CNPJ-free evidence goes to an artifact. The two full national
ZIP families remain solely in RUNNER_TEMP and expire when the job ends.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
from collections import Counter
from datetime import date
from pathlib import Path
from urllib.parse import urlsplit

from api.sources import receita_cnpj_bulk as bulk
from api.sources import receita_cnpj_identity as identity
from api.sources.receita_cnpj_a096_retention import (
    _company_root,
    local_input_manifest,
)
from api.sources.receita_cnpj_run_zip_cache import _cache_dir
from api.v2.build_consumer_gov_157_experiment import ELIGIBILITY_PATH
from api.v2.build_consumer_gov_receita_identity import (
    CONSUMER_IDENTITY_PATH,
    TARGET_UNIVERSE_HASH_METHOD,
    _cache_key,
    _load_inputs,
    target_universe_hash,
    unresolved_provider_hash,
)
from api.v2.refresh_receita_lifecycle import (
    refresh_receita_lifecycle,
    regulatory_target_universe_hash,
)
from scripts.a096_homologation import (
    _diff_paths,
    _hash_file,
    _json_bytes,
    _load_reference,
    audit,
    compare_local,
)

REFERENCE_PATH = Path("docs/evidence/a096-november-2026-reference.json")
CACHE_ROOT = Path("data/cache/v2")
DERIVED = Path("data/derived/v2")
PREFIX = "a096-live-proof-v1"
NOVEMBER_SOURCE = "receita_cnpj_lifecycle"
NOVEMBER_CONDUCT = "receita_consumer_identity"
EXPECTED = (NOVEMBER_SOURCE, NOVEMBER_CONDUCT)


def _write(path: Path, report: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
    )


def _runner_workspace() -> Path:
    path = os.environ.get("RUNNER_TEMP", "")
    if not path or not os.environ.get("V2_BUILD_ID"):
        raise RuntimeError("runner_scope_missing")
    return Path(path) / "a096-live-proof"


def _cache_file(source: str) -> Path:
    group = "source" if source == NOVEMBER_SOURCE else "conduct"
    return CACHE_ROOT / group / (source + ".json")


def _candidate_file(source: str) -> Path:
    return _runner_workspace() / "prefetch" / (source + ".json")


def _serial_file(source: str) -> Path:
    return _runner_workspace() / "serial" / (source + ".json")


def _lineage_row(source: str) -> dict:
    name = "source_lineage.json" if source == NOVEMBER_SOURCE else "conduct_source_lineage.json"
    payload = _json_bytes((DERIVED / name).read_bytes())
    rows = [
        record for record in payload.get("sources", [])
        if isinstance(record, dict) and record.get("source_id") == source
    ]
    if len(rows) != 1:
        raise RuntimeError("missing_current_run_lineage")
    return rows[0]


def preflight() -> dict:
    """Do not request official ZIPs unless both historical caches really exist."""
    ref = _load_reference(REFERENCE_PATH)
    check = audit(REFERENCE_PATH, CACHE_ROOT)
    if (
        check.get("source_checks") != {
            NOVEMBER_SOURCE: "verified", NOVEMBER_CONDUCT: "verified",
        }
        or check.get("reference_snapshots_verified") is not True
        or check.get("status") != "insufficient_evidence"
    ):
        raise RuntimeError("november_reference_cache_not_verified")

    # Save only the small filtered snapshots; the original historical ZIPs
    # were discarded in November and are NOT falsely marked as recovered.
    workspace = _runner_workspace()
    baseline_root = workspace / "historical-2026-11"
    baseline_root.mkdir(parents=True, exist_ok=True)
    for source in EXPECTED:
        actual = _cache_file(source)
        if _hash_file(actual) != ref["source_snapshots"][source]["sha256"]:
            raise RuntimeError("november_filtered_snapshot_sha_mismatch")
        shutil.copy2(actual, baseline_root / actual.name)
    return {
        "schema": PREFIX,
        "stage": "preflight",
        "status": "verified",
        "historical_run_id": ref["reference_run"]["id"],
        "reference_period": ref["reference_run"]["period"],
        "source_checks": check["source_checks"],
        "historic_zip_bytes_recovered": False,
        "production_publication_authorized": False,
    }


def _receita_events() -> list[dict]:
    path = Path(os.environ.get(
        "RECEITA_A09_TELEMETRY_PATH", "data/derived/v2/receita_a09_events.jsonl"
    ))
    if not path.is_file():
        raise RuntimeError("a09_telemetry_missing")
    return [_json_bytes(line.encode()) for line in path.read_text().splitlines() if line.strip()]


def _verify_source_rows() -> dict:
    evidence = {}
    current_build = os.environ["V2_BUILD_ID"]
    for source in EXPECTED:
        line = _lineage_row(source)
        cache_path = _cache_file(source)
        if not cache_path.is_file():
            raise RuntimeError("missing_current_receita_cache_" + source)
        if (
            line.get("build_id") != current_build
            or line.get("state") != "fresh"
            or line.get("freshness_method") != "current_fetch"
            or _hash_file(cache_path) != line.get("sha256")
        ):
            raise RuntimeError("source_not_fresh_or_cache_hash_mismatch")
        evidence[source] = {
            "status": "fresh_current_fetch",
            "sha256": line["sha256"],
        }
    return evidence


def _verify_partition_transfer_routes(rows: list[dict]) -> dict:
    """Require one proven transfer path per ZIP, allowing verified serial fallback."""
    ready = Counter(
        (x.get("category"), x.get("file_index"))
        for x in rows if x.get("event") == "prefetch_ready" and x.get("state") == "verified"
    )
    serial = Counter(
        (x.get("category"), x.get("file_index"))
        for x in rows if x.get("event") == "zip_download" and x.get("state") == "success"
    )
    fallback = Counter(
        (x.get("category"), x.get("file_index"))
        for x in rows if x.get("event") == "prefetch_fallback" and x.get("state") == "serial"
    )
    summary: dict[str, dict[str, int]] = {}
    for category in ("empresas", "estabelecimentos"):
        used = serial_fallback = 0
        for index in range(10):
            key = (category, index)
            p, d, f = ready[key], serial[key], fallback[key]
            if p == 1 and d == 0 and f == 0:
                used += 1
            elif d == 1 and (p == 0 or (p == 1 and f == 1)):
                serial_fallback += 1
            else:
                raise RuntimeError("ambiguous_or_missing_zip_transfer_" + category)
        if used == 0:
            raise RuntimeError("no_effective_prefetch_for_" + category)
        summary[category] = {
            "prefetch_used": used, "serial_fallback_verified": serial_fallback,
            "total": used + serial_fallback,
        }
    return summary


def validate() -> dict:
    """Require complete ZIP integrity and permit legitimate verified fallbacks."""
    if os.environ.get("RECEITA_A096_PREFETCH") != "1":
        raise RuntimeError("prefetch_not_explicitly_enabled")
    if os.environ.get("RECEITA_A096_RETAIN_ZIPS") != "1":
        raise RuntimeError("zip_retention_not_explicitly_enabled")

    snapshots = _verify_source_rows()
    evidence = local_input_manifest(
        source_root=CACHE_ROOT / "source",
        conduct_root=CACHE_ROOT / "conduct",
    )
    if evidence.get("zip_files_verified") != 20:
        raise RuntimeError("not_all_20_zip_files_validated")
    workspace = _runner_workspace()
    proof_path = workspace / "local-input-proof.json"
    _write(proof_path, evidence)
    for source in EXPECTED:
        destination = _candidate_file(source)
        # The original run failed here: the parent "prefetch" directory
        # was not created before shutil.copy2(). All national acquisitions
        # had already finished successfully.
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(_cache_file(source), destination)

    rows = _receita_events()
    transfers = _verify_partition_transfer_routes(rows)
    stored = Counter(
        x.get("file_index") for x in rows
        if x.get("event") == "zip_reuse" and x.get("category") == "estabelecimentos"
        and x.get("state") == "stored"
    )
    reused = Counter(
        x.get("file_index") for x in rows
        if x.get("event") == "zip_reuse" and x.get("category") == "estabelecimentos"
        and x.get("state") == "verified"
    )
    retained = Counter(
        x.get("file_index") for x in rows
        if x.get("event") == "company_capture" and x.get("category") == "empresas"
        and x.get("state") == "verified_hardlink"
    )
    expected = Counter({i: 1 for i in range(10)})
    if stored != expected or reused != expected or retained != expected:
        raise RuntimeError("a094_or_company_retention_not_proven_for_all_partitions")

    # The previous November baseline is for an EXTERNAL comparison, not
    # sufficient to claim same-input equivalence because raw hashes were lost.
    historic = {}
    for source in EXPECTED:
        earlier = _json_bytes(
            (_runner_workspace() / "historical-2026-11" / (source + ".json")).read_bytes()
        )
        current = _json_bytes(_candidate_file(source).read_bytes())
        historic[source] = {
            "period_unchanged": (
                (earlier.get("source") or {}).get("reference_period")
                == (current.get("source") or {}).get("reference_period")
            ),
            "release_fingerprint_unchanged": (
                (earlier.get("source") or {}).get("release_fingerprint")
                == (current.get("source") or {}).get("release_fingerprint")
            ),
            "diff_paths_only": _diff_paths(earlier, current, source),
        }

    return {
        "schema": PREFIX,
        "stage": "live_verified",
        "status": "verified",
        "build_id": os.environ["V2_BUILD_ID"],
        "reference_period": evidence["reference_period"],
        "zip_files_verified": 20,
        "source_lineage": snapshots,
        "transfer_paths": transfers,
        "prefetch_verified": {
            category: counts["prefetch_used"] for category, counts in transfers.items()
        },
        "a094": {"stored": 10, "verified": 10},
        "company_hardlinks_verified": 10,
        "historical_comparison_not_same_zip_bytes": historic,
        "local_input_proof_sha256": _hash_file(proof_path),
        "production_publication_authorized": False,
    }


def _small_official_or_local_downloader(original, family_root: Path, family: str):
    def get(session, url: str, destination: Path) -> Path:
        name = Path(urlsplit(url).path).name
        if name.startswith(("Empresas", "Estabelecimentos")):
            if not name.startswith(family):
                # A09.4 may already provide Estabelecimentos from local cache.
                # If it does not, retrieve them ONLY from the local run capture.
                fallback = _cache_dir() / name
            else:
                fallback = family_root / name
            if not fallback.is_file() or fallback.is_symlink():
                raise RuntimeError("missing_local_zip_for_serial_replay")
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.unlink(missing_ok=True)
            os.link(fallback, destination)
            return destination
        if name not in ("Motivos.zip", "Cnaes.zip", "Naturezas.zip"):
            raise RuntimeError("unexpected_remote_file_in_local_replay")
        # Only small official dictionaries; never re-download a national ZIP.
        return original(session, url, destination)
    return get


REPLAY_INPUT_LABELS = frozenset((
    "classification", "candidate_lifecycle", "candidate_identity",
    "local_zip_proof", "consumer_eligibility", "consumer_identity",
))


def _require_replay_input(label: str, path: Path) -> Path:
    """Fail with an allowlisted input label, not an absolute path or ZIP token."""
    if label not in REPLAY_INPUT_LABELS:
        raise ValueError("unsupported_replay_input_label")
    if not path.is_file() or path.is_symlink():
        raise RuntimeError("missing_replay_input_" + label)
    return path


def _original_observation_date(primary: dict) -> date:
    """Reproduce the original record observation date across UTC midnight.

    The official ZIP contents can remain stable while a replay launched after
    midnight would otherwise change records[].observed_at. This preserves the
    original data field rather than excluding it from semantic comparison.
    """
    records = primary.get("records")
    if not isinstance(records, list) or not records:
        raise RuntimeError("missing_primary_observation_records")
    if not all(isinstance(row, dict) for row in records):
        raise RuntimeError("invalid_primary_observation_records")
    days = {row.get("observed_at") for row in records}
    if len(days) != 1:
        raise RuntimeError("inconsistent_primary_observation_date")
    observed = next(iter(days))
    if not isinstance(observed, str) or not re.fullmatch(r"20\d{2}-\d{2}-\d{2}", observed):
        raise RuntimeError("invalid_primary_observation_date")
    try:
        return date.fromisoformat(observed)
    except ValueError as exc:
        raise RuntimeError("invalid_primary_observation_date") from exc


def replay() -> dict:
    """Replay exactly the retained large ZIPs via the existing serial parsers."""
    # Guard all small replay prerequisites BEFORE opening any large ZIP.
    _require_replay_input(
        "candidate_lifecycle", _candidate_file(NOVEMBER_SOURCE)
    )
    _require_replay_input(
        "candidate_identity", _candidate_file(NOVEMBER_CONDUCT)
    )
    _require_replay_input(
        "classification", DERIVED / "source/classification_inventory.json"
    )
    _require_replay_input("consumer_eligibility", ELIGIBILITY_PATH)
    _require_replay_input("consumer_identity", CONSUMER_IDENTITY_PATH)
    workspace = _runner_workspace()
    proof_path = _require_replay_input(
        "local_zip_proof", workspace / "local-input-proof.json"
    )
    info = _json_bytes(proof_path.read_bytes())
    companies = _company_root()
    establishments = _cache_dir()
    if companies is None or establishments is None:
        raise RuntimeError("transient_zip_workspace_missing")
    if info.get("zip_files_verified") != 20:
        raise RuntimeError("incomplete_local_zip_inputs")
    release = bulk.discover_latest_release()
    if release.period != info["reference_period"]:
        raise RuntimeError("release_changed_before_local_replay")

    # The Gate 4 source_snapshot stage explicitly outputs
    # data/derived/v2/source/classification_inventory.json (NOT the standalone
    # classification builder's entity_classification_inventory.json).
    # Check the population fingerprint against the live prefetch snapshot.
    classification = _json_bytes(
        _require_replay_input(
            "classification", DERIVED / "source/classification_inventory.json"
        ).read_bytes()
    )
    entities = list(classification.get("entities") or [])
    if not entities:
        raise RuntimeError("regulatory_universe_missing")
    primary = _json_bytes(_candidate_file(NOVEMBER_SOURCE).read_bytes())
    if (regulatory_target_universe_hash(entities)
            != (primary.get("meta") or {}).get("target_universe_hash")):
        raise RuntimeError("regulatory_universe_not_equivalent_to_primary")
    observation_date = _original_observation_date(primary)
    consumer_entities, providers = _load_inputs()
    if not consumer_entities or not providers:
        raise RuntimeError("consumer_identity_universe_missing")

    # Only this Python process changes the env. The finished Gate4 DAG
    # ran in other subprocesses with RECEITA_A096_PREFETCH=1.
    os.environ["RECEITA_A096_PREFETCH"] = "0"
    os.environ["RECEITA_A096_RETAIN_ZIPS"] = "0"
    original_bulk = bulk._download_zip
    original_identity = identity._download_zip
    original_today = bulk._today_utc
    bulk._today_utc = lambda: observation_date
    bulk._download_zip = _small_official_or_local_downloader(
        original_bulk, establishments, "Estabelecimentos"
    )
    identity._download_zip = _small_official_or_local_downloader(
        original_identity, companies, "Empresas"
    )
    try:
        # Same regulated target population, same official release metadata,
        # same A09.4 verified bytes; ordered sequential parser only.
        lifecycle = refresh_receita_lifecycle(
            output=_serial_file(NOVEMBER_SOURCE),
            release=release,
            entities=entities,
        )
        # Use exactly the live Consumer.gov input population/provider names.
        reconstructed = identity.build_filtered_identity_snapshot(
            consumer_entities, providers, release=release,
        )
        reconstructed["meta"]["gate4_cache_key"] = _cache_key(
            reference_period=release.period,
            universe_hash=target_universe_hash(consumer_entities),
            provider_hash=unresolved_provider_hash(providers),
        )
        reconstructed["meta"]["target_universe_hash_method"] = (
            TARGET_UNIVERSE_HASH_METHOD
        )
        _write(_serial_file(NOVEMBER_CONDUCT), reconstructed)
    finally:
        bulk._download_zip = original_bulk
        identity._download_zip = original_identity
        bulk._today_utc = original_today

    if not lifecycle.get("records"):
        raise RuntimeError("local_serial_lifecycle_empty")

    # compare_local's "serial_*" args refer to the sealed primary snapshot
    # from the local-input proof (the LIVE prefetch run). The comparison is
    # symmetric; the independent reconstruction occupies candidate_*.
    comparison = compare_local(
        proof_path,
        companies=companies,
        establishments=establishments,
        serial_lifecycle=_candidate_file(NOVEMBER_SOURCE),
        serial_identity=_candidate_file(NOVEMBER_CONDUCT),
        candidate_lifecycle=_serial_file(NOVEMBER_SOURCE),
        candidate_identity=_serial_file(NOVEMBER_CONDUCT),
    )
    return {
        "schema": PREFIX,
        "stage": "same_input_serial_replay",
        "status": comparison["status"],
        "reason": comparison["reason"],
        "zip_files_checked": comparison["zip_files_checked"],
        "comparison_roles": {
            "sealed_primary": "real_A096_prefetch",
            "independent_replay": "original_ordered_serial_reader",
        },
        "semantic_checks": comparison["semantic_checks"],
        "production_publication_authorized": False,
    }


def _safe_diagnostics(stage: str) -> dict:
    """Structural proof hints only: no CNPJs, URLs, ZIP bytes or source payloads."""
    observations: dict = {
        "cache_present": {source: _cache_file(source).is_file()
                          for source in EXPECTED},
        "lineage_present": {
            key: (DERIVED / file).is_file() for key, file in (
                ("lifecycle", "source_lineage.json"),
                ("identity", "conduct_source_lineage.json"),
            )
        },
        "snapshot_candidate_present": {source: _candidate_file(source).is_file()
                                       for source in EXPECTED},
        "staged_company_receipts": 0,
        "staged_establishment_receipts": 0,
        "a09_telemetry_available": False,
        "prefetch_ready_count": {"empresas": 0, "estabelecimentos": 0},
        "a094_stored_count": 0,
        "a094_verified_count": 0,
        "company_retained_count": 0,
        "serial_zip_download_success_count": {"empresas": 0, "estabelecimentos": 0},
        "prefetch_fallback_count": {"empresas": 0, "estabelecimentos": 0},
        "prefetch_disk_guard_skip_count": {"empresas": 0, "estabelecimentos": 0},
    }
    company = _company_root()
    establishment = _cache_dir()
    if company is not None:
        observations["staged_company_receipts"] = sum(
            1 for i in range(10) if (company / f"Empresas{i}.zip.json").is_file()
        )
    if establishment is not None:
        observations["staged_establishment_receipts"] = sum(
            1 for i in range(10)
            if (establishment / f"Estabelecimentos{i}.zip.json").is_file()
        )
    try:
        rows = _receita_events()
    except (OSError, ValueError, TypeError, RuntimeError):
        rows = []
    else:
        observations["a09_telemetry_available"] = True
    for row in rows:
        event, category, state = (
            row.get("event"), row.get("category"), row.get("state")
        )
        if event == "prefetch_ready" and state == "verified":
            if category in observations["prefetch_ready_count"]:
                observations["prefetch_ready_count"][category] += 1
        elif event == "zip_reuse" and category == "estabelecimentos":
            if state == "stored":
                observations["a094_stored_count"] += 1
            elif state == "verified":
                observations["a094_verified_count"] += 1
        elif (event == "company_capture" and category == "empresas"
              and state == "verified_hardlink"):
            observations["company_retained_count"] += 1
        elif (event == "zip_download" and state == "success"
              and category in observations["serial_zip_download_success_count"]):
            observations["serial_zip_download_success_count"][category] += 1
        elif (event == "prefetch_fallback"
              and category in observations["prefetch_fallback_count"]):
            observations["prefetch_fallback_count"][category] += 1
        elif (event == "prefetch_skipped"
              and category in observations["prefetch_disk_guard_skip_count"]):
            observations["prefetch_disk_guard_skip_count"][category] += 1
    observations["inspection_stage"] = stage
    return observations


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=("preflight", "validate", "replay"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        stage = globals()[args.stage]
        report = stage()
    except Exception as exc:  # noqa: BLE001 - proof failures are fail-closed and redacted
        # Only fixed-format failure identifiers may be reported: never
        # disclose arbitrary errors containing CNPJs, URLs or file paths.
        safe_code = str(exc)
        if (isinstance(exc, RuntimeError)
                and re.fullmatch(r"[a-z][a-z_]{5,100}", safe_code)):
            reason = safe_code
        else:
            reason = type(exc).__name__
        report = {
            "schema": PREFIX,
            "stage": args.stage,
            "status": "insufficient_evidence",
            "reason": reason,
            "production_publication_authorized": False,
        }
    # Always preserve safe, granular evidence even on a failed stage.
    try:
        report["diagnostics"] = _safe_diagnostics(args.stage)
    except (OSError, ValueError, TypeError, RuntimeError):
        report["diagnostics"] = {"status": "diagnostics_unavailable"}
    _write(args.output, report)
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    if report["status"] not in ("verified", "equivalent"):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
