"""Verify A09.4 reused each official Estabelecimentos ZIP once per generation.

This is a read-only proof analyzer; it never downloads, publishes, or edits
production caches. Run after the manually dispatched, isolated A09 proof DAG.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

SOURCE_PREFIX = "V2_SOURCE_ACQUISITION "
TARGET_SOURCE_IDS = ("receita_cnpj_lifecycle", "receita_consumer_identity")


class A09ProofError(RuntimeError):
    """Missing or inconsistent proof: do not claim performance savings."""


def _load_a09(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        raise A09ProofError("A09 telemetry JSONL is missing")
    events = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        value = json.loads(line)
        if not isinstance(value, dict) or not isinstance(value.get("event"), str):
            raise A09ProofError("A09 telemetry contains a non-event record")
        events.append(value)
    if not events:
        raise A09ProofError("A09 telemetry contains no events")
    return events


def _read_source_ends(path: Path) -> dict[str, dict[str, Any]]:
    if not path.is_file():
        raise A09ProofError("DAG log is missing")
    found: dict[str, dict[str, Any]] = {}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if SOURCE_PREFIX not in line:
            continue
        try:
            record = json.loads(line.split(SOURCE_PREFIX, 1)[1])
        except json.JSONDecodeError as exc:
            raise A09ProofError("Malformed source acquisition log") from exc
        if record.get("event") == "end" and record.get("source_id") in TARGET_SOURCE_IDS:
            source = record["source_id"]
            if source in found:
                raise A09ProofError(f"Duplicate end record for {source}")
            found[source] = record
    for source in TARGET_SOURCE_IDS:
        item = found.get(source)
        if item is None:
            raise A09ProofError(f"Missing source acquisition completion: {source}")
        if (
            item.get("state") != "fresh"
            or item.get("freshness_method") != "current_fetch"
            or item.get("used_cache") is not False
            or item.get("current_error") is not None
        ):
            raise A09ProofError(f"Real rebuild not demonstrated for {source}")
    return found


def _partition_events(events: list[dict[str, Any]], event: str, state: str | None = None) -> list[dict[str, Any]]:
    return [
        item for item in events
        if item.get("event") == event
        and item.get("category") == "estabelecimentos"
        and (state is None or item.get("state") == state)
    ]


def _unique_indices(rows: list[dict[str, Any]], label: str) -> dict[int, dict[str, Any]]:
    by_index: dict[int, dict[str, Any]] = {}
    for record in rows:
        index = record.get("file_index")
        if type(index) is not int or index < 0 or index in by_index:
            raise A09ProofError(f"Invalid or repeated {label} partition index")
        by_index[index] = record
    return by_index


def verify_rebuild_proof(telemetry: Path, dag_log: Path) -> dict[str, Any]:
    events = _load_a09(telemetry)
    completed = _read_source_ends(dag_log)

    discoveries = [e for e in events if e.get("event") == "release_discovery"]
    if len(discoveries) != 1:
        raise A09ProofError("Expected exactly one identity publication discovery")
    release = discoveries[0]
    expected_count = release.get("establishment_partitions")
    if type(expected_count) is not int or expected_count <= 0 or expected_count > 100:
        raise A09ProofError("Missing or implausible official partition count")
    expected_indices = set(range(expected_count))

    downloads = _unique_indices(_partition_events(events, "zip_download"), "download")
    stored = _unique_indices(_partition_events(events, "zip_reuse", "stored"), "stored")
    verified = _unique_indices(_partition_events(events, "zip_reuse", "verified"), "verified")

    for label, rows in (("download", downloads), ("stored", stored), ("verified", verified)):
        if set(rows) != expected_indices:
            raise A09ProofError(
                f"{label} partitions do not match official catalog: "
                f"observed={sorted(rows)} expected={sorted(expected_indices)}"
            )

    invalid_reuses = [
        e for e in _partition_events(events, "zip_reuse")
        if e.get("state") not in {"stored", "verified"}
    ]
    if invalid_reuses:
        raise A09ProofError("At least one ZIP was not safely stored or reused")

    for index in sorted(expected_indices):
        download = downloads[index]
        first = stored[index]
        second = verified[index]
        size = download.get("received_bytes")
        if (
            download.get("state") != "success"
            or type(size) is not int
            or size <= 0
            or first.get("expected_bytes") != size
            or second.get("expected_bytes") != size
        ):
            raise A09ProofError(f"Partition {index} has mismatched size/integrity observations")

    if not any(e.get("event") == "identity_complete" for e in events):
        raise A09ProofError("Identity stage did not report completion")
    # Enforce observed ordering: first stage downloaded all official partitions;
    # second stage then verified their reuse, without any second HTTP transfer.
    first_verified_offset = next(
        i for i, e in enumerate(events)
        if e.get("event") == "zip_reuse" and e.get("category") == "estabelecimentos"
        and e.get("state") == "verified"
    )
    if any(
        e.get("event") == "zip_download" and e.get("category") == "estabelecimentos"
        for e in events[first_verified_offset:]
    ):
        raise A09ProofError("A repeated establishment HTTP download followed verified reuse")
    if any(
        e.get("event") == "zip_reuse" and e.get("category") == "estabelecimentos"
        and e.get("state") == "stored"
        for e in events[first_verified_offset:]
    ):
        raise A09ProofError("A new establishment was stored after identity reuse began")

    duplicate_bytes_avoided = sum(download["received_bytes"] for download in downloads.values())
    download_seconds = sum(float(e.get("elapsed_download_s") or 0) for e in downloads.values())
    observed_reuse_seconds = sum(float(e.get("elapsed_s") or 0) for e in verified.values())
    if not (download_seconds > 0 and observed_reuse_seconds >= 0):
        raise A09ProofError("Expected positive measured download timing")

    return {
        "schema": "a09-controlled-official-rebuild-proof-v1",
        "status": "verified",
        "scope": "isolated_full_gate4_rebuild_no_hostgator_publication",
        "reference_period": release["reference_period"],
        "official_metadata_fingerprint": release["release_fingerprint"],
        "establishment_partitions": expected_count,
        "official_establishment_gets": len(downloads),
        "reused_without_second_get": len(verified),
        "avoided_duplicate_download_bytes": duplicate_bytes_avoided,
        "initial_establishment_download_seconds": round(download_seconds, 3),
        "verified_reuse_seconds": round(observed_reuse_seconds, 3),
        "source_acquisition_seconds": {
            key: round(float(value["duration_seconds"]), 3)
            for key, value in sorted(completed.items())
        },
        "reference_comparison": (
            "Previous 2026-11-15 run re-downloaded 5.42 GB in 6614.152 seconds; "
            "not a controlled time-saving measurement."
        ),
        "note": (
            "The observed proof is one official GET per establishment partition "
            "in the same generation, followed by SHA-validated local reuse. "
            "Whole-generation runtime improvements depend on source and environment."
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--telemetry", type=Path, required=True)
    parser.add_argument("--dag-log", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = verify_rebuild_proof(args.telemetry, args.dag_log)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
