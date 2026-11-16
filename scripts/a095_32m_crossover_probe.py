"""A09.5 final isolated crossover: two ordinary GETs, 32-MiB prefixes.

Measure sequential -> parallel then parallel -> sequential on identical
official ZIP prefixes without changing production acquisition or outputs.
Check the official November 2026 metadata catalog for ALL required families.
Never download a complete ZIP, persist source data, or publish to HostGator.
"""
from __future__ import annotations

import argparse
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import requests

from api.sources.receita_cnpj_bulk import (
    _configure_official_session,
    _propfind,
    discover_latest_release,
)
from api.sources.receita_cnpj_cache_integrity import (
    checked_release_fingerprint,
    lifecycle_release_fingerprint,
)
from scripts.a095_bounded_get_probe import GetProbeError, _get_prefix

PREFIX_BYTES = 32 * 1024 * 1024
PARTITIONS = 10
MAX_CONCURRENT = 2
TOTAL_GETS = 8
MIN_GAIN = 1.15
MAX_INDIVIDUAL_SLOWDOWN = 2.0
COMPANY_FILE = re.compile(r"Empresas(\d+)\.zip\Z")
ESTABLISHMENT_FILE = re.compile(r"Estabelecimentos(\d+)\.zip\Z")
REFERENCE_PERIOD = "2026-11"
DIAGNOSTIC_SCHEMA = "a095-32m-crossover-metadata-proof-v1"


def _official_catalog() -> dict[str, Any]:
    """PROPFIND-only catalog check; cannot verify bytes of every full ZIP."""
    release = discover_latest_release()
    if release.period != REFERENCE_PERIOD:
        raise GetProbeError("newest_official_period_is_not_2026_11")
    lifecycle_fp = lifecycle_release_fingerprint(release)
    if not lifecycle_fp:
        raise GetProbeError("lifecycle_fingerprint_unavailable")
    with requests.Session() as session:
        _configure_official_session(session)
        catalog = [
            r for r in _propfind(session, release.release_url)
            if not r.get("is_collection")
        ]

    by_name: dict[str, dict[str, Any]] = {}
    for item in catalog:
        name = item.get("name")
        if not isinstance(name, str) or name in by_name:
            raise GetProbeError("duplicate_or_invalid_catalog_name")
        by_name[name] = item

    companies = sorted(
        (name for name in by_name if COMPANY_FILE.fullmatch(name)),
        key=lambda name: int(COMPANY_FILE.fullmatch(name).group(1)),
    )
    establishments = sorted(
        (name for name in by_name if ESTABLISHMENT_FILE.fullmatch(name)),
        key=lambda name: int(ESTABLISHMENT_FILE.fullmatch(name).group(1)),
    )
    expected_indices = list(range(PARTITIONS))
    if (
        [int(COMPANY_FILE.fullmatch(name).group(1)) for name in companies] != expected_indices
        or [int(ESTABLISHMENT_FILE.fullmatch(name).group(1)) for name in establishments]
        != expected_indices
        or tuple(establishments) != tuple(release.establishment_files)
    ):
        raise GetProbeError("incomplete_company_or_establishment_partitions")

    required = [*companies, *establishments, "Motivos.zip", "Cnaes.zip", "Naturezas.zip"]
    if any(name not in by_name for name in required):
        raise GetProbeError("official_required_dictionaries_missing")
    manifest = [
        {
            "name": name,
            "size": by_name[name].get("size"),
            "etag": by_name[name].get("etag"),
            "last_modified": by_name[name].get("last_modified"),
        }
        for name in required
    ]
    full_fingerprint = checked_release_fingerprint(release.period, manifest, required)
    if not full_fingerprint:
        raise GetProbeError("official_catalog_metadata_incomplete_or_unreliable")

    lifecyle_names = ["Motivos.zip", *establishments]
    local_lifecycle = checked_release_fingerprint(
        release.period, [next(x for x in manifest if x["name"] == n)
                         for n in lifecyle_names], lifecyle_names,
    )
    if local_lifecycle != lifecycle_fp:
        raise GetProbeError("lifecycle_catalog_changed_during_discovery")

    # A snapshot of trusted metadata only; no full ZIP content is inspected.
    return {
        "reference_period": release.period,
        "release_url": release.release_url,
        "fingerprint": full_fingerprint,
        "lifecycle_fingerprint": lifecycle_fp,
        "company_partitions": len(companies),
        "establishment_partitions": len(establishments),
        "required_files": len(required),
        "required_aggregate_bytes": sum(item["size"] for item in manifest),
        "full_zip_content_verified": False,
        "status": "complete_metadata_only",
        "resources": [
            {
                "url": release.release_url + name,
                "file_index": idx,
                "size": by_name[name]["size"],
                "etag": by_name[name].get("etag"),
            }
            for idx, name in enumerate(establishments[:MAX_CONCURRENT])
        ],
    }


def _measure(mode: str, resources: list[dict[str, Any]]) -> dict[str, Any]:
    started = time.monotonic()
    if mode == "sequential":
        rows = [_get_prefix(resource, read_limit=PREFIX_BYTES) for resource in resources]
    elif mode == "parallel":
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as pool:
            futures = [
                pool.submit(_get_prefix, resource, read_limit=PREFIX_BYTES)
                for resource in resources
            ]
            rows = [future.result() for future in futures]
    else:
        raise GetProbeError("unknown_measurement_mode")
    wall = time.monotonic() - started
    if not 0 < wall:
        raise GetProbeError("invalid_mode_elapsed_seconds")
    if len(rows) != MAX_CONCURRENT:
        raise GetProbeError("incomplete_mode_measurement")
    return {
        "mode": mode,
        "wall_seconds": round(wall, 3),
        "aggregate_mib_per_second": round((MAX_CONCURRENT * PREFIX_BYTES / 1048576) / wall, 4),
        "files": rows,
    }


def _validate_bytes(
    rounds: list[dict[str, Any]], resources: list[dict[str, Any]],
) -> None:
    for position, resource in enumerate(resources):
        seen_digests: set[str] = set()
        seen_etags: set[str] = set()
        for round_ in rounds:
            for item in round_["measurements"]:
                file_ = item["files"][position]
                if (
                    file_.get("file_index") != resource["file_index"]
                    or file_.get("read_bytes") != PREFIX_BYTES
                    or file_.get("http_status") != 200
                ):
                    raise GetProbeError("unexpected_bounded_get_result")
                digest = file_.get("sha256_prefix")
                if not isinstance(digest, str) or not re.fullmatch(r"[a-f0-9]{64}", digest):
                    raise GetProbeError("prefix_sha256_missing")
                seen_digests.add(digest)
                etag = file_.get("etag")
                if isinstance(etag, str) and etag:
                    seen_etags.add(etag)
        if len(seen_digests) != 1 or len(seen_etags) > 1:
            raise GetProbeError("official_file_content_or_etag_changed")
        if (resource.get("etag") and seen_etags
                and resource["etag"] not in seen_etags):
            raise GetProbeError("official_catalog_etag_differs_from_response")


def _decision(rounds: list[dict[str, Any]]) -> dict[str, Any]:
    ratios: list[float] = []
    peak_slowdowns: list[float] = []
    for round_ in rounds:
        by_mode = {row["mode"]: row for row in round_["measurements"]}
        serial = by_mode["sequential"]
        parallel = by_mode["parallel"]
        if parallel["wall_seconds"] <= 0:
            raise GetProbeError("invalid_parallel_time")
        ratios.append(serial["wall_seconds"] / parallel["wall_seconds"])
        slowdowns = [
            parallel["files"][i]["elapsed_seconds"] / serial["files"][i]["elapsed_seconds"]
            for i in range(MAX_CONCURRENT)
        ]
        if any(x <= 0 for x in slowdowns):
            raise GetProbeError("invalid_individual_time")
        peak_slowdowns.append(max(slowdowns))
    consistent = (
        all(ratio >= MIN_GAIN for ratio in ratios)
        and all(slowdown <= MAX_INDIVIDUAL_SLOWDOWN for slowdown in peak_slowdowns)
    )
    return {
        "classification": (
            "promising_for_two_zip_prefetch_design"
            if consistent else "mixed_or_unfavorable_no_production_change"
        ),
        "both_orders_gain_at_least_15_percent": all(x >= MIN_GAIN for x in ratios),
        "no_individual_file_slowdown_above_2x": all(
            x <= MAX_INDIVIDUAL_SLOWDOWN for x in peak_slowdowns
        ),
        "speedups": [round(x, 3) for x in ratios],
        "max_individual_slowdown_per_round": [round(x, 3) for x in peak_slowdowns],
        "gain_criterion": MIN_GAIN,
        "individual_degradation_ceiling": MAX_INDIVIDUAL_SLOWDOWN,
    }


def run_crossover() -> dict[str, Any]:
    initial = _official_catalog()
    resources = initial["resources"]
    if len(resources) != MAX_CONCURRENT:
        raise GetProbeError("missing_probe_partitions")

    rounds: list[dict[str, Any]] = []
    for order in (("sequential", "parallel"), ("parallel", "sequential")):
        rows = [_measure(mode, resources) for mode in order]
        rounds.append({"order": list(order), "measurements": rows})

    _validate_bytes(rounds, resources)
    final = _official_catalog()
    if (
        initial["reference_period"] != final["reference_period"]
        or initial["release_url"] != final["release_url"]
        or initial["fingerprint"] != final["fingerprint"]
        or initial["lifecycle_fingerprint"] != final["lifecycle_fingerprint"]
    ):
        raise GetProbeError("official_catalog_changed_during_crossover")
    decision = _decision(rounds)
    return {
        "schema": DIAGNOSTIC_SCHEMA,
        "status": "measured",
        "official_catalog": {
            k: initial[k] for k in (
                "reference_period", "status", "fingerprint", "lifecycle_fingerprint",
                "company_partitions", "establishment_partitions", "required_files",
                "required_aggregate_bytes", "full_zip_content_verified",
            )
        },
        "file_indices": [x["file_index"] for x in resources],
        "experiment_orders": [r["order"] for r in rounds],
        "prefix_mib_per_get": PREFIX_BYTES // 1048576,
        "total_requests": TOTAL_GETS,
        "max_parallel_connections": MAX_CONCURRENT,
        "max_application_bytes": TOTAL_GETS * PREFIX_BYTES,
        "observed_application_bytes": TOTAL_GETS * PREFIX_BYTES,
        "rounds": rounds,
        "decision": decision,
        "interpretation_limit": (
            "An A/B crossover of 32-MiB prefixes is not a causal estimate "
            "of multi-GB end-to-end speed or proof of full ZIP integrity. "
            "The official catalog check is metadata-only."
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    failed = False
    try:
        report = run_crossover()
    except (GetProbeError, requests.RequestException, OSError) as exc:
        failed = True
        report = {
            "schema": DIAGNOSTIC_SCHEMA,
            "status": "rejected",
            "reason": str(exc) if isinstance(exc, GetProbeError) else type(exc).__name__,
        }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False, indent=2), flush=True)
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
