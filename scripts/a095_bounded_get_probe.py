"""A09.5 second proof: bounded ordinary GET, one versus two connections.

Only four HTTP GET requests against two official CNPJ ZIP partitions.
Each response is closed after at most 8 MiB of client-side reading.
No HTTP Range dependency, no ZIP persistence, no full national download,
no production cache/pipeline/deployment changes or paid API.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import requests

from api.sources.receita_cnpj_bulk import (
    _configure_official_session,
    discover_latest_release,
)
from api.sources.receita_cnpj_cache_integrity import lifecycle_release_fingerprint

READ_LIMIT = 8 * 1024 * 1024
READ_CHUNK = 64 * 1024
MAX_CONCURRENT = 2
MAX_REQUEST_SECONDS = 100.0
FILE_INDEX = re.compile(r"Estabelecimentos(\d+)\.zip\Z")


class GetProbeError(RuntimeError):
    """Safe, non-URL error for an incomplete or inconsistent probe."""


def _release_resources() -> tuple[Any, str, list[dict[str, Any]]]:
    release = discover_latest_release()
    fingerprint = lifecycle_release_fingerprint(release)
    if not fingerprint or len(release.establishment_files) < MAX_CONCURRENT:
        raise GetProbeError("official_manifest_incomplete")
    metadata = {entry["name"]: entry for entry in release.resource_metadata}
    resources = []
    for name in release.establishment_files[:MAX_CONCURRENT]:
        match = FILE_INDEX.fullmatch(name)
        entry = metadata.get(name)
        if not match or not isinstance(entry, dict):
            raise GetProbeError("official_partition_metadata_missing")
        size = entry.get("size")
        if type(size) is not int or size <= READ_LIMIT:
            raise GetProbeError("official_partition_size_invalid")
        resources.append({
            "url": urljoin(release.release_url, name),
            "file_index": int(match.group(1)),
            "size": size,
        })
    if len({x["file_index"] for x in resources}) != MAX_CONCURRENT:
        raise GetProbeError("duplicate_partition_index")
    return release, fingerprint, resources


def _check_get_headers(response: requests.Response, expected_size: int) -> dict[str, Any]:
    """Reject unexpected representations before reading any response body."""
    if response.status_code != 200:
        raise GetProbeError("unexpected_http_status_" + str(response.status_code))
    if response.headers.get("Content-Range"):
        raise GetProbeError("unexpected_content_range_on_full_get")
    if response.headers.get("Content-Encoding", "identity").lower().strip() not in ("", "identity"):
        raise GetProbeError("unexpected_content_encoding")
    content_length = response.headers.get("Content-Length")
    if content_length is not None and (
        not content_length.isdecimal() or int(content_length) != expected_size
    ):
        raise GetProbeError("official_size_does_not_match_response")
    return {
        "content_length_present": content_length is not None,
        "etag": response.headers.get("ETag"),
    }


def _get_prefix(resource: dict[str, Any], *, read_limit: int | None = None) -> dict[str, Any]:
    """Read a strictly bounded prefix and close the full-GET response immediately.

    Underlying network and OS buffers may receive slightly more bytes; this
    caps APPLICATION-CONSUMED data, not bytes sent by the official server.
    """
    if read_limit is None:
        read_limit = READ_LIMIT
    if type(read_limit) is not int or read_limit < 4 or read_limit > 32 * 1024 * 1024:
        raise GetProbeError("invalid_probe_read_limit")
    size = resource["size"]
    index = resource["file_index"]
    if type(size) is not int or size <= read_limit or type(index) is not int:
        raise GetProbeError("invalid_probe_resource")
    started = time.monotonic()
    received = 0
    digest = hashlib.sha256()

    # requests.Session is NOT shared between threads.
    with requests.Session() as session:
        _configure_official_session(session)
        with session.get(
            resource["url"],
            headers={"Accept-Encoding": "identity"},
            stream=True,
            timeout=(10, 20),
            allow_redirects=True,
        ) as response:
            header_info = _check_get_headers(response, size)
            headers_seconds = time.monotonic() - started
            read_started = time.monotonic()
            for chunk in response.iter_content(chunk_size=READ_CHUNK):
                if time.monotonic() - started > MAX_REQUEST_SECONDS:
                    raise GetProbeError("get_elapsed_budget_exceeded")
                if not chunk:
                    continue
                if len(chunk) > READ_CHUNK or received + len(chunk) > read_limit:
                    raise GetProbeError("prefix_read_limit_exceeded")
                if received == 0 and chunk[:4] != b"PK\x03\x04":
                    raise GetProbeError("unexpected_zip_local_header")
                digest.update(chunk)
                received += len(chunk)
                if received == read_limit:
                    break
            read_seconds = time.monotonic() - read_started

    if received != read_limit:
        raise GetProbeError("truncated_get_prefix")
    elapsed = time.monotonic() - started
    if elapsed <= 0 or elapsed > MAX_REQUEST_SECONDS:
        raise GetProbeError("get_elapsed_budget_exceeded")
    return {
        "file_index": index,
        "http_status": 200,
        "read_bytes": received,
        "elapsed_seconds": round(elapsed, 3),
        "headers_seconds": round(headers_seconds, 3),
        "read_seconds": round(read_seconds, 3),
        "throughput_mib_s": round((received / 1048576) / elapsed, 4),
        "sha256_prefix": digest.hexdigest(),
        **header_info,
    }


def run_get_probe() -> dict[str, Any]:
    release, fingerprint, resources = _release_resources()

    sequential_start = time.monotonic()
    sequential = [_get_prefix(resource) for resource in resources]
    sequential_wall = time.monotonic() - sequential_start

    parallel_start = time.monotonic()
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as pool:
        futures = [pool.submit(_get_prefix, resource) for resource in resources]
        concurrent = [future.result() for future in futures]
    parallel_wall = time.monotonic() - parallel_start

    if any(
        left["file_index"] != right["file_index"]
        or left["read_bytes"] != READ_LIMIT or right["read_bytes"] != READ_LIMIT
        or left["sha256_prefix"] != right["sha256_prefix"]
        or (
            left.get("etag") is not None and right.get("etag") is not None
            and left["etag"] != right["etag"]
        )
        for left, right in zip(sequential, concurrent)
    ):
        raise GetProbeError("prefix_identity_changed_between_modes")

    later, later_fingerprint, _ = _release_resources()
    if later.release_url != release.release_url or later_fingerprint != fingerprint:
        raise GetProbeError("official_release_changed_during_probe")

    if sequential_wall <= 0 or parallel_wall <= 0:
        raise GetProbeError("invalid_wall_clock_duration")
    return {
        "schema": "a095-bounded-ordinary-get-transport-v2",
        "status": "measured",
        "reference_period": release.period,
        "official_metadata_fingerprint": fingerprint,
        "file_indices": [x["file_index"] for x in resources],
        "max_concurrent_connections": MAX_CONCURRENT,
        "get_requests": MAX_CONCURRENT * 2,
        "max_application_read_bytes": MAX_CONCURRENT * 2 * READ_LIMIT,
        "observed_application_read_bytes": sum(
            item["read_bytes"] for item in sequential + concurrent
        ),
        "sequential": sequential,
        "parallel": concurrent,
        "sequential_wall_seconds": round(sequential_wall, 3),
        "parallel_wall_seconds": round(parallel_wall, 3),
        "sequential_aggregate_mib_s": round((2 * READ_LIMIT / 1048576) / sequential_wall, 4),
        "parallel_aggregate_mib_s": round((2 * READ_LIMIT / 1048576) / parallel_wall, 4),
        "observed_speedup": round(sequential_wall / parallel_wall, 3),
        "caveats": [
            "Four conventional GET requests intentionally stop after 8 MiB of client reading.",
            "The official server may transmit some buffered bytes after client disconnect.",
            "Sequential is measured before parallel; time/order/cache effects are uncontrolled.",
            "One short-prefix comparison cannot predict multi-GB download speed or reliability.",
            "No productive code or persistent caches were modified by this diagnostic.",
        ],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    failed = False
    try:
        result = run_get_probe()
    except (GetProbeError, requests.RequestException, OSError) as exc:
        failed = True
        result = {
            "schema": "a095-bounded-ordinary-get-transport-v2",
            "status": "rejected",
            # Avoid leaking DAV token/URL in raw requests exceptions.
            "reason": str(exc) if isinstance(exc, GetProbeError) else type(exc).__name__,
        }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
