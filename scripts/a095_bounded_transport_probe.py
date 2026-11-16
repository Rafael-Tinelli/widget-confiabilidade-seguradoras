"""A09.5: bounded official WebDAV throughput probe, never a full download.

Manual diagnostic only. Up to 2 concurrent HTTP Range requests, over the
first 8 MiB of each of two official Estabelecimentos ZIP partitions. Each
request has its own requests.Session; raw source bytes are discarded.
No package publication, no persisted cache, no CNPJ data, no mutations.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urljoin

import requests

from api.sources.receita_cnpj_bulk import (
    _configure_official_session,
    discover_latest_release,
)
from api.sources.receita_cnpj_cache_integrity import lifecycle_release_fingerprint

MAX_CONCURRENT = 2
RANGE_BYTES = 8 * 1024 * 1024
MAX_REQUEST_SECONDS = 100.0
CONTENT_RANGE = re.compile(r"bytes (\d+)-(\d+)/(\d+)\Z")


class RangeUnsupported(RuntimeError):
    """Official endpoint ignored a byte range or returned invalid range headers."""


class ProbeError(RuntimeError):
    """Transfer was incomplete or official source integrity became uncertain."""


def _require_range_response(response: requests.Response, *, size: int, count: int) -> None:
    """Fail BEFORE reading bytes when the server ignores Range."""
    if response.status_code != 206:
        raise RangeUnsupported(f"Expected HTTP 206, got {response.status_code}")
    header = response.headers.get("Content-Range", "")
    match = CONTENT_RANGE.fullmatch(header.strip())
    if not match or tuple(map(int, match.groups())) != (0, count - 1, size):
        raise RangeUnsupported("Invalid Content-Range for official WebDAV resource")
    length = response.headers.get("Content-Length")
    if length is not None and (not length.isdigit() or int(length) != count):
        raise RangeUnsupported("Incorrect Content-Length for partial response")
    if response.headers.get("Content-Encoding", "identity").lower() not in (
        "", "identity",
    ):
        raise RangeUnsupported("Unexpected content encoding for ZIP bytes")


def _transfer_range(
    *,
    url: str,
    size: int,
    count: int,
    file_index: int,
) -> dict:
    if size <= count or count <= 0 or count > RANGE_BYTES:
        raise ProbeError("Invalid resource size or requested probe bound")
    started = time.monotonic()
    digest = hashlib.sha256()
    received = 0
    # A new Session PER THREAD avoids shared mutable Session state.
    with requests.Session() as session:
        _configure_official_session(session)
        with session.get(
            url,
            headers={
                "Range": f"bytes=0-{count - 1}",
                "Accept-Encoding": "identity",
            },
            timeout=(10, 30),
            stream=True,
            allow_redirects=True,
        ) as response:
            _require_range_response(response, size=size, count=count)
            for chunk in response.iter_content(chunk_size=256 * 1024):
                if time.monotonic() - started > MAX_REQUEST_SECONDS:
                    raise ProbeError("Bounded probe exceeded its wall-clock budget")
                received += len(chunk)
                if received > count:
                    raise ProbeError("Official server exceeded requested byte range")
                digest.update(chunk)
    if received != count:
        raise ProbeError(f"Truncated partial response: {received}/{count} bytes")
    elapsed = time.monotonic() - started
    return {
        "file_index": file_index,
        "bytes": received,
        "elapsed_seconds": round(elapsed, 3),
        "sha256_prefix": digest.hexdigest()[:12],
        "throughput_mib_per_s": round(received / (1024 * 1024) / elapsed, 4),
    }


def _release_metadata():
    release = discover_latest_release()
    fingerprint = lifecycle_release_fingerprint(release)
    if fingerprint is None or len(release.establishment_files) < MAX_CONCURRENT:
        raise ProbeError("Official publication lacks complete metadata or two ZIPs")
    by_name = {entry["name"]: entry for entry in release.resource_metadata}
    names = release.establishment_files[:MAX_CONCURRENT]
    resources = []
    for name in names:
        item = by_name.get(name)
        if not item or type(item.get("size")) is not int or item["size"] <= RANGE_BYTES:
            raise ProbeError("Official file size missing or smaller than range")
        resources.append({
            "name": name, "url": urljoin(release.release_url, name),
            "size": item["size"],
            "file_index": int(re.search(r"(\d+)", name).group(1)),
        })
    return release, fingerprint, resources


def run_transport_probe() -> dict:
    release, fingerprint, resources = _release_metadata()
    # 1 KiB probe: abort without downloading the full response if Range is
    # ignored. This also detects redirects that strip Range headers.
    first = resources[0]
    preflight = _transfer_range(
        url=first["url"], size=first["size"], count=1024,
        file_index=first["file_index"],
    )

    sequential = [
        _transfer_range(
            url=item["url"], size=item["size"], count=RANGE_BYTES,
            file_index=item["file_index"],
        )
        for item in resources
    ]

    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as pool:
        futures = [
            pool.submit(
                _transfer_range,
                url=item["url"], size=item["size"],
                count=RANGE_BYTES, file_index=item["file_index"],
            )
            for item in resources
        ]
        concurrent = [future.result() for future in futures]
    parallel_elapsed = time.monotonic() - started

    # Same offsets and file versions must yield identical bytes.
    if any(a["sha256_prefix"] != b["sha256_prefix"]
           for a, b in zip(sequential, concurrent)):
        raise ProbeError("Official partial responses differ across modes")

    latest, latest_fingerprint, _ = _release_metadata()
    if latest.release_url != release.release_url or latest_fingerprint != fingerprint:
        raise ProbeError("Official publication changed during the experiment")

    serial_seconds = sum(item["elapsed_seconds"] for item in sequential)
    return {
        "schema": "a095-limited-parallel-webdav-probe-v1",
        "status": "measured",
        "reference_period": release.period,
        "official_metadata_fingerprint": fingerprint,
        "file_indices": [x["file_index"] for x in resources],
        "max_concurrent_connections": MAX_CONCURRENT,
        "probe_bytes_per_request": RANGE_BYTES,
        "total_range_bytes_requested": 1024 + 4 * RANGE_BYTES,
        "range_preflight": preflight,
        "sequential": sequential,
        "concurrent": concurrent,
        "sequential_wall_seconds": round(serial_seconds, 3),
        "parallel_wall_seconds": round(parallel_elapsed, 3),
        "observed_speedup": round(serial_seconds / parallel_elapsed, 3),
        "caveat": (
            "One short-range A/B comparison is diagnostic, NOT a measured "
            "full-generation saving or authorization to modify the downloader."
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        report = run_transport_probe()
    except RangeUnsupported as exc:
        report = {
            "schema": "a095-limited-parallel-webdav-probe-v1",
            "status": "range_not_supported",
            "reason": str(exc),
            "note": "No full official ZIP was transferred; parallel GET speed not tested.",
        }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
