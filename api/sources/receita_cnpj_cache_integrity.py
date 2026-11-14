"""A09.2: metadata evidence needed before labeling a Receita cache current.

The official WebDAV metadata fingerprint is NOT a checksum of remote content.
It detects changes in resource names, sizes, ETags or Last-Modified fields,
including republishing in the same calendar period. Absence of dependable
metadata is never evidence that the publication is unchanged.
"""
from __future__ import annotations

import re
from typing import Any

from api.sources.receita_cnpj_telemetry import release_fingerprint

FINGERPRINT_METHOD = "period_filename_size_etag_last_modified_sha256_metadata"
PERIOD = re.compile(r"20\d\d-(0[1-9]|1[0-2])")


def checked_release_fingerprint(
    period: str,
    resources: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    required_names: list[str] | tuple[str, ...],
) -> str | None:
    """Return a comparable digest only for a complete, reliable manifest.

    A stable digest is derived from the exact required resource set. One file
    without size or independent change metadata makes validation unavailable.
    """
    if not isinstance(period, str) or not PERIOD.fullmatch(period):
        return None
    if not required_names or len(set(required_names)) != len(required_names):
        return None
    if not isinstance(resources, (list, tuple)):
        return None

    by_name: dict[str, dict[str, Any]] = {}
    for item in resources:
        if not isinstance(item, dict):
            return None
        name = item.get("name")
        if not isinstance(name, str) or not name or name in by_name:
            return None
        by_name[name] = item

    if set(by_name) != set(required_names):
        return None
    for name in required_names:
        item = by_name[name]
        size = item.get("size")
        if type(size) is not int or size <= 0:
            return None
        if not (isinstance(item.get("etag"), str) and item["etag"].strip()
                or isinstance(item.get("last_modified"), str)
                and item["last_modified"].strip()):
            return None
    return release_fingerprint(period, [by_name[name] for name in required_names])


def lifecycle_release_fingerprint(release: Any) -> str | None:
    """Only Motivos and every discovered Estabelecimentos partition matter."""
    names = [release.reasons_file, *release.establishment_files]
    return checked_release_fingerprint(
        release.period, list(release.resource_metadata), names,
    )


def lifecycle_cache_current(payload: dict[str, Any], release: Any) -> bool:
    """Require exact source period, URL and metadata fingerprint for freshness."""
    source = payload.get("source") if isinstance(payload, dict) else None
    if not isinstance(source, dict):
        return False
    digest = lifecycle_release_fingerprint(release)
    return bool(
        digest
        and source.get("reference_period") == release.period
        and source.get("release_url") == release.release_url
        and source.get("release_fingerprint_method") == FINGERPRINT_METHOD
        and source.get("release_fingerprint") == digest
    )


def identity_cache_current(
    payload: dict[str, Any],
    release: Any,
    fingerprint: str | None,
) -> bool:
    source = payload.get("source") if isinstance(payload, dict) else None
    if not isinstance(source, dict):
        return False
    return bool(
        fingerprint
        and source.get("reference_period") == release.period
        and source.get("release_url") == release.release_url
        and source.get("release_fingerprint_method") == FINGERPRINT_METHOD
        and source.get("release_fingerprint") == fingerprint
    )
