#!/usr/bin/env python3
"""Validate and seal the persistent HostGator Receita cache.

The input metadata must come from a fresh official release discovery performed
by the GitHub runner through the pinned HostGator bridge. Each cached ZIP is
validated by size + ZIP structure + SHA-256. The manifest binds those local
bytes to the current remote ETag/size fingerprint.
"""

import argparse
import hashlib
import json
import os
import zipfile
from pathlib import Path


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_write(path, payload):
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(str(temp), str(path))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--metadata", required=True)
    parser.add_argument("--cache-root", required=True)
    args = parser.parse_args()

    meta = json.loads(Path(args.metadata).read_text(encoding="utf-8"))
    period = str(meta.get("period") or "")
    resources = list(meta.get("resources") or [])
    if not period or not resources:
        raise SystemExit("CACHE_MANIFEST_INVALID_INPUT")

    cache_root = Path(args.cache_root).expanduser().resolve() / period
    if not cache_root.is_dir():
        raise SystemExit("CACHE_MANIFEST_PERIOD_MISSING=" + period)

    manifest_path = cache_root / "manifest.json"
    previous = {}
    if manifest_path.is_file():
        loaded = json.loads(manifest_path.read_text(encoding="utf-8"))
        if str(loaded.get("reference_period") or "") == period:
            previous = dict(loaded.get("files") or {})

    entries = {}
    for remote in sorted(resources, key=lambda row: str(row.get("name") or "")):
        name = str(remote.get("name") or "")
        if not name.endswith(".zip"):
            continue
        path = cache_root / name
        if not path.is_file():
            raise SystemExit("CACHE_MANIFEST_MISSING=" + name)
        expected_size = remote.get("size")
        actual_size = path.stat().st_size
        if expected_size is not None and actual_size != int(expected_size):
            raise SystemExit(
                "CACHE_MANIFEST_SIZE_MISMATCH="
                + name
                + ":"
                + str(actual_size)
                + "!="
                + str(expected_size)
            )
        if not zipfile.is_zipfile(str(path)):
            raise SystemExit("CACHE_MANIFEST_INVALID_ZIP=" + name)

        remote_identity = {
            "size": expected_size,
            "etag": str(remote.get("etag") or ""),
            "last_modified": str(remote.get("last_modified") or ""),
        }
        previous_entry = previous.get(name) or {}
        previous_remote = previous_entry.get("remote") or {}
        if previous_remote and previous_remote != remote_identity:
            raise SystemExit(
                "CACHE_REMOTE_FINGERPRINT_CHANGED="
                + name
                + ":"
                + json.dumps(
                    {
                        "previous": previous_remote,
                        "current": remote_identity,
                    },
                    sort_keys=True,
                )
            )

        digest = sha256_file(path)
        previous_local = previous_entry.get("local") or {}
        previous_sha = str(previous_local.get("sha256") or "")
        if previous_sha and digest != previous_sha:
            raise SystemExit(
                "CACHE_LOCAL_SHA256_MISMATCH="
                + name
                + ":"
                + digest
                + "!="
                + previous_sha
            )
        entries[name] = {
            "remote": remote_identity,
            "local": {
                "size": actual_size,
                "sha256": digest,
                "mtime_ns": path.stat().st_mtime_ns,
            },
        }
        print(
            "CACHE_MANIFEST_VERIFIED "
            + name
            + " size="
            + str(actual_size)
            + " sha256="
            + digest,
            flush=True,
        )

    expected_names = {
        str(row.get("name") or "")
        for row in resources
        if str(row.get("name") or "").endswith(".zip")
    }
    if set(entries) != expected_names:
        raise SystemExit(
            "CACHE_MANIFEST_INCOMPLETE="
            + json.dumps(
                {
                    "missing": sorted(expected_names - set(entries)),
                    "extra": sorted(set(entries) - expected_names),
                },
                sort_keys=True,
            )
        )

    manifest = {
        "schema_version": "1.0",
        "reference_period": period,
        "files": entries,
    }
    atomic_write(manifest_path, manifest)
    print(
        "CACHE_MANIFEST_READY "
        + json.dumps(
            {
                "period": period,
                "file_count": len(entries),
                "manifest": str(manifest_path),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
