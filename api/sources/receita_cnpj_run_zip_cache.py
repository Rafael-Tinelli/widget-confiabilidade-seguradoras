"""A09.4: integrity-checked ZIP reuse only within one Gate 4 generation.

The lifecycle and identity commands are separate processes. Retain just their
common Estabelecimentos partitions in RUNNER_TEMP, never in persisted source
caches or publication artifacts. Every read verifies official per-file metadata,
the exact build identity, length, ZIP structure and local SHA-256.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import time
import zipfile
from pathlib import Path
from typing import Any, Callable

from api.sources.receita_cnpj_telemetry import emit, file_category

_BUILD_ID = re.compile(r"[A-Za-z0-9_.-]{1,160}\Z")
_ESTABLISHMENT = re.compile(r"Estabelecimentos\d+\.zip\Z")


def _cache_dir() -> Path | None:
    build_id = os.environ.get("V2_BUILD_ID", "")
    runner_temp = os.environ.get("RUNNER_TEMP", "")
    if not _BUILD_ID.fullmatch(build_id) or not runner_temp:
        return None
    # RUNNER_TEMP belongs to one ephemeral runner job; the build id separates
    # sequential generations and excludes snapshots from other executions.
    return Path(runner_temp) / "receita-a09-run-zips" / build_id


def _trusted_metadata(name: str, metadata: dict[str, Any] | None) -> dict[str, Any] | None:
    if not _ESTABLISHMENT.fullmatch(name) or not isinstance(metadata, dict):
        return None
    if metadata.get("name") != name:
        return None
    size = metadata.get("size")
    if type(size) is not int or size <= 0:
        return None
    etag = metadata.get("etag")
    modified = metadata.get("last_modified")
    if not (isinstance(etag, str) and etag.strip()
            or isinstance(modified, str) and modified.strip()):
        return None
    return {
        "name": name, "size": size,
        "etag": etag if isinstance(etag, str) else None,
        "last_modified": modified if isinstance(modified, str) else None,
    }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _valid_file(path: Path, size: int, sha256: str | None = None) -> bool:
    if path.is_symlink() or not path.is_file() or path.stat().st_size != size:
        return False
    if not zipfile.is_zipfile(path):
        return False
    return sha256 is None or _sha256(path) == sha256


def _link_or_copy(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.unlink(missing_ok=True)
    try:
        os.link(source, destination)
    except OSError:
        shutil.copy2(source, destination)


def acquire_run_establishment_zip(
    *,
    release: Any,
    name: str,
    metadata: dict[str, Any] | None,
    destination: Path,
    download: Callable[[], Path],
) -> Path:
    """Use a verified current-run file or perform the unchanged official GET.

    A cache error never causes consumption of unchecked bytes: treat it as a
    miss and invoke the original downloader. Each consumer unlinks only its
    working path; the run-scoped hard link survives until the runner exits.
    """
    root = _cache_dir()
    resource = _trusted_metadata(name, metadata)
    period = getattr(release, "period", None)
    url = getattr(release, "release_url", None)
    if (root is None or resource is None or not isinstance(period, str)
            or not re.fullmatch(r"20\d{2}-(?:0[1-9]|1[0-2])", period)
            or not isinstance(url, str) or not url.startswith("https://")):
        return download()

    scope = {
        "schema": "receita-run-establishment-zip-v1",
        "build_id": os.environ["V2_BUILD_ID"],
        "reference_period": period,
        "release_url": url,
        "resource": resource,
    }
    stored_zip = root / name
    stored_meta = root / (name + ".json")
    started = time.perf_counter()
    category, index = file_category(name)
    try:
        saved = json.loads(stored_meta.read_text(encoding="utf-8"))
        if (
            isinstance(saved, dict)
            and all(saved.get(key) == value for key, value in scope.items())
            and isinstance(saved.get("sha256"), str)
            and re.fullmatch(r"[0-9a-f]{64}", saved["sha256"])
            and _valid_file(stored_zip, resource["size"], saved["sha256"])
        ):
            _link_or_copy(stored_zip, destination)
            emit(
                "zip_reuse", category=category, file_index=index,
                state="verified", expected_bytes=resource["size"],
                elapsed_s=round(time.perf_counter() - started, 3),
            )
            return destination
    except (OSError, ValueError, TypeError):
        pass

    # Fail closed on stale metadata, absent bytes, corrupt SHA or a different
    # run. A hit never performs HTTP; a miss still uses the original downloader.
    fetched = download()
    try:
        if _valid_file(fetched, resource["size"]):
            root.mkdir(parents=True, exist_ok=True)
            temp_zip = root / (name + ".incoming")
            temp_meta = root / (name + ".json.incoming")
            try:
                temp_zip.unlink(missing_ok=True)
                _link_or_copy(fetched, temp_zip)
                sha256 = _sha256(temp_zip)
                temp_zip.replace(stored_zip)
                temp_meta.write_text(
                    json.dumps({**scope, "sha256": sha256}, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
                temp_meta.replace(stored_meta)
                emit("zip_reuse", category=category, file_index=index,
                     state="stored", expected_bytes=resource["size"])
            finally:
                temp_zip.unlink(missing_ok=True)
                temp_meta.unlink(missing_ok=True)
        else:
            emit("zip_reuse", category=category, file_index=index,
                 state="unverifiable", expected_bytes=resource["size"])
    except (OSError, ValueError, TypeError):
        # Cache maintenance must not mask a valid official acquisition.
        emit("zip_reuse", category=category, file_index=index,
             state="storage_unavailable")
    return fetched
