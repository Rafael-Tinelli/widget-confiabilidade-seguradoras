"""A09.6 opt-in same-run ZIP retention for a later LOCAL sequential replay.

Nothing survives the ephemeral GitHub runner. ZIPs are hard-linked, never
copied or published, and checked by official size/metadata and local SHA-256.
A failure to retain is purely observational and never changes a source build.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import zipfile
from pathlib import Path
from typing import Any

from api.sources.receita_cnpj_run_zip_cache import _cache_dir as a094_cache_dir
from api.sources.receita_cnpj_telemetry import emit

ENV_NAME = "RECEITA_A096_RETAIN_ZIPS"
_BUILD_ID = re.compile(r"[A-Za-z0-9_.-]{1,160}\Z")
_COMPANY = re.compile(r"Empresas(?:0|[1-9]\d*)\.zip\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
FAMILIES = ("Empresas", "Estabelecimentos")
REQUIRED_INDICES = tuple(range(10))


def _company_root() -> Path | None:
    build = os.environ.get("V2_BUILD_ID", "")
    runner_temp = os.environ.get("RUNNER_TEMP", "")
    if not _BUILD_ID.fullmatch(build) or not runner_temp:
        return None
    return Path(runner_temp) / "a096-company-run-zips" / build


def enabled() -> bool:
    return os.environ.get(ENV_NAME) == "1"


def _sha256(path: Path) -> str:
    sha = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            sha.update(block)
    return sha.hexdigest()


def _file_valid(path: Path, size: int, digest: str | None = None) -> bool:
    return bool(
        not path.is_symlink() and path.is_file()
        and path.stat().st_size == size
        and zipfile.is_zipfile(path)
        and (digest is None or _sha256(path) == digest)
    )


def capture_company(
    original: Path, *, name: str, metadata: dict[str, Any] | None,
    release: Any,
) -> None:
    """Link one already acquired file; a failure cannot affect the parser."""
    if not enabled():
        return
    root = _company_root()
    try:
        if root is None or not _COMPANY.fullmatch(name) or not isinstance(metadata, dict):
            return
        size = metadata.get("size")
        etag, modified = metadata.get("etag"), metadata.get("last_modified")
        if (metadata.get("name") != name or type(size) is not int or size <= 0
                or not (isinstance(etag, str) and etag.strip()
                        or isinstance(modified, str) and modified.strip())
                or not _file_valid(original, size)):
            emit("a096_capture", category="empresas", state="invalid_candidate")
            return
        period = getattr(release, "period", "")
        release_url = getattr(release, "release_url", "")
        if not re.fullmatch(r"20\d{2}-(?:0[1-9]|1[0-2])", period):
            return
        if not isinstance(release_url, str) or not release_url.startswith("https://"):
            return
        root.mkdir(parents=True, exist_ok=True)
        target = root / name
        sidecar = root / (name + ".json")
        target.unlink(missing_ok=True)
        sidecar.unlink(missing_ok=True)
        os.link(original, target)
        local_digest = _sha256(target)
        receipt = {
            "schema": "a096-company-capture-v1",
            "build_id": os.environ["V2_BUILD_ID"],
            "reference_period": period,
            "release_url_sha256": hashlib.sha256(release_url.encode()).hexdigest(),
            "resource": {
                "name": name, "size": size,
                "etag": etag if isinstance(etag, str) else None,
                "last_modified": modified if isinstance(modified, str) else None,
            },
            "sha256": local_digest,
        }
        sidecar.write_text(json.dumps(receipt, sort_keys=True) + "\n", encoding="utf-8")
        emit("a096_capture", category="empresas", file_index=int(name[7:-4]),
             state="verified_hardlink", expected_bytes=size)
    except (OSError, ValueError, TypeError, zipfile.BadZipFile):
        # Never override the valid original download or interfere with parsing.
        emit("a096_capture", category="empresas", state="unavailable")


def local_input_manifest(
    *, source_root: Path, conduct_root: Path,
) -> dict[str, Any]:
    """Require all 20 SAME-RUN validated ZIPs and both materialized snapshots.

    Receipts from A09.4 authenticate Estabelecimentos; our opt-in receipts
    authenticate Empresas. No ZIP bytes enter the returned JSON.
    """
    company_root = _company_root()
    establishment_root = a094_cache_dir()
    build = os.environ.get("V2_BUILD_ID")
    if (company_root is None or establishment_root is None or not enabled()
            or build is None):
        raise ValueError("capture_not_enabled_or_unscoped")
    families: dict[str, list[dict[str, Any]]] = {}
    periods: set[str] = set()
    official_sha: dict[str, set[str]] = {family: set() for family in FAMILIES}
    for family, root, prefix in (
        ("Empresas", company_root, "a096-company-capture-v1"),
        ("Estabelecimentos", establishment_root, "receita-run-establishment-zip-v1"),
    ):
        records = []
        for index in REQUIRED_INDICES:
            name = f"{family}{index}.zip"
            path = root / name
            receipt_path = root / (name + ".json")
            if not receipt_path.is_file() or not path.is_file():
                raise ValueError("incomplete_local_zip_capture")
            payload = json.loads(receipt_path.read_text(encoding="utf-8"))
            resource = payload.get("resource") or {}
            if (payload.get("schema") != prefix or payload.get("build_id") != build
                    or resource.get("name") != name
                    or type(resource.get("size")) is not int
                    or resource["size"] <= 0
                    or not _SHA256.fullmatch(str(payload.get("sha256") or ""))
                    or not _file_valid(path, resource["size"], payload["sha256"])):
                raise ValueError("invalid_local_zip_capture")
            period = payload.get("reference_period")
            if not isinstance(period, str):
                raise ValueError("missing_local_reference_period")
            periods.add(period)
            if family == "Empresas":
                official_sha[family].add(payload.get("release_url_sha256"))
            else:
                original_url = payload.get("release_url")
                if not isinstance(original_url, str):
                    raise ValueError("missing_a094_release_url")
                official_sha[family].add(hashlib.sha256(original_url.encode()).hexdigest())
            records.append({
                "name": name, "size": resource["size"], "sha256": payload["sha256"],
                "etag": resource.get("etag"),
                "last_modified": resource.get("last_modified"),
            })
        families[family] = records
    if len(periods) != 1 or len(official_sha["Empresas"]) != 1 or (
        official_sha["Empresas"] != official_sha["Estabelecimentos"]
    ):
        raise ValueError("local_sources_do_not_share_release")

    snapshots = {}
    for source, root in (
        ("receita_cnpj_lifecycle", source_root),
        ("receita_consumer_identity", conduct_root),
    ):
        file = root / (source + ".json")
        receipt = root / (source + ".meta.json")
        if not file.is_file() or not receipt.is_file():
            raise ValueError("missing_snapshot_and_cache_receipt")
        meta = json.loads(receipt.read_text(encoding="utf-8"))
        content = json.loads(file.read_text(encoding="utf-8"))
        digest = _sha256(file)
        if (meta.get("source_id") != source or meta.get("sha256") != digest
                or (content.get("source") or {}).get("reference_period") not in periods):
            raise ValueError("snapshot_contract_does_not_match_captured_zips")
        snapshots[source] = digest

    return {
        "schema": "a096-local-replay-input-v1",
        "build_id": build,
        "reference_period": periods.pop(),
        "source_head_sha": os.environ.get("V2_SOURCE_HEAD_SHA"),
        "release_url_sha256": next(iter(official_sha["Empresas"])),
        "input_mode": "same_exact_local_zip_bytes",
        "zip_files_verified": sum(len(x) for x in families.values()),
        "zip_partitions": families,
        "serial_snapshot_sha256": snapshots,
        "retention": "ephemeral_runner_only_no_zip_artifact",
    }
