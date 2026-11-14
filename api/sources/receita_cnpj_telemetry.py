"""A09.1 observational telemetry for Receita open-data acquisition.

Metrics contain no CNPJ, company name, provider label, URL or raw source
record. Errors in the optional metrics channel cannot affect acquisition.
"""
from __future__ import annotations

import hashlib
import json
import os
import platform
import re
import resource
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

FILE_CATEGORY = re.compile(r"^(Empresas|Estabelecimentos)(\d+)\.zip$", re.IGNORECASE)
METADATA_KEYS = ("name", "size", "etag", "last_modified")
SAFE_FIELDS = frozenset({
    "phase", "category", "file_index", "attempts", "retry_wait_s",
    "elapsed_s", "elapsed_download_s", "elapsed_parse_s",
    "received_bytes", "transferred_bytes_total", "expected_bytes", "rows_scanned", "rows_retained",
    "reference_period", "release_fingerprint", "fingerprint_method",
    "file_count", "company_partitions", "establishment_partitions",
    "cache_decision", "state", "target_count", "provider_count",
    "etag_present", "last_modified_present", "zip_valid",
})


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def file_category(filename: str) -> tuple[str, int | None]:
    name = Path(filename).name
    match = FILE_CATEGORY.fullmatch(name)
    if match:
        return match.group(1).lower(), int(match.group(2))
    if name == "Cnaes.zip":
        return "cnaes", None
    if name == "Naturezas.zip":
        return "naturezas", None
    if name == "Motivos.zip":
        return "motivos", None
    return "other", None


def release_fingerprint(period: str, resources: list[dict[str, Any]]) -> str:
    """Metadata identity, explicitly NOT a digest of remote ZIP bytes."""
    allowed = [
        {field: item.get(field) for field in METADATA_KEYS}
        for item in resources
        if isinstance(item, dict)
    ]
    packed = json.dumps(
        {"reference_period": period,
         "resources": sorted(allowed, key=lambda r: str(r["name"]))},
        sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(packed).hexdigest()


def emit(event: str, **fields: Any) -> None:
    """Emit bounded, PII-free JSON and optional local JSONL (best effort)."""
    try:
        if not re.fullmatch(r"[a-z_]{2,48}", event):
            return
        safe = {key: value for key, value in fields.items()
                if key in SAFE_FIELDS and isinstance(
                    value, (str, int, float, bool, type(None))
                )}
        rss = None
        if platform.system() == "Linux":
            rss = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 2)
        free_mb = round(shutil.disk_usage(Path.cwd()).free / (1024 * 1024), 2)
        payload = {"event": event, "observed_at": utcnow(),
                   **safe, "peak_rss_mib": rss, "disk_free_mib": free_mb}
        line = "RECEITA_A09 " + json.dumps(payload, sort_keys=True)
        print(line, file=sys.stderr, flush=True)
        destination = os.environ.get("RECEITA_A09_TELEMETRY_PATH")
        if destination:
            path = Path(destination)
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as output:
                output.write(json.dumps(payload, sort_keys=True) + "\n")
    except (OSError, TypeError, ValueError):
        # No audit-side effect can change an official source decision.
        return
