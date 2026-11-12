"""Fail-closed selection of the active Consumer.gov Base Completa window.

The original versioned core has an embedded 12-month selection in aggregate
metadata, independently protected by the existing V2 Git/bootstrap manifest.
Future refreshed generations use one atomic, content-addressed sidecar.
Neither route discovers months by globbing a directory.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

WINDOW_MANIFEST = "consumidor_gov_window_manifest.json"
WINDOW_MANIFEST_VERSION = 1
SOURCE_FAMILY = "base_completa"


class ConsumerGovWindowError(RuntimeError):
    """The declared source window is incomplete or inconsistent."""


@dataclass(frozen=True)
class ActiveConsumerWindow:
    aggregate_path: Path
    aggregate: dict[str, Any]
    months: list[tuple[str, Path, dict[str, Any]]]
    provenance: str


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_gz(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rb") as handle:
            payload = json.load(handle)
    except (OSError, UnicodeError, ValueError) as exc:
        raise ConsumerGovWindowError(f"unreadable Consumer.gov object: {path}") from exc
    if not isinstance(payload, dict):
        raise ConsumerGovWindowError(f"Consumer.gov object is not JSON mapping: {path}")
    return payload


def _selected_months(months: Any, required: int) -> list[str]:
    if not isinstance(months, list) or len(months) != required:
        raise ConsumerGovWindowError(
            f"window requires exactly {required} declared months"
        )
    if not all(isinstance(m, str) and re.fullmatch(r"20\d{2}-(?:0[1-9]|1[0-2])", m) for m in months):
        raise ConsumerGovWindowError("window has malformed month identifiers")
    if len(set(months)) != len(months) or months != sorted(months):
        raise ConsumerGovWindowError("window months are duplicated or unordered")
    anchor = date.fromisoformat(months[0] + "-01")
    expected = [
        f"{anchor.year + (anchor.month - 1 + n) // 12:04d}-"
        f"{(anchor.month - 1 + n) % 12 + 1:02d}"
        for n in range(required)
    ]
    if months != expected:
        raise ConsumerGovWindowError("window has missing or nonconsecutive months")
    return months


def _object(
    root: Path, relative: str, expected_hash: str, pattern: str
) -> Path:
    if not re.fullmatch(r"[0-9a-f]{64}", expected_hash):
        raise ConsumerGovWindowError("missing or malformed object digest")
    if not re.fullmatch(pattern, relative):
        raise ConsumerGovWindowError(f"unsafe Consumer.gov object path: {relative!r}")
    path = root / relative
    if not path.is_file() or _sha256(path) != expected_hash:
        raise ConsumerGovWindowError(
            f"Consumer.gov object digest mismatch: {relative}"
        )
    return path


def _entry_counts(root: dict[str, Any], *, month: str) -> Counter[str]:
    entries = (
        root.get("by_name_key_raw")
        or root.get("by_name_key")
        or root.get("by_name")
        or {}
    )
    if not isinstance(entries, dict) or not entries:
        raise ConsumerGovWindowError(f"{month}: no monthly providers")
    counts: Counter[str] = Counter()
    for provider, entry in entries.items():
        if not isinstance(entry, dict) or not isinstance(entry.get("statistics"), dict):
            raise ConsumerGovWindowError(f"{month}: malformed provider statistics")
        stats = entry["statistics"]
        value = stats.get("complaintsCount", stats.get("total_claims"))
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ConsumerGovWindowError(f"{month}: invalid complaints for {provider}")
        counts[str(provider)] += value
    return counts


def load_active_window(
    derived_dir: Path,
    monthly_dir: Path,
    *,
    required: int = 12,
) -> ActiveConsumerWindow:
    """Verify the selected aggregate and monthly objects before returning rows."""
    sidecar = derived_dir / WINDOW_MANIFEST
    manifest: dict[str, Any] | None = None
    provenance = "pinned_legacy_embedded_months"

    if sidecar.exists():
        try:
            manifest = json.loads(sidecar.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, ValueError) as exc:
            raise ConsumerGovWindowError("active window manifest is unreadable") from exc
        if not isinstance(manifest, dict) or (
            manifest.get("artifact") != "consumer_gov_validated_window"
            or manifest.get("version") != WINDOW_MANIFEST_VERSION
            or manifest.get("state") != "complete"
            or manifest.get("source_family") != SOURCE_FAMILY
            or manifest.get("reference_event") != "abertura"
        ):
            raise ConsumerGovWindowError(
                "active window manifest has incompatible contract"
            )
        record = manifest.get("aggregate")
        if not isinstance(record, dict):
            raise ConsumerGovWindowError("active window aggregate is undeclared")
        aggregate_path = _object(
            derived_dir,
            str(record.get("path") or ""),
            str(record.get("sha256") or ""),
            r"objects/[0-9a-f]{64}\.json\.gz",
        )
        provenance = "atomic_content_manifest"
    else:
        # Historic versioned files are covered by the Git HEAD pristine
        # bootstrap check and the existing content-hashed V2 cache manifest.
        aggregate_path = derived_dir / "consumidor_gov_agg.json.gz"

    aggregate = _read_gz(aggregate_path)
    meta = aggregate.get("meta")
    if not isinstance(meta, dict) or meta.get("status") != "ok" or (
        meta.get("invalid_months")
    ):
        raise ConsumerGovWindowError("aggregate is partial or has invalid months")
    if (meta.get("source_family") not in (None, SOURCE_FAMILY)
        or meta.get("reference_event") not in (None, "abertura")):
        raise ConsumerGovWindowError("aggregate source family is incompatible")
    months = _selected_months(meta.get("months"), required)
    if manifest is not None:
        if manifest.get("selected_months") != months:
            raise ConsumerGovWindowError("manifest and aggregate windows differ")
        rows = manifest.get("months")
        if not isinstance(rows, list) or len(rows) != required:
            raise ConsumerGovWindowError("manifest monthly file set is incomplete")
        if [item.get("month") for item in rows if isinstance(item, dict)] != months:
            raise ConsumerGovWindowError("manifest monthly records are misordered")
    else:
        rows = [None] * required

    selected: list[tuple[str, Path, dict[str, Any]]] = []
    month_counts: Counter[str] = Counter()
    for month, record in zip(months, rows, strict=True):
        if manifest is None:
            path = monthly_dir / f"consumidor_gov_{month}.json.gz"
        else:
            if not isinstance(record, dict):
                raise ConsumerGovWindowError("invalid monthly manifest entry")
            digest = str(record.get("sha256") or "")
            path = _object(
                derived_dir,
                str(record.get("path") or ""),
                digest,
                rf"monthly/objects/consumidor_gov_{month}-[0-9a-f]{{64}}\.json\.gz",
            )
            if path.name != f"consumidor_gov_{month}-{digest}.json.gz":
                raise ConsumerGovWindowError("monthly file name does not match digest")
        monthly = _read_gz(path)
        monthly_meta = monthly.get("meta")
        if not isinstance(monthly_meta, dict) or (
            monthly_meta.get("month", monthly_meta.get("ym")) != month
            or monthly_meta.get("status") not in (None, "ok")
            or monthly_meta.get("invalid") is True
        ):
            raise ConsumerGovWindowError(f"{month}: invalid monthly metadata")
        month_counts.update(_entry_counts(monthly, month=month))
        selected.append((month, path, monthly))

    aggregate_counts = _entry_counts(aggregate, month="aggregate")
    if month_counts != aggregate_counts:
        raise ConsumerGovWindowError(
            "aggregate/monthly provider complaints do not reconcile"
        )
    return ActiveConsumerWindow(
        aggregate_path=aggregate_path,
        aggregate=aggregate,
        months=selected,
        provenance=provenance,
    )
