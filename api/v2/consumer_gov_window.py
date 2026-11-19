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
import math
import re
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


# Canonical counters and legacy aliases are *the same reported measure*.
# Missing and malformed values must not be converted to zero. All fields
# below are emitted by the existing Base Completa monthly aggregator.
STAT_FIELDS: tuple[tuple[str, str | None], ...] = (
    ("complaintsCount", "total_claims"),
    ("respondedCount", "responded_claims"),
    ("finalizedCount", "finalized_claims"),
    ("resolvedCount", "resolved_claims"),
    ("satisfactionCount", None),
)


def _nonnegative_int(value: Any, *, label: str) -> int:
    if type(value) is not int or value < 0:
        raise ConsumerGovWindowError(f"{label}: invalid nonnegative integer")
    return value


def _metric(stats: dict[str, Any], key: str, alias: str | None, *, label: str) -> int:
    names = (key, alias) if alias else (key,)
    present = [name for name in names if name in stats]
    if not present:
        raise ConsumerGovWindowError(f"{label}: missing required {key}")
    values = [_nonnegative_int(stats[name], label=f"{label}.{name}") for name in present]
    if len(values) == 2 and values[0] != values[1]:
        raise ConsumerGovWindowError(f"{label}: conflicting aliases for {key}")
    return values[0]


def _entry_metrics(
    root: dict[str, Any], *, month: str, monthly: bool,
) -> dict[str, dict[str, int | float]]:
    entries = (
        root.get("by_name_key_raw")
        or root.get("by_name_key")
        or root.get("by_name")
        or {}
    )
    if not isinstance(entries, dict) or not entries:
        raise ConsumerGovWindowError(f"{month}: no monthly providers")
    meta = root.get("meta")
    if not isinstance(meta, dict):
        raise ConsumerGovWindowError(f"{month}: missing source metadata")
    if (meta.get("source_family") not in (None, SOURCE_FAMILY)
            or meta.get("reference_event") not in (None, "abertura")):
        raise ConsumerGovWindowError(f"{month}: incompatible source family or event")
    if meta.get("invalid") is True or meta.get("invalid_months"):
        raise ConsumerGovWindowError(f"{month}: source flagged invalid")
    # A legacy month is allowed not to DECLARE its temporal event. Absence
    # must not be interpreted as independent proof that it was opening-based.
    # A new content-addressed window states its event in the selector itself.
    metrics: dict[str, dict[str, int | float]] = {}
    lines_kept = 0
    for provider, entry in entries.items():
        if (not isinstance(provider, str) or not provider
                or not isinstance(entry, dict)
                or not isinstance(entry.get("statistics"), dict)):
            raise ConsumerGovWindowError(f"{month}: invalid provider entry")
        stats = entry["statistics"]
        label = f"{month}:{provider}"
        counts = {
            field: _metric(stats, field, alias, label=label)
            for field, alias in STAT_FIELDS
        }
        total = counts["complaintsCount"]
        if total <= 0:
            raise ConsumerGovWindowError(f"{label}: complaints must be positive")
        for field, number in counts.items():
            if number > total:
                raise ConsumerGovWindowError(
                    f"{label}: {field} exceeds complaints"
                )
        score = stats.get("scoreSum")
        if type(score) not in (float, int) or not math.isfinite(score) or score < 0:
            raise ConsumerGovWindowError(f"{label}: invalid scoreSum")
        if counts["satisfactionCount"] == 0 and score != 0:
            raise ConsumerGovWindowError(f"{label}: score without ratings")
        if (score < counts["satisfactionCount"]
                or score > 5 * counts["satisfactionCount"]
                or not math.isclose(score, round(score), rel_tol=0.0, abs_tol=1e-9)):
            raise ConsumerGovWindowError(
                f"{label}: scoreSum incompatible with integer 1..5 ratings"
            )
        reported_average = stats.get("averageScore")
        if reported_average is not None and (
            type(reported_average) not in (int, float)
            or not math.isfinite(reported_average)
            or abs(reported_average - round(
                score / counts["satisfactionCount"]
                if counts["satisfactionCount"] else 0.0, 4
            )) > 0.00011
        ):
            raise ConsumerGovWindowError(
                f"{label}: averageScore inconsistent with scoreSum"
            )
        counts["scoreSum"] = float(score)
        metrics[provider] = counts
        lines_kept += total

    if "companies" in meta and _nonnegative_int(
        meta["companies"], label=f"{month}.companies"
    ) != len(entries):
        raise ConsumerGovWindowError(f"{month}: companies metadata mismatch")
    if monthly:
        if meta.get("status") not in (None, "ok"):
            raise ConsumerGovWindowError(f"{month}: month status invalid")
        if "lines_kept" in meta and _nonnegative_int(
            meta["lines_kept"], label=f"{month}.lines_kept"
        ) != lines_kept:
            raise ConsumerGovWindowError(f"{month}: lines_kept mismatch")
        if "lines_total" in meta and _nonnegative_int(
            meta["lines_total"], label=f"{month}.lines_total"
        ) < lines_kept:
            raise ConsumerGovWindowError(f"{month}: lines_total below kept")
    elif meta.get("status") != "ok":
        raise ConsumerGovWindowError("aggregate status invalid")
    return metrics


def _reconcile_provider_metrics(
    monthly: list[tuple[str, dict[str, dict[str, int | float]]]],
    aggregate: dict[str, dict[str, int | float]],
) -> None:
    summed: dict[str, dict[str, int | float]] = {}
    for _, entries in monthly:
        for provider, stats in entries.items():
            target = summed.setdefault(
                provider, {key: 0 for key, _ in STAT_FIELDS} | {"scoreSum": 0.0}
            )
            for field in target:
                target[field] += stats[field]
    if set(summed) != set(aggregate):
        raise ConsumerGovWindowError(
            "aggregate/monthly provider population does not reconcile"
        )
    for provider, stats in summed.items():
        for field, number in stats.items():
            declared = aggregate[provider][field]
            if field == "scoreSum":
                if not math.isclose(
                    float(number), float(declared), rel_tol=1e-12, abs_tol=1e-7
                ):
                    raise ConsumerGovWindowError(
                        f"aggregate/monthly provider scoreSum mismatch: {provider}"
                    )
            elif number != declared:
                raise ConsumerGovWindowError(
                    f"aggregate/monthly provider {field} mismatch: {provider}"
                )



def load_active_window(
    derived_dir: Path,
    monthly_dir: Path,
    *,
    required: int = 12,
    manifest_path: Path | None = None,
) -> ActiveConsumerWindow:
    """Verify the selected aggregate and monthly objects before returning rows."""
    sidecar = manifest_path if manifest_path is not None else derived_dir / WINDOW_MANIFEST
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
    month_metrics: list[tuple[str, dict[str, dict[str, int | float]]]] = []
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
        validated = _entry_metrics(monthly, month=month, monthly=True)
        if manifest is not None:
            assert isinstance(record, dict)
            fields = (
                ("rows", "lines_total"),
                ("complaints", "lines_kept"),
            )
            for declared_key, source_key in fields:
                expected = (monthly_meta or {}).get(source_key)
                # A V2 cache restore may omit these optional sidecar
                # counters; its *objects* remain source-hashed and validated.
                # A claimed value must exactly match the source metadata.
                if declared_key in record and (
                    expected is None
                    or _nonnegative_int(
                        record[declared_key],
                        label=f"{month}.manifest.{declared_key}",
                    ) != expected
                ):
                    raise ConsumerGovWindowError(
                        f"{month}: manifest {declared_key} differs from source metadata"
                    )
            source_sha = (monthly_meta or {}).get("source_sha256")
            if ("source_sha256" in record
                    and (source_sha or record["source_sha256"] not in (None, ""))
                    and record["source_sha256"] != source_sha):
                raise ConsumerGovWindowError(
                    f"{month}: manifest/source SHA mismatch"
                )
        month_metrics.append((month, validated))
        selected.append((month, path, monthly))

    aggregate_metrics = _entry_metrics(aggregate, month="aggregate", monthly=False)
    _reconcile_provider_metrics(month_metrics, aggregate_metrics)
    return ActiveConsumerWindow(
        aggregate_path=aggregate_path,
        aggregate=aggregate,
        months=selected,
        provenance=provenance,
    )
