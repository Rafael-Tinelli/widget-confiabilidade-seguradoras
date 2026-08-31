from __future__ import annotations

import os
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

DEFAULT_SES_ZIP = Path(os.getenv("SES_CACHE_DIR", "data/raw/ses")) / "BaseCompleta.zip"
LOOKBACK_MONTHS = 12


class SesActivityEvidenceError(RuntimeError):
    """Raised when current SES activity evidence cannot be derived safely."""


@dataclass(frozen=True)
class ActivitySource:
    activity: str
    label: str
    member_suffixes: tuple[str, ...]
    required: bool = False


ACTIVITY_SOURCES: tuple[ActivitySource, ...] = (
    ActivitySource("insurance", "SEGUROS", ("ses_seguros.csv",), required=True),
    ActivitySource("pension", "PREVIDENCIA", ("contrib_benef.csv",)),
    ActivitySource("capitalization", "CAPITALIZACAO", ("ses_dados_cap.csv",)),
    ActivitySource("reinsurance", "RESSEGURO", ("ses_resseguro.csv", "resseguro.csv")),
)

_FIP_COLUMNS = ("coenti", "co_entidade", "codigofip", "codfip")
_PERIOD_COLUMNS = ("damesano", "competencia", "periodo")


def _canon_fip(value: Any) -> str:
    text = str(value or "").strip().removesuffix(".0")
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits.zfill(6) if digits else ""


def _period(value: Any) -> int | None:
    text = str(value or "").strip().replace("-", "").removesuffix(".0")
    if len(text) != 6 or not text.isdigit():
        return None
    year = int(text[:4])
    month = int(text[4:])
    if year < 2000 or not 1 <= month <= 12:
        return None
    return year * 100 + month


def _month_index(period: int) -> int:
    year = period // 100
    month = period % 100
    return year * 12 + month - 1


def _period_from_index(index: int) -> int:
    year, zero_based_month = divmod(index, 12)
    return year * 100 + zero_based_month + 1


def _pick_column(columns: list[str], candidates: tuple[str, ...]) -> str | None:
    normalized = {str(column).strip().casefold(): str(column) for column in columns}
    for candidate in candidates:
        if candidate in normalized:
            return normalized[candidate]
    return None


def _find_member(archive: zipfile.ZipFile, source: ActivitySource) -> str | None:
    members = sorted(archive.namelist())
    for suffix in source.member_suffixes:
        suffix_lower = suffix.casefold()
        member = next(
            (name for name in members if name.casefold().endswith(suffix_lower)),
            None,
        )
        if member:
            return member
    return None


def _scan_latest_period_by_fip(
    archive: zipfile.ZipFile,
    member: str,
) -> tuple[dict[str, int], int]:
    with archive.open(member) as handle:
        header = pd.read_csv(handle, sep=";", encoding="latin1", nrows=0)
    fip_column = _pick_column(list(header.columns), _FIP_COLUMNS)
    period_column = _pick_column(list(header.columns), _PERIOD_COLUMNS)
    if fip_column is None or period_column is None:
        raise SesActivityEvidenceError(
            f"SES activity member {member} lacks FIP/period columns"
        )

    latest_by_fip: dict[str, int] = {}
    latest_period = 0
    with archive.open(member) as handle:
        for chunk in pd.read_csv(
            handle,
            sep=";",
            encoding="latin1",
            dtype=str,
            usecols=[fip_column, period_column],
            chunksize=300_000,
            on_bad_lines="skip",
        ):
            for raw_fip, raw_period in zip(
                chunk[fip_column], chunk[period_column], strict=False
            ):
                fip = _canon_fip(raw_fip)
                period = _period(raw_period)
                if not fip or period is None:
                    continue
                if period > latest_by_fip.get(fip, 0):
                    latest_by_fip[fip] = period
                latest_period = max(latest_period, period)
    return latest_by_fip, latest_period


def derive_ses_activity_evidence(
    zip_path: Path | None = None,
) -> dict[str, Any]:
    """Derive recent SES data-flow activity from the already materialized BaseCompleta.

    Activity is evidence that a FIP has rows in the relevant SES component within the
    latest 12-month window observed across the available activity components. It is not
    a legal classification and never infers activity from names or corporate groups.
    """
    path = Path(zip_path or DEFAULT_SES_ZIP)
    if not path.is_file() or not zipfile.is_zipfile(path):
        raise SesActivityEvidenceError(f"SES BaseCompleta unavailable or invalid: {path}")

    scans: dict[str, dict[str, Any]] = {}
    global_reference = 0
    with zipfile.ZipFile(path) as archive:
        for source in ACTIVITY_SOURCES:
            member = _find_member(archive, source)
            if member is None:
                if source.required:
                    raise SesActivityEvidenceError(
                        f"required SES activity source missing: {source.member_suffixes[0]}"
                    )
                scans[source.activity] = {
                    "label": source.label,
                    "member": None,
                    "state": "not_published_in_snapshot",
                    "latest_by_fip": {},
                    "reference_period": None,
                }
                continue
            try:
                latest_by_fip, reference_period = _scan_latest_period_by_fip(
                    archive, member
                )
            except (OSError, ValueError, KeyError, pd.errors.ParserError) as exc:
                raise SesActivityEvidenceError(
                    f"cannot read SES activity source {member}: {exc}"
                ) from exc
            if source.required and not latest_by_fip:
                raise SesActivityEvidenceError(
                    f"required SES activity source has no valid FIP periods: {member}"
                )
            scans[source.activity] = {
                "label": source.label,
                "member": member,
                "state": "available",
                "latest_by_fip": latest_by_fip,
                "reference_period": reference_period or None,
            }
            global_reference = max(global_reference, reference_period)

    if not global_reference:
        raise SesActivityEvidenceError("SES activity sources have no valid reference period")

    threshold_index = _month_index(global_reference) - (LOOKBACK_MONTHS - 1)
    window_start = _period_from_index(threshold_index)
    by_fip: dict[str, dict[str, bool]] = {}
    labels_by_fip: dict[str, list[str]] = {}
    for source in ACTIVITY_SOURCES:
        scan = scans[source.activity]
        for fip, latest_period in scan["latest_by_fip"].items():
            if _month_index(latest_period) < threshold_index:
                continue
            activities = by_fip.setdefault(
                fip,
                {
                    "insurance": False,
                    "pension": False,
                    "capitalization": False,
                    "reinsurance": False,
                },
            )
            activities[source.activity] = True
            labels_by_fip.setdefault(fip, []).append(source.label)

    return {
        "source": "SUSEP SES / BaseCompleta.zip",
        "semantics": "recent_data_flow_row_presence_not_legal_classification",
        "reference_period": global_reference,
        "window_start": window_start,
        "lookback_months": LOOKBACK_MONTHS,
        "components": {
            activity: {
                "label": scan["label"],
                "member": scan["member"],
                "state": scan["state"],
                "reference_period": scan["reference_period"],
            }
            for activity, scan in scans.items()
        },
        "activities_by_fip": by_fip,
        "labels_by_fip": {
            fip: sorted(set(labels)) for fip, labels in labels_by_fip.items()
        },
    }


def enrich_entities_with_ses_activity_evidence(
    entities: list[dict[str, Any]],
    zip_path: Path | None = None,
) -> list[dict[str, Any]]:
    """Attach current SES activity evidence without downgrading existing evidence."""
    path = Path(zip_path or DEFAULT_SES_ZIP)
    if not path.exists():
        return entities

    payload = derive_ses_activity_evidence(path)
    activities_by_fip = payload["activities_by_fip"]
    labels_by_fip = payload["labels_by_fip"]
    for entity in entities:
        fip = _canon_fip(entity.get("fip_code") or entity.get("entity_id"))
        derived = activities_by_fip.get(fip) or {}
        current = dict(entity.get("activities") or {})
        for key in ("insurance", "pension", "capitalization", "reinsurance"):
            current[key] = bool(current.get(key)) or bool(derived.get(key))
        entity["activities"] = current

        evidence = dict(entity.get("evidence") or {})
        prior_labels = [
            str(value).strip().upper()
            for value in evidence.get("activity_sources") or []
            if str(value).strip()
        ]
        evidence["activity_sources"] = sorted(
            set(prior_labels) | set(labels_by_fip.get(fip) or [])
        )
        evidence["ses_recent_activity"] = {
            "source": payload["source"],
            "semantics": payload["semantics"],
            "reference_period": payload["reference_period"],
            "window_start": payload["window_start"],
            "lookback_months": payload["lookback_months"],
            "observed_labels": list(labels_by_fip.get(fip) or []),
        }
        entity["evidence"] = evidence
    return entities
