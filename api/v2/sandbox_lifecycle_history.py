from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path
from typing import Any

from api.utils.identifiers import normalize_cnpj_v2

TRACKED_FIELDS = (
    "regulatory_status",
    "authorization_start",
    "authorization_end",
    "authorization_extended",
    "modalities",
    "edition",
)


class SandboxLifecycleHistoryError(ValueError):
    """Raised when the persistent Sandbox lifecycle ledger is malformed."""


def _event_id(cnpj: str, event: dict[str, Any]) -> str:
    material = json.dumps(
        {
            "cnpj": cnpj,
            "event_type": event.get("event_type"),
            "observed_at": event.get("observed_at"),
            "previous_value": event.get("previous_value"),
            "observed_value": event.get("observed_value"),
            "fip_code": event.get("fip_code"),
        },
        ensure_ascii=False,
        sort_keys=True,
        default=str,
    )
    digest = hashlib.sha256(material.encode("utf-8")).hexdigest()[:20]
    return f"sandbox-event:{digest}"


def _inferred_authorization_extended(row: dict[str, Any]) -> bool:
    """Repair legacy rows that predate the explicit extension boolean.

    The official raw status/end-date text is authoritative evidence for the
    extension signal. Older cached Sandbox rows may lack the explicit boolean;
    treating a missing field as False would manufacture a contradiction when
    the preserved raw text explicitly says the authorization was extended.
    """

    if row.get("authorization_extended") is True:
        return True
    raw = " ".join(
        str(row.get(field) or "")
        for field in ("raw_status", "authorization_end_raw")
    ).casefold()
    return bool(row.get("authorization_extended")) or "prorrogad" in raw


def _snapshot(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "legal_name": row.get("legal_name") or row.get("name"),
        "cnpj": normalize_cnpj_v2(row.get("cnpj")),
        "edition": row.get("edition"),
        "regulatory_status": row.get("regulatory_status"),
        "raw_status": row.get("raw_status"),
        "authorization_start": row.get("authorization_start"),
        "authorization_end": row.get("authorization_end"),
        "authorization_end_raw": row.get("authorization_end_raw"),
        "authorization_extended": _inferred_authorization_extended(row),
        "modalities": row.get("modalities"),
        "source": row.get("source"),
    }


def _index_rows(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for raw in rows:
        row = dict(raw)
        cnpj = normalize_cnpj_v2(row.get("cnpj"))
        if not cnpj:
            continue
        if cnpj in result and result[cnpj] != row:
            raise SandboxLifecycleHistoryError(
                f"duplicate Sandbox CNPJ with conflicting rows: {cnpj}"
            )
        result[cnpj] = row
    return result


def _licensed_index(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for raw in rows:
        row = dict(raw)
        if row.get("entity_type") != "insurer":
            continue
        cnpj = normalize_cnpj_v2(row.get("cnpj"))
        if cnpj:
            result[cnpj] = row
    return result


def _record_index(payload: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    if payload is None:
        return {}
    if payload.get("artifact") != "v2_sandbox_regulatory_history":
        raise SandboxLifecycleHistoryError("unexpected Sandbox lifecycle artifact")
    records = payload.get("records")
    if not isinstance(records, list):
        raise SandboxLifecycleHistoryError("Sandbox lifecycle records must be a list")
    result: dict[str, dict[str, Any]] = {}
    for raw in records:
        row = deepcopy(raw)
        cnpj = normalize_cnpj_v2(row.get("cnpj"))
        if not cnpj:
            raise SandboxLifecycleHistoryError("Sandbox lifecycle record without CNPJ")
        if cnpj in result:
            raise SandboxLifecycleHistoryError(
                f"duplicate Sandbox lifecycle CNPJ: {cnpj}"
            )
        row["cnpj"] = cnpj
        row["events"] = list(row.get("events") or [])
        last_snapshot = dict(row.get("last_snapshot") or {})
        if last_snapshot:
            last_snapshot["authorization_extended"] = (
                _inferred_authorization_extended(last_snapshot)
            )
            row["last_snapshot"] = last_snapshot
        result[cnpj] = row
    return result


def _append_event(
    record: dict[str, Any],
    *,
    observed_at: str,
    event_type: str,
    previous_value: Any = None,
    observed_value: Any = None,
    fip_code: Any = None,
) -> None:
    event = {
        "event_type": event_type,
        "observed_at": observed_at,
        "previous_value": previous_value,
        "observed_value": observed_value,
    }
    if fip_code:
        event["fip_code"] = str(fip_code)
    event["event_id"] = _event_id(str(record["cnpj"]), event)
    existing = {str(item.get("event_id") or "") for item in record.get("events") or []}
    if event["event_id"] not in existing:
        record.setdefault("events", []).append(event)


def _new_record(row: dict[str, Any], *, observed_at: str) -> dict[str, Any]:
    snap = _snapshot(row)
    cnpj = str(snap["cnpj"])
    record = {
        "cnpj": cnpj,
        "legal_name": snap.get("legal_name"),
        "editions": [snap["edition"]] if snap.get("edition") else [],
        "first_seen": observed_at,
        "last_seen_in_current_sandbox": observed_at,
        "current_sandbox_present": True,
        "current_outcome": None,
        "last_snapshot": snap,
        "ordinary_license": None,
        "events": [],
    }
    _append_event(
        record,
        observed_at=observed_at,
        event_type="sandbox_participant_first_observed",
        observed_value=snap.get("regulatory_status"),
    )
    return record


def seed_sandbox_lifecycle_history(
    rows: list[dict[str, Any]],
    *,
    observed_at: str,
) -> dict[str, Any]:
    records = [
        _new_record(row, observed_at=observed_at)
        for _, row in sorted(_index_rows(rows).items())
    ]
    for record in records:
        status = (record.get("last_snapshot") or {}).get("regulatory_status")
        record["current_outcome"] = (
            "sandbox_authorization_cancelled"
            if status == "sandbox_authorization_cancelled"
            else "active_sandbox"
        )
    return {
        "artifact": "v2_sandbox_regulatory_history",
        "version": 1,
        "updated_at": observed_at,
        "last_source_state": "seeded_from_validated_cache",
        "policy": {
            "history_is_append_only": True,
            "absence_from_current_sandbox_deletes_identity": False,
            "removal_without_transition_evidence_means_discontinued": False,
            "graduation_requires_exact_cnpj_current_license": True,
        },
        "records": records,
    }


def update_sandbox_lifecycle_history(
    existing: dict[str, Any] | None,
    *,
    current_rows: list[dict[str, Any]],
    current_licensed_rows: list[dict[str, Any]],
    observed_at: str,
    source_state: str,
) -> dict[str, Any]:
    """Update the append-only ledger from one trustworthy current-state observation."""

    by_cnpj = _record_index(existing)
    current = _index_rows(current_rows)
    licensed = _licensed_index(current_licensed_rows)

    if source_state == "fresh":
        for cnpj, row in sorted(current.items()):
            snap = _snapshot(row)
            record = by_cnpj.get(cnpj)
            if record is None:
                record = _new_record(row, observed_at=observed_at)
                by_cnpj[cnpj] = record
            else:
                before = dict(record.get("last_snapshot") or {})
                for field in TRACKED_FIELDS:
                    old_value = before.get(field)
                    new_value = snap.get(field)
                    if old_value != new_value:
                        _append_event(
                            record,
                            observed_at=observed_at,
                            event_type=f"sandbox_{field}_changed",
                            previous_value=old_value,
                            observed_value=new_value,
                        )
                record["last_snapshot"] = snap
                record["last_seen_in_current_sandbox"] = observed_at
                record["current_sandbox_present"] = True
                if snap.get("legal_name"):
                    record["legal_name"] = snap["legal_name"]
                editions = set(record.get("editions") or [])
                if snap.get("edition"):
                    editions.add(str(snap["edition"]))
                record["editions"] = sorted(editions)

            status = snap.get("regulatory_status")
            record["ordinary_license"] = None
            record["current_outcome"] = (
                "sandbox_authorization_cancelled"
                if status == "sandbox_authorization_cancelled"
                else "active_sandbox"
            )

        for cnpj, record in sorted(by_cnpj.items()):
            if cnpj in current:
                continue
            was_current = record.get("current_sandbox_present") is not False
            current_license = licensed.get(cnpj)
            record["current_sandbox_present"] = False
            if current_license is not None:
                record["current_outcome"] = "graduated_to_ordinary_license"
                record["ordinary_license"] = {
                    "fip_code": current_license.get("fip_code"),
                    "legal_name": current_license.get("legal_name"),
                    "entity_type": current_license.get("entity_type"),
                }
                if was_current:
                    _append_event(
                        record,
                        observed_at=observed_at,
                        event_type="sandbox_graduated_to_ordinary_license",
                        previous_value=(
                            (record.get("last_snapshot") or {}).get("regulatory_status")
                        ),
                        observed_value="active_licensed",
                        fip_code=current_license.get("fip_code"),
                    )
            else:
                record["current_outcome"] = "not_present_in_current_sandbox"
                record["ordinary_license"] = None
                if was_current:
                    _append_event(
                        record,
                        observed_at=observed_at,
                        event_type="sandbox_removed_from_current_source",
                        previous_value=(
                            (record.get("last_snapshot") or {}).get("regulatory_status")
                        ),
                        observed_value=None,
                    )

    return {
        "artifact": "v2_sandbox_regulatory_history",
        "version": 1,
        "updated_at": observed_at,
        "last_source_state": source_state,
        "policy": {
            "history_is_append_only": True,
            "absence_from_current_sandbox_deletes_identity": False,
            "removal_without_transition_evidence_means_discontinued": False,
            "graduation_requires_exact_cnpj_current_license": True,
        },
        "records": [
            {
                **record,
                "events": sorted(
                    record.get("events") or [],
                    key=lambda item: (
                        str(item.get("observed_at") or ""),
                        str(item.get("event_id") or ""),
                    ),
                ),
            }
            for _, record in sorted(by_cnpj.items())
        ],
    }


def write_sandbox_lifecycle_history(
    payload: dict[str, Any],
    path: Path,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temp.replace(path)
    return path
