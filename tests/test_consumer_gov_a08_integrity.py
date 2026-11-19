"""A08 regression: monthly/aggregate metadata and ALL counters fail closed.

Only tiny self-contained fixtures. No official acquisition, Full Generation,
HostGator publication or modification of the pinned 2025-07..2026-06 corpus.
"""
from __future__ import annotations

import copy
import gzip
import json
from pathlib import Path

import pytest

from api import build_consumidor_gov as build
from api.v2.consumer_gov_conduct_core import (
    add_entry_statistics,
    aggregate_entry_to_month,
)
from api.v2.consumer_gov_window import (
    ConsumerGovWindowError,
    load_active_window,
)

MONTHS = ["2026-01", "2026-02", "2026-03"]


def _source(month: str) -> dict:
    entry = {
        "name": "Seguradora Demonstração",
        "statistics": {
            "complaintsCount": 5, "total_claims": 5,
            "respondedCount": 4, "responded_claims": 4,
            "finalizedCount": 3, "finalized_claims": 3,
            "resolvedCount": 2, "resolved_claims": 2,
            "satisfactionCount": 2, "scoreSum": 8.0,
            "averageScore": 4.0,
        },
    }
    return {
        "meta": {
            "month": month, "status": "ok", "companies": 1,
            "lines_kept": 5, "lines_total": 10,
            "source_family": "base_completa",
            "reference_event": "abertura",
        },
        "by_name_key_raw": {"seguradora demonstracao": entry},
    }


def _write_gz(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as out:
        json.dump(payload, out, ensure_ascii=False)


def _legacy_window(tmp_path: Path, *, corrupt=None) -> tuple[Path, Path]:
    derived = tmp_path / "derived"
    monthly = derived / "monthly"
    roots = [_source(month) for month in MONTHS]
    aggregate = build._merge_months(roots)
    if corrupt is not None:
        corrupt(roots, aggregate)
    _write_gz(derived / "consumidor_gov_agg.json.gz", aggregate)
    for month, root in zip(MONTHS, roots, strict=True):
        _write_gz(monthly / f"consumidor_gov_{month}.json.gz", root)
    return derived, monthly


def test_a08_valid_legacy_shape_and_satisfaction_semantics(tmp_path: Path) -> None:
    derived, monthly = _legacy_window(tmp_path)
    active = load_active_window(derived, monthly, required=3)
    assert active.provenance == "pinned_legacy_embedded_months"
    assert [m for m, _, _ in active.months] == MONTHS
    actual = active.aggregate["by_name_key_raw"]["seguradora demonstracao"]
    assert actual["statistics"]["scoreSum"] == 24.0
    assert actual["statistics"]["satisfactionCount"] == 6


@pytest.mark.parametrize("field,value", [
    ("complaintsCount", True),
    ("complaintsCount", 5.3),
    ("complaintsCount", "5"),
    ("respondedCount", "4"),
    ("respondedCount", -1),
    ("respondedCount", True),
    ("finalizedCount", 6),
    ("resolvedCount", 6),
    ("satisfactionCount", 6),
    ("scoreSum", "8.0"),
    ("scoreSum", float("nan")),
    ("scoreSum", float("inf")),
    ("scoreSum", -2.0),
    ("scoreSum", 30.0),
    ("averageScore", 1.0),
])
def test_a08_rejects_corrupt_monthly_metric_even_if_complaints_unchanged(
    tmp_path: Path, field: str, value: object,
) -> None:
    def corrupt(roots, _agg):
        stats = roots[1]["by_name_key_raw"]["seguradora demonstracao"]["statistics"]
        stats[field] = value
    derived, monthly = _legacy_window(tmp_path, corrupt=corrupt)
    with pytest.raises(ConsumerGovWindowError):
        load_active_window(derived, monthly, required=3)


@pytest.mark.parametrize("field", [
    "respondedCount", "finalizedCount", "resolvedCount",
    "satisfactionCount", "scoreSum",
])
def test_a08_missing_required_monthly_counter_is_not_zero(
    tmp_path: Path, field: str,
) -> None:
    def corrupt(roots, _agg):
        stats = roots[0]["by_name_key_raw"]["seguradora demonstracao"]["statistics"]
        del stats[field]
        aliases = {
            "respondedCount": "responded_claims",
            "finalizedCount": "finalized_claims",
            "resolvedCount": "resolved_claims",
        }
        if field in aliases:
            del stats[aliases[field]]
    derived, monthly = _legacy_window(tmp_path, corrupt=corrupt)
    with pytest.raises(ConsumerGovWindowError, match="missing required|scoreSum"):
        load_active_window(derived, monthly, required=3)


@pytest.mark.parametrize("field,value", [
    ("responded_claims", 3),
    ("resolved_claims", 1),
    ("total_claims", 4),
    ("finalized_claims", 2),
])
def test_a08_alias_disagreement_is_rejected(
    tmp_path: Path, field: str, value: int,
) -> None:
    def corrupt(roots, _agg):
        roots[0]["by_name_key_raw"]["seguradora demonstracao"]["statistics"][field] = value
    derived, monthly = _legacy_window(tmp_path, corrupt=corrupt)
    with pytest.raises(ConsumerGovWindowError, match="conflicting aliases"):
        load_active_window(derived, monthly, required=3)


@pytest.mark.parametrize("field,value", [
    ("companies", 2),
    ("companies", True),
    ("lines_kept", 7),
    ("lines_kept", "5"),
    ("lines_total", 2),
    ("lines_total", -1),
    ("month", "2026-07"),
    ("status", "partial"),
    ("source_family", "finalizadas"),
    ("reference_event", "finalizacao"),
    ("invalid", True),
])
def test_a08_invalid_month_metadata_rejected(
    tmp_path: Path, field: str, value: object,
) -> None:
    def corrupt(roots, _agg):
        roots[0]["meta"][field] = value
    derived, monthly = _legacy_window(tmp_path, corrupt=corrupt)
    with pytest.raises(ConsumerGovWindowError):
        load_active_window(derived, monthly, required=3)


@pytest.mark.parametrize("field,value", [
    ("respondedCount", 13),
    ("finalizedCount", 10),
    ("resolvedCount", 4),
    ("satisfactionCount", 7),
    ("scoreSum", 24.5),
])
def test_a08_same_complaint_totals_cannot_hide_aggregate_counter_mismatch(
    tmp_path: Path, field: str, value: object,
) -> None:
    def corrupt(_roots, aggregate):
        stats = aggregate["by_name_key_raw"]["seguradora demonstracao"]["statistics"]
        stats[field] = value
        alias = {
            "respondedCount": "responded_claims",
            "finalizedCount": "finalized_claims",
            "resolvedCount": "resolved_claims",
        }.get(field)
        if alias:
            stats[alias] = value
        if field in ("satisfactionCount", "scoreSum"):
            stats["averageScore"] = round(
                stats["scoreSum"] / stats["satisfactionCount"], 4
            )
        # A self-consistent but different aggregate must still fail on parity.
    derived, monthly = _legacy_window(tmp_path, corrupt=corrupt)
    with pytest.raises(
        ConsumerGovWindowError, match="mismatch|inconsistent|integer 1..5 ratings"
    ):
        load_active_window(derived, monthly, required=3)


def test_a08_atomic_candidate_rejected_before_changing_active_selector(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    derived = tmp_path / "derived"
    monthly = derived / "monthly"
    derived.mkdir()
    monthly.mkdir()
    monkeypatch.setattr(build, "DERIVED_DIR", derived)
    monkeypatch.setattr(build, "MONTHLY_DIR", monthly)
    monkeypatch.setattr(build, "MONTHS_BACK", 3)

    original = [_source(m) for m in MONTHS]
    # _publish_validated_window performs the new full contract verification
    # on a temporary sidecar before its one atomic replace.
    base_agg = build._merge_months(original)
    build._publish_validated_window(original, base_agg)
    current = derived / "consumidor_gov_window_manifest.json"
    previous = current.read_bytes()

    altered = copy.deepcopy(original)
    altered[1]["by_name_key_raw"]["seguradora demonstracao"]["statistics"][
        "respondedCount"
    ] = 3
    agg = build._merge_months(original)
    with pytest.raises(ConsumerGovWindowError):
        build._publish_validated_window(altered, agg)
    assert current.read_bytes() == previous
    assert load_active_window(derived, monthly, required=3).aggregate == base_agg


@pytest.mark.parametrize("value", [True, 1.5, "2", -1])
def test_a08_conduct_core_never_casts_invalid_counters(
    value: object,
) -> None:
    entry = {"statistics": {
        "complaintsCount": 5,
        "respondedCount": value,
        "finalizedCount": 4,
        "resolvedCount": 2,
        "satisfactionCount": 1,
        "scoreSum": 4.0,
    }}
    with pytest.raises(RuntimeError, match="invalid respondedCount"):
        aggregate_entry_to_month(
            "2026-03", entry,
            matched_current_insurer_market_complaints=10,
        )
    with pytest.raises(RuntimeError, match="invalid respondedCount"):
        add_entry_statistics({}, entry)


def test_a08_conduct_core_rejects_inconsistent_aliases_and_nonfinite_scores() -> None:
    base = _source("2026-03")["by_name_key_raw"]["seguradora demonstracao"]
    for key, value in (("responded_claims", 3), ("scoreSum", float("nan"))):
        entry = copy.deepcopy(base)
        entry["statistics"][key] = value
        with pytest.raises(RuntimeError):
            aggregate_entry_to_month(
                "2026-03", entry,
                matched_current_insurer_market_complaints=20,
            )
