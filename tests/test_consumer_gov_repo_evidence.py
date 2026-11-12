"""Offline parity checks against Consumer.gov snapshots committed to this repo.

No official network access, downloads, cache rewriting or Full Generation.
Historical derived evidence is not a substitute for the original CSV.
"""

from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path

from api import build_consumidor_gov as build
from api.sources.consumidor_gov_agg import extract_consumidor_gov_aggregated
from api.v2.build_conduct_source_snapshot import _consumer_manifest_payload
from api.v2.build_consumer_gov_157_experiment import _load_latest_months
from api.v2.compare_consumer_gov_sources import _canonical_entry_payload
from api.v2.consumer_gov_window import load_active_window

REPO = Path(__file__).resolve().parents[1]
DERIVED = REPO / "data/derived/consumidor_gov"
MONTHLY = DERIVED / "monthly"
MONTHS = [f"2025-{m:02d}" for m in range(7, 13)] + [
    f"2026-{m:02d}" for m in range(1, 7)
]


def _read(path: Path) -> dict:
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        return json.load(stream)


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _original_main_selector() -> list[str]:
    """Pre-change selector reproduced without importing modified code."""
    valid = {}
    for path in sorted(MONTHLY.glob("consumidor_gov_20??-??.json.gz")):
        try:
            root = _read(path)
        except (OSError, UnicodeError, ValueError):
            continue
        meta = root.get("meta") if isinstance(root.get("meta"), dict) else {}
        if meta.get("invalid") is True or str(meta.get("status") or "").lower() == "invalid":
            continue
        month = str(meta.get("month") or meta.get("ym") or "")
        entries = root.get("by_name_key_raw") or root.get("by_name_key") or root.get("by_name") or {}
        if month and entries:
            valid[month] = path
    return sorted(valid)[-12:]


def test_repository_window_matches_original_selector_and_complete_manifest() -> None:
    assert len(list(MONTHLY.glob("consumidor_gov_20??-??.json.gz"))) == 18
    expected = _original_main_selector()
    assert expected == MONTHS
    active = load_active_window(DERIVED, MONTHLY)
    assert active.provenance == "pinned_legacy_embedded_months"
    assert [m for m, _, _ in active.months] == expected
    assert [p.name for _, p, _ in active.months] == [
        f"consumidor_gov_{month}.json.gz" for month in expected
    ]
    assert active.aggregate_path == DERIVED / "consumidor_gov_agg.json.gz"


def test_repository_monthly_vs_aggregate_all_provider_metrics() -> None:
    active = load_active_window(DERIVED, MONTHLY)
    reconstructed = build._merge_months(
        [root for _, _, root in active.months]
    )
    expected = _canonical_entry_payload(active.aggregate)
    observed = _canonical_entry_payload(reconstructed)
    names = sorted(set(expected) | set(observed))
    mismatches = [
        (name, expected.get(name), observed.get(name))
        for name in names if expected.get(name) != observed.get(name)
    ]
    assert not mismatches, (
        f"Historical aggregate differs from 12 versioned months for "
        f"{len(mismatches)}/{len(names)} providers; first={mismatches[:3]}"
    )
    assert len(expected) == len(observed)
    assert active.aggregate["meta"]["companies"] == len(expected)


def test_repo_v1_v2_and_identity_read_exact_same_versioned_evidence() -> None:
    active = load_active_window(DERIVED, MONTHLY)
    frozen_paths = {m: p for m, p, _ in active.months}
    manifest = _consumer_manifest_payload()
    assert manifest["aggregate"]["sha256"] == _sha(active.aggregate_path)
    assert [(r["month"], r["sha256"]) for r in manifest["months"]] == [
        (m, _sha(p)) for m, p, _ in active.months
    ]
    v1_meta, v1_payload = extract_consumidor_gov_aggregated(DERIVED)
    assert v1_meta.get("status") == "ok"
    assert _canonical_entry_payload(v1_payload) == _canonical_entry_payload(active.aggregate)
    identity_selection = _load_latest_months()
    assert [m for m, _, _ in identity_selection] == MONTHS
    assert {m: p.resolve() for m, p, _ in identity_selection} == {
        m: p.resolve() for m, p in frozen_paths.items()
    }
    for (month, path, root), (_, active_path, selected) in zip(
        identity_selection, active.months, strict=True
    ):
        assert path.resolve() == active_path.resolve()
        assert _canonical_entry_payload(root) == _canonical_entry_payload(selected)


def test_reconstructed_supplier_keys_and_display_names_match_pinned_aggregate() -> None:
    active = load_active_window(DERIVED, MONTHLY)
    reconstructed = build._merge_months(
        [root for _, _, root in active.months]
    )
    pinned_entries = build._entries_from_root(active.aggregate)
    recomputed_entries = build._entries_from_root(reconstructed)
    assert set(pinned_entries) == set(recomputed_entries)
    discrepancies = []
    for key in sorted(pinned_entries):
        pinned = pinned_entries[key]
        computed = recomputed_entries[key]
        for field in ("name", "display_name", "cnpj"):
            if (pinned.get(field) or "") != (computed.get(field) or ""):
                discrepancies.append(
                    (key, field, pinned.get(field), computed.get(field))
                )
    assert not discrepancies, (
        f"Persisted supplier labels differ from monthly recomposition: "
        f"count={len(discrepancies)}; first={discrepancies[:6]}"
    )
