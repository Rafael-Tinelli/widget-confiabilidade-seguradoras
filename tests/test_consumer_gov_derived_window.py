from __future__ import annotations

import json
from pathlib import Path

import pytest

import api.build_consumidor_gov as build
from api.v2.consumer_gov_window import (
    ConsumerGovWindowError,
    load_active_window,
)

MONTHS = ["2026-01", "2026-02", "2026-03"]


def _root(month: str, *, source_digest: str = "a" * 64) -> dict:
    return {
        "meta": {
            "status": "ok",
            "month": month,
            "companies": 2,
            "lines_total": 2,
            "lines_kept": 2,
            "source_sha256": source_digest,
            "source_family": "base_completa",
            "reference_event": "abertura",
        },
        "by_name_key_raw": {
            "empresa a": {
                "name": "Empresa A",
                "statistics": {"complaintsCount": 1},
            },
            "empresa b": {
                "name": "Empresa B",
                "statistics": {"complaintsCount": 1},
            },
        },
    }


def _configure(monkeypatch, tmp_path: Path) -> tuple[Path, Path]:
    derived = tmp_path / "derived"
    monthly = derived / "monthly"
    derived.mkdir()
    monthly.mkdir()
    monkeypatch.setattr(build, "DERIVED_DIR", derived)
    monkeypatch.setattr(build, "MONTHLY_DIR", monthly)
    monkeypatch.setattr(build, "MONTHS_BACK", 3)
    return derived, monthly


def _publish(roots: list[dict]) -> tuple[dict, dict]:
    aggregate = build._merge_months(roots)
    return build._publish_validated_window(roots, aggregate), aggregate


def test_atomic_window_selects_only_declared_three_months(tmp_path, monkeypatch):
    derived, monthly = _configure(monkeypatch, tmp_path)
    manifest, aggregate = _publish([_root(month) for month in MONTHS])
    active = load_active_window(derived, monthly, required=3)
    assert [month for month, _, _ in active.months] == MONTHS
    assert active.aggregate == aggregate
    assert manifest["source_family"] == "base_completa"
    assert manifest["reference_event"] == "abertura"
    assert active.provenance == "atomic_content_manifest"

    # Orphan file from a later update may not be selected as newest month.
    (monthly / "consumidor_gov_2026-04.json.gz").write_bytes(b"untrusted")
    again = load_active_window(derived, monthly, required=3)
    assert [month for month, _, _ in again.months] == MONTHS


def test_missing_month_does_not_replace_active_manifest(tmp_path, monkeypatch):
    derived, monthly = _configure(monkeypatch, tmp_path)
    _publish([_root(month) for month in MONTHS])
    manifest_path = derived / "consumidor_gov_window_manifest.json"
    before = manifest_path.read_bytes()
    with pytest.raises(ValueError, match="incomplete"):
        _publish([_root(month) for month in MONTHS[:2]])
    assert manifest_path.read_bytes() == before
    assert len(load_active_window(derived, monthly, required=3).months) == 3


def test_interrupted_immutable_object_creation_preserves_active_window(
    tmp_path, monkeypatch
):
    derived, monthly = _configure(monkeypatch, tmp_path)
    _publish([_root(month) for month in MONTHS])
    manifest_path = derived / "consumidor_gov_window_manifest.json"
    original = manifest_path.read_bytes()
    writer = build._write_immutable_gzip
    attempts = []

    def interrupted(root, path):
        attempts.append(path)
        if len(attempts) == 2:
            raise OSError("interrupted during staging")
        return writer(root, path)

    monkeypatch.setattr(build, "_write_immutable_gzip", interrupted)
    changed = [
        _root(month, source_digest="b" * 64) for month in MONTHS
    ]
    with pytest.raises(OSError, match="interrupted"):
        _publish(changed)
    assert manifest_path.read_bytes() == original
    assert len(load_active_window(derived, monthly, required=3).months) == 3


def test_same_size_monthly_object_tamper_detected(tmp_path, monkeypatch):
    derived, monthly = _configure(monkeypatch, tmp_path)
    _publish([_root(month) for month in MONTHS])
    active = load_active_window(derived, monthly, required=3)
    path = active.months[0][1]
    data = path.read_bytes()
    path.write_bytes(bytes([data[0] ^ 1]) + data[1:])
    assert path.stat().st_size == len(data)
    with pytest.raises(ConsumerGovWindowError, match="digest mismatch"):
        load_active_window(derived, monthly, required=3)


def test_same_global_total_but_wrong_provider_fails_before_activation(
    tmp_path, monkeypatch
):
    derived, _ = _configure(monkeypatch, tmp_path)
    roots = [_root(month) for month in MONTHS]
    aggregate = build._merge_months(roots)
    aggregate["by_name_key_raw"]["empresa a"]["statistics"]["complaintsCount"] = 4
    aggregate["by_name_key_raw"]["empresa b"]["statistics"]["complaintsCount"] = 2
    with pytest.raises(ValueError, match="provider counts"):
        build._publish_validated_window(roots, aggregate)
    assert not (derived / "consumidor_gov_window_manifest.json").exists()


def test_family_mismatch_rejected_even_with_valid_hashes(tmp_path, monkeypatch):
    derived, monthly = _configure(monkeypatch, tmp_path)
    _publish([_root(month) for month in MONTHS])
    manifest_path = derived / "consumidor_gov_window_manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["source_family"] = "finalizadas"
    manifest_path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ConsumerGovWindowError, match="incompatible contract"):
        load_active_window(derived, monthly, required=3)


def test_nonconsecutive_months_never_become_active(tmp_path, monkeypatch):
    derived, _ = _configure(monkeypatch, tmp_path)
    wrong = ["2026-01", "2026-03", "2026-04"]
    with pytest.raises(ConsumerGovWindowError, match="nonconsecutive"):
        _publish([_root(month) for month in wrong])
    assert not (derived / "consumidor_gov_window_manifest.json").exists()


def test_unchanged_source_hashes_are_idempotent(tmp_path, monkeypatch):
    derived, monthly = _configure(monkeypatch, tmp_path)
    first, _ = _publish([_root(month) for month in MONTHS])
    before = (derived / "consumidor_gov_window_manifest.json").read_bytes()
    changed_timestamps = [_root(month) for month in MONTHS]
    for root in changed_timestamps:
        root["meta"]["generated_at"] = "some newly generated timestamp"
    second, _ = _publish(changed_timestamps)
    assert first == second
    assert (derived / "consumidor_gov_window_manifest.json").read_bytes() == before
    assert len(load_active_window(derived, monthly, required=3).months) == 3


def test_negative_monthly_counts_never_get_published(tmp_path, monkeypatch):
    derived, _ = _configure(monkeypatch, tmp_path)
    bad = [_root(month) for month in MONTHS]
    bad[1]["by_name_key_raw"]["empresa a"]["statistics"]["complaintsCount"] = -1
    with pytest.raises(ValueError, match="invalid"):
        _publish(bad)
    assert not (derived / "consumidor_gov_window_manifest.json").exists()


def test_embedded_month_manifest_ignores_newer_unreferenced_legacy_file(
    tmp_path, monkeypatch
):
    import gzip

    derived, monthly = _configure(monkeypatch, tmp_path)
    roots = [_root(month) for month in MONTHS]
    aggregate = build._merge_months(roots)
    for month, root in zip(MONTHS, roots, strict=True):
        path = monthly / f"consumidor_gov_{month}.json.gz"
        with gzip.open(path, "wt", encoding="utf-8") as handle:
            json.dump(root, handle)
    with gzip.open(
        derived / "consumidor_gov_agg.json.gz", "wt", encoding="utf-8"
    ) as handle:
        json.dump(aggregate, handle)
    (monthly / "consumidor_gov_2026-04.json.gz").write_bytes(b"orphan")
    selected = load_active_window(derived, monthly, required=3)
    assert selected.provenance == "pinned_legacy_embedded_months"
    assert [month for month, _, _ in selected.months] == MONTHS


def test_aggregate_modified_without_hash_update_fails_closed(tmp_path, monkeypatch):
    derived, monthly = _configure(monkeypatch, tmp_path)
    _publish([_root(month) for month in MONTHS])
    active = load_active_window(derived, monthly, required=3)
    aggregate_path = active.aggregate_path
    aggregate_path.write_bytes(b"broken previous aggregate")
    with pytest.raises(ConsumerGovWindowError, match="digest mismatch"):
        load_active_window(derived, monthly, required=3)


def test_versioned_real_baseline_has_complete_consistent_provider_window() -> None:
    """Protect the pinned Consumer.gov production bootstrap from regressions."""
    derived = Path("data/derived/consumidor_gov")
    monthly = derived / "monthly"
    assert (derived / "consumidor_gov_agg.json.gz").is_file()
    active = load_active_window(derived, monthly)
    assert active.provenance == "pinned_legacy_embedded_months"
    assert len(active.months) == 12
    assert all(path.is_file() for _, path, _ in active.months)


def test_same_source_hash_with_changed_derived_counters_requires_review(
    tmp_path, monkeypatch
) -> None:
    derived, monthly = _configure(monkeypatch, tmp_path)
    _publish([_root(month) for month in MONTHS])
    manifest = derived / "consumidor_gov_window_manifest.json"
    before = manifest.read_bytes()

    roots = [_root(month) for month in MONTHS]
    row = roots[0]["by_name_key_raw"]["empresa a"]["statistics"]
    row["complaintsCount"] = 2
    roots[0]["meta"]["lines_kept"] = 3
    roots[0]["meta"]["lines_total"] = 3
    aggregate = build._merge_months(roots)

    # The source SHA-256 has deliberately NOT changed.
    with pytest.raises(ValueError, match="derived semantics drift"):
        build._publish_validated_window(roots, aggregate)
    assert manifest.read_bytes() == before
    assert len(load_active_window(derived, monthly, required=3).months) == 3
