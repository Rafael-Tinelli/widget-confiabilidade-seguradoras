import hashlib
import json
from pathlib import Path

import pytest

from api.v2.build_conduct_source_snapshot import (
    CONSUMER_MANIFEST_VERSION,
    CONSUMER_SOURCE_URL,
    ConductSourceSnapshotError,
    _cache_consumer_manifest,
    _cache_matches_current_inputs,
    _manifest_content_key,
    _materialize_consumer_manifest,
    _validate_consumer_manifest,
)
from api.v2.build_consumer_gov_receita_identity import CACHE_KEY_VERSION


def test_consumer_manifest_content_key_ignores_provenance_timestamp() -> None:
    first = {
        "fetched_at": "2026-08-01T00:00:00Z",
        "aggregate": {"sha256": "a" * 64},
        "months": [
            {"month": "2026-07", "sha256": "b" * 64},
            {"month": "2026-08", "sha256": "c" * 64},
        ],
    }
    second = {**first, "fetched_at": "2026-08-30T00:00:00Z"}
    changed = {
        **first,
        "months": [
            {"month": "2026-07", "sha256": "b" * 64},
            {"month": "2026-08", "sha256": "d" * 64},
        ],
    }

    assert _manifest_content_key(first) == _manifest_content_key(second)
    assert _manifest_content_key(first) != _manifest_content_key(changed)


def _materialized_fixture(tmp_path: Path) -> dict:
    work = tmp_path / "work"
    aggregate = work / "consumidor_gov_agg.json.gz"
    aggregate.parent.mkdir(parents=True, exist_ok=True)
    aggregate.write_bytes(b"aggregate")
    months = []
    for index in range(12):
        path = work / "monthly" / f"consumidor_gov_2026-{index + 1:02d}.json.gz"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(f"month-{index}".encode())
        months.append(
            {
                "month": f"2026-{index + 1:02d}",
                "path": str(path),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
        )
    return {
        "artifact": "v2_consumer_gov_core_source_manifest",
        "version": CONSUMER_MANIFEST_VERSION,
        "aggregate": {
            "path": str(aggregate),
            "sha256": hashlib.sha256(aggregate.read_bytes()).hexdigest(),
        },
        "months": months,
    }


def test_consumer_cache_survives_fresh_workspace_materialization(tmp_path) -> None:
    materialized = _materialized_fixture(tmp_path)
    cache_dir = tmp_path / "cache"
    cached = _cache_consumer_manifest(
        materialized,
        cache_dir=cache_dir,
        fetched_at="2026-08-30T00:00:00Z",
    )
    manifest = cache_dir / "consumer_gov_core_manifest.json"
    manifest.write_text(json.dumps(cached), encoding="utf-8")
    validated = _validate_consumer_manifest(manifest)

    original_paths = [
        Path(materialized["aggregate"]["path"]),
        *(Path(item["path"]) for item in materialized["months"]),
    ]
    expected = {path: path.read_bytes() for path in original_paths}
    for path in original_paths:
        path.unlink()

    _materialize_consumer_manifest(validated)

    for path, content in expected.items():
        assert path.read_bytes() == content


def test_consumer_manifest_requires_fetched_at_version_and_valid_hashes(tmp_path) -> None:
    materialized = _materialized_fixture(tmp_path)
    cache_dir = tmp_path / "cache"
    payload = _cache_consumer_manifest(
        materialized,
        cache_dir=cache_dir,
        fetched_at="2026-08-30T00:00:00Z",
    )
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    assert _validate_consumer_manifest(manifest)["fetched_at"] == payload["fetched_at"]

    wrong_version = {**payload, "version": CONSUMER_MANIFEST_VERSION - 1}
    manifest.write_text(json.dumps(wrong_version), encoding="utf-8")
    with pytest.raises(ConductSourceSnapshotError, match="version"):
        _validate_consumer_manifest(manifest)

    payload.pop("fetched_at")
    manifest.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(ConductSourceSnapshotError, match="fetched_at"):
        _validate_consumer_manifest(manifest)


def test_receita_cache_reuse_requires_current_schema_universe_and_provider_hashes(
    tmp_path,
) -> None:
    path = tmp_path / "receita.json"
    key = {
        "version": CACHE_KEY_VERSION,
        "reference_period": "2026-08",
        "target_universe_hash": "a" * 64,
        "target_universe_hash_method": "sorted_normalized_cnpjs_v1",
        "unresolved_provider_hash": "b" * 64,
    }
    path.write_text(
        json.dumps({"meta": {"gate4_cache_key": key}}),
        encoding="utf-8",
    )

    assert _cache_matches_current_inputs(path, "a" * 64, "b" * 64)
    assert not _cache_matches_current_inputs(path, "c" * 64, "b" * 64)
    assert not _cache_matches_current_inputs(path, "a" * 64, "d" * 64)

    incompatible = {
        **key,
        "version": "v2-consumer-receita-identity-0",
    }
    path.write_text(
        json.dumps({"meta": {"gate4_cache_key": incompatible}}),
        encoding="utf-8",
    )
    assert not _cache_matches_current_inputs(path, "a" * 64, "b" * 64)


def test_consumer_gate4_source_url_is_direct_official_publication_page() -> None:
    assert CONSUMER_SOURCE_URL == (
        "https://www.consumidor.gov.br/pages/dadosabertos/externo/"
    )
    assert "dados.mj.gov.br" not in CONSUMER_SOURCE_URL


def test_fallback_reselects_exact_v2_snapshot_instead_of_newer_sidecar(
    tmp_path: Path, monkeypatch
) -> None:
    import gzip

    from api import build_consumidor_gov as build
    from api.v2.consumer_gov_window import load_active_window

    derived = tmp_path / "derived"
    monthly_dir = derived / "monthly"
    monkeypatch.setattr(build, "DERIVED_DIR", derived)
    monkeypatch.setattr(build, "MONTHLY_DIR", monthly_dir)
    monkeypatch.setattr(build, "MONTHS_BACK", 12)

    months = [f"2026-{m:02d}" for m in range(1, 13)]

    def root(month: str, complaints: int) -> dict:
        return {
            "meta": {
                "month": month,
                "status": "ok",
                "companies": 1,
                "lines_total": complaints,
                "lines_kept": complaints,
                "source_family": "base_completa",
                "reference_event": "abertura",
                "source_sha256": ("a" if complaints == 1 else "b") * 64,
            },
            "by_name_key_raw": {
                "empresa": {
                    "name": "Empresa",
                    "statistics": {"complaintsCount": complaints},
                }
            },
        }

    old_roots = [root(month, 1) for month in months]
    old_aggregate = build._merge_months(old_roots)
    aggregate_path = derived / "consumidor_gov_agg.json.gz"
    aggregate_path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(aggregate_path, "wt", encoding="utf-8") as handle:
        json.dump(old_aggregate, handle)

    materialized = {
        "aggregate": {
            "path": str(aggregate_path),
            "sha256": hashlib.sha256(aggregate_path.read_bytes()).hexdigest(),
        },
        "months": [],
    }
    for month, month_root in zip(months, old_roots, strict=True):
        month_path = monthly_dir / f"consumidor_gov_{month}.json.gz"
        month_path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(month_path, "wt", encoding="utf-8") as handle:
            json.dump(month_root, handle)
        materialized["months"].append({
            "month": month,
            "path": str(month_path),
            "sha256": hashlib.sha256(month_path.read_bytes()).hexdigest(),
        })

    cached = _cache_consumer_manifest(
        materialized,
        cache_dir=tmp_path / "cache",
        fetched_at="2026-11-09T00:00:00Z",
    )
    # Simulate a newer candidate selected before a downstream Gate 4 failure.
    new_roots = [root(month, 2) for month in months]
    build._publish_validated_window(new_roots, build._merge_months(new_roots))
    before = load_active_window(derived, monthly_dir)
    assert before.aggregate["by_name_key_raw"]["empresa"]["statistics"]["complaintsCount"] == 24

    _materialize_consumer_manifest(cached)

    after = load_active_window(derived, monthly_dir)
    assert after.aggregate["by_name_key_raw"]["empresa"]["statistics"]["complaintsCount"] == 12
    assert [m for m, _, _ in after.months] == months
    assert all(
        r["by_name_key_raw"]["empresa"]["statistics"]["complaintsCount"] == 1
        for _, _, r in after.months
    )
    assert after.provenance == "atomic_content_manifest"
    # A restored V2 snapshot still has exactly the original hashes.
    assert all(
        hashlib.sha256(Path(row["path"]).read_bytes()).hexdigest() == row["sha256"]
        for row in [cached["aggregate"], *cached["months"]]
    )


def test_corrupt_v2_restore_never_switches_newer_active_manifest(
    tmp_path: Path, monkeypatch
) -> None:
    import gzip

    from api import build_consumidor_gov as build
    from api.v2.consumer_gov_window import load_active_window

    derived = tmp_path / "derived"
    monthly = derived / "monthly"
    monkeypatch.setattr(build, "DERIVED_DIR", derived)
    monkeypatch.setattr(build, "MONTHLY_DIR", monthly)
    monkeypatch.setattr(build, "MONTHS_BACK", 12)

    def entry(month: str) -> dict:
        return {
            "meta": {"month": month, "status": "ok", "companies": 1},
            "by_name_key_raw": {
                "empresa": {"name": "Empresa", "statistics": {"complaintsCount": 1}}
            },
        }

    months = [f"2026-{i:02d}" for i in range(1, 13)]
    roots = [entry(m) for m in months]
    build._publish_validated_window(roots, build._merge_months(roots))
    sidecar = derived / "consumidor_gov_window_manifest.json"
    before = sidecar.read_bytes()

    # The V2 cache has a valid self-hash, but semantically inconsistent data.
    # The active newer window must not be replaced by that bad snapshot.
    altered_aggregate = build._merge_months(roots)
    altered_aggregate["by_name_key_raw"]["empresa"]["statistics"]["complaintsCount"] = 13
    def store_gzip(path: Path, body: dict) -> str:
        path.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(path, "wt", encoding="utf-8") as file:
            json.dump(body, file)
        return hashlib.sha256(path.read_bytes()).hexdigest()

    cache_dir = tmp_path / "fake_cache"
    aggregate_path = cache_dir / "agg.json.gz"
    aggregate_sha = store_gzip(aggregate_path, altered_aggregate)
    rows = []
    for month, month_root in zip(months, roots, strict=True):
        path = cache_dir / f"{month}.json.gz"
        sha = store_gzip(path, month_root)
        rows.append({"month": month, "path": str(path),
                     "materialized_path": str(monthly / f"consumidor_gov_{month}.json.gz"),
                     "sha256": sha})
    payload = {
        "fetched_at": "2026-11-09T00:00:00Z",
        "aggregate": {
            "path": str(aggregate_path),
            "materialized_path": str(derived / "consumidor_gov_agg.json.gz"),
            "sha256": aggregate_sha,
        },
        "months": rows,
    }
    with pytest.raises(ConductSourceSnapshotError, match="inconsistent"):
        _materialize_consumer_manifest(payload)
    # A failed fallback preflight must leave both the active selector and
    # traditional materialized paths untouched.
    assert sidecar.read_bytes() == before
    assert not (derived / "consumidor_gov_agg.json.gz").exists()
    assert not (monthly / "consumidor_gov_2026-01.json.gz").exists()
    assert len(load_active_window(derived, monthly).months) == 12
