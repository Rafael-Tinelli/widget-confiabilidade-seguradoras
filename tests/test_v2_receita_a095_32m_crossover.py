"""A09.5 final isolated crossover: strict offline invariants and false-positive guards."""
from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import a095_32m_crossover_probe as probe
from scripts import a095_bounded_get_probe as get_probe


def _fake_manifest() -> tuple[SimpleNamespace, list[dict]]:
    names = [
        *(f"Estabelecimentos{i}.zip" for i in range(10)),
        *(f"Empresas{i}.zip" for i in range(10)),
        "Motivos.zip", "Cnaes.zip", "Naturezas.zip",
    ]
    records = [
        {
            "name": n, "size": 100_000_000,
            "etag": f'"etag-{n}"', "last_modified": "Mon, 16 Nov 2026 13:00:00 GMT",
            "is_collection": False,
        }
        for n in names
    ]
    release = SimpleNamespace(
        period="2026-11", reasons_file="Motivos.zip",
        release_url="https://official.example/2026-11/",
        establishment_files=tuple(f"Estabelecimentos{i}.zip" for i in range(10)),
        resource_metadata=tuple(
            {k: row[k] for k in ("name", "size", "etag", "last_modified")}
            for row in records if row["name"] == "Motivos.zip"
            or row["name"].startswith("Estabelecimentos")
        ),
    )
    return release, records


def _patch_manifest(monkeypatch: pytest.MonkeyPatch, records=None, release=None) -> None:
    actual_release, actual_rows = _fake_manifest()
    if records is None:
        records = actual_rows
    if release is None:
        release = actual_release

    class Session:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

    monkeypatch.setattr(probe, "discover_latest_release", lambda: release)
    monkeypatch.setattr(probe.requests, "Session", Session)
    monkeypatch.setattr(probe, "_configure_official_session", lambda _: None)
    monkeypatch.setattr(probe, "_propfind", lambda _s, _url: records)


def test_a095_full_2026_11_manifest_is_metadata_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_manifest(monkeypatch)
    result = probe._official_catalog()
    assert result["reference_period"] == "2026-11"
    assert result["status"] == "complete_metadata_only"
    assert result["full_zip_content_verified"] is False
    assert result["company_partitions"] == 10
    assert result["establishment_partitions"] == 10
    assert result["required_files"] == 23
    assert result["required_aggregate_bytes"] == 23 * 100_000_000
    assert len(result["resources"]) == 2


@pytest.mark.parametrize("removed", [
    "Empresas5.zip", "Estabelecimentos7.zip",
    "Motivos.zip", "Cnaes.zip", "Naturezas.zip",
])
def test_a095_catalog_rejects_missing_partition_or_dictionary(
    monkeypatch: pytest.MonkeyPatch, removed: str,
) -> None:
    release, rows = _fake_manifest()
    rows = [x for x in rows if x["name"] != removed]
    _patch_manifest(monkeypatch, records=rows, release=release)
    with pytest.raises(get_probe.GetProbeError):
        probe._official_catalog()


def test_a095_catalog_rejects_unreliable_size_or_fingerprint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release, rows = _fake_manifest()
    row = next(x for x in rows if x["name"] == "Empresas9.zip")
    row["size"] = None
    _patch_manifest(monkeypatch, records=rows, release=release)
    with pytest.raises(get_probe.GetProbeError, match="metadata_incomplete"):
        probe._official_catalog()


def test_a095_catalog_rejects_missing_change_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release, rows = _fake_manifest()
    row = next(x for x in rows if x["name"] == "Empresas9.zip")
    row["etag"] = None
    row["last_modified"] = None
    _patch_manifest(monkeypatch, records=rows, release=release)
    with pytest.raises(get_probe.GetProbeError, match="metadata_incomplete"):
        probe._official_catalog()


def test_a095_catalog_rejects_duplicate_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release, rows = _fake_manifest()
    rows.append(dict(rows[-1]))
    _patch_manifest(monkeypatch, records=rows, release=release)
    with pytest.raises(get_probe.GetProbeError, match="duplicate"):
        probe._official_catalog()


def test_a095_catalog_requires_new_2026_11_release(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release, rows = _fake_manifest()
    release.period = "2026-10"
    _patch_manifest(monkeypatch, records=rows, release=release)
    with pytest.raises(get_probe.GetProbeError, match="2026_11"):
        probe._official_catalog()


def _measurements() -> list[dict]:
    return [
        {
            "order": ["sequential", "parallel"],
            "measurements": [
                {
                    "mode": "sequential", "wall_seconds": 4.0,
                    "files": [{"elapsed_seconds": 2.0}, {"elapsed_seconds": 2.0}],
                },
                {
                    "mode": "parallel", "wall_seconds": 2.3,
                    "files": [{"elapsed_seconds": 2.3}, {"elapsed_seconds": 2.2}],
                },
            ],
        },
        {
            "order": ["parallel", "sequential"],
            "measurements": [
                {
                    "mode": "parallel", "wall_seconds": 2.5,
                    "files": [{"elapsed_seconds": 2.5}, {"elapsed_seconds": 2.4}],
                },
                {
                    "mode": "sequential", "wall_seconds": 4.3,
                    "files": [{"elapsed_seconds": 2.1}, {"elapsed_seconds": 2.2}],
                },
            ],
        },
    ]


def test_a095_both_orders_must_improve() -> None:
    rounds = _measurements()
    assert probe._decision(rounds)["classification"] == "promising_for_two_zip_prefetch_design"
    rounds[1]["measurements"][0]["wall_seconds"] = 4.5
    decision = probe._decision(rounds)
    assert decision["classification"] == "mixed_or_unfavorable_no_production_change"
    assert not decision["both_orders_gain_at_least_15_percent"]


def test_a095_rejects_large_per_file_degradation_even_if_total_faster() -> None:
    rounds = _measurements()
    rounds[0]["measurements"][1]["files"][0]["elapsed_seconds"] = 4.2
    decision = probe._decision(rounds)
    assert decision["classification"] == "mixed_or_unfavorable_no_production_change"
    assert not decision["no_individual_file_slowdown_above_2x"]


@pytest.mark.parametrize("corruption", ["digest", "etag", "size", "status"])
def test_a095_rejects_any_cross_round_content_or_identity_change(
    corruption: str,
) -> None:
    resources = [
        {"file_index": i, "etag": f'"etag{i}"'}
        for i in range(2)
    ]
    sample = [
        {
            "file_index": i,
            "read_bytes": probe.PREFIX_BYTES,
            "http_status": 200,
            "sha256_prefix": hex(i + 1)[2:] * 64,
            "etag": f'"etag{i}"',
        }
        for i in range(2)
    ]
    import copy

    rounds = [
        {"measurements": [
            {"files": copy.deepcopy(sample)},
            {"files": copy.deepcopy(sample)},
        ]},
        {"measurements": [
            {"files": copy.deepcopy(sample)},
            {"files": copy.deepcopy(sample)},
        ]},
    ]
    target = rounds[1]["measurements"][1]["files"][1]
    if corruption == "digest":
        target["sha256_prefix"] = "f" * 64
    elif corruption == "etag":
        target["etag"] = '"other"'
    elif corruption == "size":
        target["read_bytes"] = probe.PREFIX_BYTES - 1
    elif corruption == "status":
        target["http_status"] = 206
    with pytest.raises(get_probe.GetProbeError):
        probe._validate_bytes(rounds, resources)


def _stub_probe(monkeypatch: pytest.MonkeyPatch, flip_catalog=False, flip_digest=False):
    manifest = {
        "reference_period": "2026-11",
        "release_url": "https://official.example/2026-11/",
        "fingerprint": "a" * 64,
        "lifecycle_fingerprint": "b" * 64,
        "company_partitions": 10,
        "establishment_partitions": 10,
        "required_files": 23,
        "required_aggregate_bytes": 1_000_000_000,
        "full_zip_content_verified": False,
        "status": "complete_metadata_only",
        "resources": [
            {"url": f"https://official.example/{i}", "file_index": i,
             "size": 100_000_000, "etag": f'"etag{i}"'}
            for i in range(2)
        ],
    }
    count_catalog = 0
    def catalog():
        nonlocal count_catalog
        count_catalog += 1
        result = dict(manifest)
        if flip_catalog and count_catalog == 2:
            result["fingerprint"] = "c" * 64
        return result
    monkeypatch.setattr(probe, "_official_catalog", catalog)
    counts = {0: 0, 1: 0}
    lock = threading.Lock()
    current = 0
    maximum = 0
    def fetch(resource, *, read_limit):
        nonlocal current, maximum
        assert read_limit == probe.PREFIX_BYTES
        idx = resource["file_index"]
        with lock:
            counts[idx] += 1
            attempt = counts[idx]
            current += 1
            maximum = max(maximum, current)
        time.sleep(0.025)
        with lock:
            current -= 1
        digest = str(idx + 1) * 64
        if flip_digest and idx == 1 and attempt == 4:
            digest = "f" * 64
        return {
            "file_index": idx, "read_bytes": probe.PREFIX_BYTES,
            "http_status": 200, "sha256_prefix": digest,
            "etag": f'"etag{idx}"', "elapsed_seconds": 0.025,
        }
    monkeypatch.setattr(probe, "_get_prefix", fetch)
    return counts, lambda: maximum, lambda: count_catalog


def test_a095_32m_crossover_four_measurements_and_eight_gets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    counts, concurrent, catalog_calls = _stub_probe(monkeypatch)
    result = probe.run_crossover()
    assert result["status"] == "measured"
    assert result["experiment_orders"] == [
        ["sequential", "parallel"], ["parallel", "sequential"],
    ]
    assert result["total_requests"] == 8
    assert result["max_parallel_connections"] == 2
    assert result["prefix_mib_per_get"] == 32
    assert result["max_application_bytes"] == 8 * 32 * 1048576
    assert result["observed_application_bytes"] == 8 * 32 * 1048576
    assert result["official_catalog"]["full_zip_content_verified"] is False
    assert counts == {0: 4, 1: 4}
    assert concurrent() == 2
    assert catalog_calls() == 2
    assert result["decision"]["both_orders_gain_at_least_15_percent"]


@pytest.mark.parametrize("change", ["catalog", "digest"])
def test_a095_32m_crossover_fails_closed_for_changed_publication(
    monkeypatch: pytest.MonkeyPatch, change: str,
) -> None:
    _stub_probe(monkeypatch, flip_catalog=change=="catalog",
                flip_digest=change=="digest")
    with pytest.raises(get_probe.GetProbeError):
        probe.run_crossover()


def test_a095_default_8m_probe_unmodified_and_32m_limit_strict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert get_probe.READ_LIMIT == 8 * 1024 * 1024
    assert probe.PREFIX_BYTES == 32 * 1024 * 1024
    with pytest.raises(get_probe.GetProbeError, match="read_limit"):
        get_probe._get_prefix({}, read_limit=32 * 1048576 + 1)
    with pytest.raises(get_probe.GetProbeError, match="read_limit"):
        get_probe._get_prefix({}, read_limit=True)
    with pytest.raises(get_probe.GetProbeError, match="read_limit"):
        get_probe._get_prefix({}, read_limit=0)


def test_a095_final_workflow_manual_no_production_or_deploy() -> None:
    workflow = Path(".github/workflows/v2-a095-final-crossover.yml").read_text()
    publisher = Path(".github/workflows/v2-hostgator-publication.yml").read_text()
    code = Path("scripts/a095_32m_crossover_probe.py").read_text()
    assert "name: V2 A09.5 Final 32MiB Crossover" in workflow
    assert "workflow_dispatch:" in workflow
    assert "if: inputs.confirm_probe == 'CROSSOVER'" in workflow
    assert "timeout-minutes: 15" in workflow
    assert "actions/cache/restore" not in workflow
    assert "actions/cache/save" not in workflow
    assert "gate4_pipeline" not in workflow
    assert "v2-hostgator" not in workflow
    assert "data/derived/v2/public" not in workflow
    assert "V2 A09.5 Final 32MiB Crossover" not in publisher
    assert "PREFIX_BYTES = 32 * 1024 * 1024" in code
    assert "PARTITIONS = 10" in code
    assert "TOTAL_GETS = 8" in code
    assert "REFERENCE_PERIOD = \"2026-11\"" in code
