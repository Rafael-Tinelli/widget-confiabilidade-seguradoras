from __future__ import annotations

import gzip
import json
import subprocess
from pathlib import Path

import pytest

import api.v2.bootstrap_conduct_consumer_cache as bootstrap
from api.v2.bootstrap_conduct_consumer_cache import (
    ConductConsumerBootstrapError,
    bootstrap_consumer_cache,
)
from api.v2.build_conduct_source_snapshot import _validate_consumer_manifest


def _write_gzip(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wb") as handle:
        handle.write(json.dumps(payload).encode("utf-8"))


def _entry(month: str) -> dict:
    return {
        "meta": {
            "status": "ok",
            "month": month,
            "generated_at": "2026-07-01T12:00:00Z",
            "companies": 1,
        },
        "by_name_key_raw": {
            "seguradora teste": {
                "name": "Seguradora Teste",
                "statistics": {"complaintsCount": 1},
            }
        },
    }


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _versioned_core(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    aggregate_months: list[str] | None = None,
) -> tuple[Path, list[str], str]:
    derived = root / "data" / "derived" / "consumidor_gov"
    monthly = derived / "monthly"
    months = [f"2026-{month:02d}" for month in range(1, 13)]
    for month in months:
        _write_gzip(monthly / f"consumidor_gov_{month}.json.gz", _entry(month))

    aggregate = {
        "meta": {
            "status": "ok",
            "months": aggregate_months if aggregate_months is not None else months,
            "generated_at": "2026-07-01T12:30:00Z",
            "companies": 1,
            "invalid_months": [],
        },
        "by_name_key_raw": {
            "seguradora teste": {
                "name": "Seguradora Teste",
                "statistics": {"complaintsCount": 12},
            }
        },
    }
    _write_gzip(derived / "consumidor_gov_agg.json.gz", aggregate)

    monkeypatch.setattr(bootstrap.consumer_gov_build, "DERIVED_DIR", derived)
    monkeypatch.setattr(bootstrap.consumer_gov_build, "MONTHLY_DIR", monthly)

    _git(root, "init")
    _git(root, "config", "user.name", "Gate4 Test")
    _git(root, "config", "user.email", "gate4@example.test")
    _git(root, "add", "data/derived/consumidor_gov")
    _git(root, "commit", "-m", "seed Consumer.gov core")
    head = _git(root, "rev-parse", "HEAD")
    monkeypatch.setenv("V2_SOURCE_HEAD_SHA", head)
    return derived, months, head


def test_bootstrap_seeds_hash_validated_cache_from_exact_git_head(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    _, months, head = _versioned_core(tmp_path, monkeypatch)
    cache = tmp_path / "cache"

    result = bootstrap_consumer_cache(cache_dir=cache, repo_root=tmp_path)
    manifest = _validate_consumer_manifest(cache / "consumer_gov_core_manifest.json")

    assert result["action"] == "versioned_repository_bootstrap"
    assert result["months"] == months
    assert result["fetched_at"] == "2026-07-01T12:30:00Z"
    assert manifest["bootstrap_provenance"] == {
        "method": "versioned_repository_snapshot",
        "source_head_sha": head,
        "tracked_paths": result["bootstrap_provenance"]["tracked_paths"],
        "policy": "bootstrap_only_when_gate4_cache_absent",
    }
    assert len(manifest["months"]) == 12
    assert all(Path(item["path"]).is_file() for item in manifest["months"])


def test_bootstrap_refuses_materialized_file_changed_after_git_head(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    derived, _, _ = _versioned_core(tmp_path, monkeypatch)
    path = derived / "monthly" / "consumidor_gov_2026-12.json.gz"
    payload = _entry("2026-12")
    payload["by_name_key_raw"]["seguradora teste"]["statistics"]["complaintsCount"] = 2
    _write_gzip(path, payload)

    with pytest.raises(ConductConsumerBootstrapError, match="differ from Git HEAD"):
        bootstrap_consumer_cache(cache_dir=tmp_path / "cache", repo_root=tmp_path)


def test_bootstrap_refuses_aggregate_month_window_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    months = [f"2026-{month:02d}" for month in range(1, 12)]
    _versioned_core(tmp_path, monkeypatch, aggregate_months=months)

    with pytest.raises(ConductConsumerBootstrapError, match="window mismatch"):
        bootstrap_consumer_cache(cache_dir=tmp_path / "cache", repo_root=tmp_path)
