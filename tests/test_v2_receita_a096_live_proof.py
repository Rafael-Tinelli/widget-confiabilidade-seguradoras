"""One-shot A09.6 proof end-to-end stage wiring: no official HTTP or big ZIPs.

The real 44080300940 run reached the validate stage but failed with
FileNotFoundError. These tests preserve the entire staged-snapshot copy and
proof contract, preventing another 3-hour run with a missing output folder.
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest

from api.v2.gate4_pipeline import STAGES
from scripts import a096_live_proof as proof


def _write(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def _events() -> list[dict]:
    records = []
    for i in range(10):
        records.extend((
            {"event": "prefetch_ready", "category": "empresas",
             "file_index": i, "state": "verified"},
            {"event": "prefetch_ready", "category": "estabelecimentos",
             "file_index": i, "state": "verified"},
            {"event": "zip_reuse", "category": "estabelecimentos",
             "file_index": i, "state": "stored"},
            {"event": "zip_reuse", "category": "estabelecimentos",
             "file_index": i, "state": "verified"},
            {"event": "company_capture", "category": "empresas",
             "file_index": i, "state": "verified_hardlink"},
        ))
    return records


def _setup(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    root = tmp_path / "ephemeral-runner"
    cache_root = tmp_path / "cache"
    monkeypatch.setenv("V2_BUILD_ID", "a096-live-local-1")
    monkeypatch.setenv("RECEITA_A096_PREFETCH", "1")
    monkeypatch.setenv("RECEITA_A096_RETAIN_ZIPS", "1")
    monkeypatch.setattr(proof, "CACHE_ROOT", cache_root)
    monkeypatch.setattr(proof, "_runner_workspace", lambda: root)
    monkeypatch.setattr(proof, "_receita_events", _events)
    monkeypatch.setattr(proof, "_verify_source_rows", lambda: {
        source: {"sha256": "a" * 64, "status": "fresh_current_fetch"}
        for source in proof.EXPECTED
    })
    monkeypatch.setattr(
        proof, "local_input_manifest",
        lambda **_kwargs: {
            "schema": "a096-local-replay-input-v1",
            "reference_period": "2026-11",
            "zip_files_verified": 20,
            "serial_snapshot_sha256": {source: "a" * 64
                                       for source in proof.EXPECTED},
        },
    )
    for source in proof.EXPECTED:
        snap = {
            "artifact": "example",
            "source": {
                "reference_period": "2026-11",
                "release_fingerprint": "b" * 64,
            },
            "records": [{"id": 1, "observed_at": "2026-11-17"}],
        }
        name = "source" if source == proof.NOVEMBER_SOURCE else "conduct"
        _write(cache_root / name / (source + ".json"), snap)
        _write(root / "historical-2026-11" / (source + ".json"), snap)
    return root


def test_a096_live_validate_writes_parent_directory_before_copy(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    root = _setup(monkeypatch, tmp_path)
    destination = root / "prefetch"
    assert not destination.exists()
    result = proof.validate()
    assert result["status"] == "verified"
    assert result["zip_files_verified"] == 20
    assert result["prefetch_verified"] == {"empresas": 10, "estabelecimentos": 10}
    assert result["a094"] == {"stored": 10, "verified": 10}
    assert result["company_hardlinks_verified"] == 10
    assert (root / "local-input-proof.json").is_file()
    for source in proof.EXPECTED:
        assert (destination / (source + ".json")).is_file()
    assert result["historical_comparison_not_same_zip_bytes"]


def test_a096_live_validate_rejects_any_missing_prefetch_event(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    _setup(monkeypatch, tmp_path)
    events = _events()
    events.pop(0)
    monkeypatch.setattr(proof, "_receita_events", lambda: events)
    with pytest.raises(RuntimeError, match="ambiguous_or_missing_zip_transfer"):
        proof.validate()


def test_a096_lineage_guard_reports_safe_source_label_for_missing_cache(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setenv("V2_BUILD_ID", "a096-local")
    monkeypatch.setattr(proof, "_cache_file",
                        lambda source: tmp_path / (source + ".json"))
    monkeypatch.setattr(proof, "_lineage_row", lambda source: {
        "build_id": "a096-local", "state": "fresh",
        "freshness_method": "current_fetch", "sha256": "a" * 64,
    })
    with pytest.raises(RuntimeError, match="missing_current_receita_cache_"):
        proof._verify_source_rows()


def test_a096_safe_diagnostics_only_contains_counts_and_presence(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    _setup(monkeypatch, tmp_path)
    monkeypatch.setattr(proof, "_company_root", lambda: tmp_path / "company-zips")
    monkeypatch.setattr(proof, "_cache_dir", lambda: tmp_path / "estab-zips")
    stats = proof._safe_diagnostics("validate")
    assert stats["a09_telemetry_available"] is True
    assert stats["prefetch_ready_count"] == {
        "empresas": 10, "estabelecimentos": 10,
    }
    assert stats["a094_stored_count"] == 10
    assert stats["a094_verified_count"] == 10
    assert stats["company_retained_count"] == 10
    assert "12345678000100" not in str(stats)
    assert "https" not in str(stats).lower()


def _offline_replay_inputs(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    eligibility = tmp_path / "derived/entity_eligibility_inventory.json"
    consumer = tmp_path / "derived/consumer_gov_identity_experiment.json"
    _write(eligibility, {"entities": [{"cnpj": "12345678000100"}]})
    _write(consumer, {"unresolved_providers": [{"provider": "DEMO"}]})
    monkeypatch.setattr(proof, "ELIGIBILITY_PATH", eligibility)
    monkeypatch.setattr(proof, "CONSUMER_IDENTITY_PATH", consumer)


def test_a096_replay_classification_path_follows_real_gate4_dag() -> None:
    source = next(stage for stage in STAGES if stage.stage_id == "source_snapshot")
    expected = "data/derived/v2/source/classification_inventory.json"
    assert expected in source.outputs
    assert str(proof.DERIVED / "source/classification_inventory.json") == expected
    assert "data/derived/v2/entity_classification_inventory.json" not in source.outputs


@pytest.mark.parametrize(
    ("file_label", "missing_name"),
    [
        ("classification", "source/classification_inventory.json"),
        ("candidate_identity", "candidate_identity"),
        ("consumer_eligibility", "consumer_eligibility"),
        ("consumer_identity", "consumer_identity"),
    ],
)
def test_a096_replay_fails_before_parsing_if_a_required_input_is_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
    file_label: str, missing_name: str,
) -> None:
    root = _setup(monkeypatch, tmp_path)
    proof.validate()
    derived = tmp_path / "derived"
    _write(
        derived / "source/classification_inventory.json",
        {"entities": [{"cnpj": "12345678000100"}]},
    )
    monkeypatch.setattr(proof, "DERIVED", derived)
    _offline_replay_inputs(monkeypatch, tmp_path)
    if missing_name == "candidate_identity":
        proof._candidate_file(proof.NOVEMBER_CONDUCT).unlink()
    elif missing_name == "consumer_eligibility":
        proof.ELIGIBILITY_PATH.unlink()
    elif missing_name == "consumer_identity":
        proof.CONSUMER_IDENTITY_PATH.unlink()
    else:
        (derived / missing_name).unlink()
    monkeypatch.setattr(
        proof.bulk, "discover_latest_release",
        lambda: pytest.fail("no network or WebDAV before replay preflight"),
    )
    with pytest.raises(RuntimeError, match="missing_replay_input_" + file_label):
        proof.replay()
    assert root.joinpath("local-input-proof.json").is_file()


def test_a096_replay_calls_serial_reader_and_checks_same_input_verdict(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    _setup(monkeypatch, tmp_path)
    proof.validate()
    derivative = tmp_path / "derived"
    target_entities = [{"cnpj": "12345678000100", "legal_name": "DEMO"}]
    _write(
        derivative / "source/classification_inventory.json",
        {"entities": target_entities},
    )
    primary = proof._candidate_file(proof.NOVEMBER_SOURCE)
    primary_snapshot = json.loads(primary.read_text(encoding="utf-8"))
    primary_snapshot["meta"] = {
        "target_universe_hash": proof.regulatory_target_universe_hash(target_entities),
    }
    _write(primary, primary_snapshot)
    monkeypatch.setattr(proof, "DERIVED", derivative)
    _offline_replay_inputs(monkeypatch, tmp_path)
    monkeypatch.setattr(
        proof.bulk, "discover_latest_release",
        lambda: SimpleNamespace(period="2026-11"),
    )
    monkeypatch.setattr(
        proof, "_load_inputs",
        lambda: ([{"cnpj": "12345678000100"}], ["DEMO"]),
    )
    monkeypatch.setattr(
        proof, "_company_root", lambda: tmp_path / "company",
    )
    monkeypatch.setattr(
        proof, "_cache_dir", lambda: tmp_path / "estab",
    )
    observed = []

    def fake_lifecycle(*, output, release, entities):
        observed.append(("lifecycle", release.period, len(entities)))
        _write(output, {"records": [{"cnpj": "12345678000100"}]})
        return {"records": [{"cnpj": "12345678000100"}]}

    def fake_identity(targets, providers, *, release):
        observed.append(("identity", release.period, len(targets), len(providers)))
        return {"meta": {}, "source": {"reference_period": release.period}}

    def fake_compare(_zip_proof, **kwargs):
        assert kwargs["serial_lifecycle"] == proof._candidate_file(proof.NOVEMBER_SOURCE)
        assert kwargs["candidate_lifecycle"] == proof._serial_file(proof.NOVEMBER_SOURCE)
        assert kwargs["serial_identity"].is_file()
        assert kwargs["candidate_identity"].is_file()
        assert proof.os.environ["RECEITA_A096_PREFETCH"] == "0"
        assert proof.os.environ["RECEITA_A096_RETAIN_ZIPS"] == "0"
        return {
            "status": "equivalent", "reason": "same_exact_zip_set_and_semantic_match",
            "zip_files_checked": 20,
            "semantic_checks": {"receita_cnpj_lifecycle": {},
                                "receita_consumer_identity": {}},
        }

    monkeypatch.setattr(proof, "refresh_receita_lifecycle", fake_lifecycle)
    monkeypatch.setattr(proof.identity, "build_filtered_identity_snapshot", fake_identity)
    today = lambda: date(2027, 1, 1)
    monkeypatch.setattr(proof.bulk, "_today_utc", today)
    monkeypatch.setattr(proof, "compare_local", fake_compare)
    result = proof.replay()
    assert proof.bulk._today_utc is today  # monkeypatch restored after replay
    assert result["status"] == "equivalent"
    assert result["zip_files_checked"] == 20
    assert result["production_publication_authorized"] is False
    assert observed == [
        ("lifecycle", "2026-11", 1), ("identity", "2026-11", 1, 1),
    ]



def test_a096_realistic_mixed_prefetch_and_verified_serial_fallback(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    _setup(monkeypatch, tmp_path)
    records = [
        row for row in _events()
        if not (
            row["event"] == "prefetch_ready"
            and (
                (row["category"] == "empresas" and row["file_index"] == 9)
                or (row["category"] == "estabelecimentos"
                    and row["file_index"] in (5, 6, 7, 8, 9))
            )
        )
    ]
    for category, indices in (
        ("empresas", (9,)),
        ("estabelecimentos", (5, 6, 7, 8, 9)),
    ):
        for index in indices:
            records.extend((
                {"event": "prefetch_skipped", "category": category,
                 "file_index": index, "state": "disk_guard"},
                {"event": "zip_download", "category": category,
                 "file_index": index, "state": "success"},
            ))
    monkeypatch.setattr(proof, "_receita_events", lambda: records)
    outcome = proof.validate()
    assert outcome["status"] == "verified"
    assert outcome["transfer_paths"] == {
        "empresas": {"prefetch_used": 9, "serial_fallback_verified": 1, "total": 10},
        "estabelecimentos": {
            "prefetch_used": 5, "serial_fallback_verified": 5, "total": 10,
        },
    }
    assert outcome["a094"] == {"stored": 10, "verified": 10}
    assert outcome["company_hardlinks_verified"] == 10


def test_a096_ambiguous_duplicate_transfer_without_fallback_is_rejected() -> None:
    records = _events()
    records.append({
        "event": "zip_download", "category": "empresas",
        "file_index": 7, "state": "success",
    })
    with pytest.raises(RuntimeError, match="ambiguous_or_missing_zip_transfer"):
        proof._verify_partition_transfer_routes(records)


def test_a096_explicit_prefetch_failure_and_serial_success_is_valid() -> None:
    records = _events()
    records.extend((
        {"event": "prefetch_fallback", "category": "empresas",
         "file_index": 3, "state": "serial"},
        {"event": "zip_download", "category": "empresas",
         "file_index": 3, "state": "success"},
    ))
    result = proof._verify_partition_transfer_routes(records)
    assert result["empresas"]["prefetch_used"] == 9
    assert result["empresas"]["serial_fallback_verified"] == 1


def test_a096_candidate_universe_mismatch_refuses_real_serial_replay(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    _setup(monkeypatch, tmp_path)
    proof.validate()
    source = proof._candidate_file(proof.NOVEMBER_SOURCE)
    payload = json.loads(source.read_text())
    payload["meta"] = {"target_universe_hash": "0" * 64}
    _write(source, payload)
    derived = tmp_path / "derived"
    monkeypatch.setattr(proof, "DERIVED", derived)
    _offline_replay_inputs(monkeypatch, tmp_path)
    _write(
        derived / "source/classification_inventory.json",
        {"entities": [{"cnpj": "12345678000100"}]},
    )
    monkeypatch.setattr(
        proof.bulk, "discover_latest_release",
        lambda: SimpleNamespace(period="2026-11"),
    )
    monkeypatch.setattr(proof, "_load_inputs", lambda: ([{"cnpj": "12345678000100"}], ["DEMO"]))
    with pytest.raises(RuntimeError, match="regulatory_universe_not_equivalent"):
        proof.replay()



def test_a096_original_observation_date_is_strict_and_not_discarded() -> None:
    baseline = {
        "records": [
            {"cnpj": "12345678000100", "observed_at": "2026-11-17"},
            {"cnpj": "87654321000100", "observed_at": "2026-11-17"},
        ],
    }
    assert proof._original_observation_date(baseline) == date(2026, 11, 17)
    from scripts.a096_homologation import _diff_paths
    candidate = json.loads(json.dumps(baseline))
    candidate["records"][0]["observed_at"] = "2026-11-18"
    assert any("observed_at" in field for field in _diff_paths(
        baseline, candidate, "receita_cnpj_lifecycle",
    ))


@pytest.mark.parametrize(
    "records",
    [
        [],
        [{"observed_at": "2026-11-17"}, {"observed_at": "2026-11-18"}],
        [{"observed_at": "2026-11-17"}, {"observed_at": None}],
        [{"observed_at": "2026-13-17"}],
        [{"observed_at": "2026-11-17T00:00:00Z"}],
        [{"cnpj": "12345678000100"}],
    ],
)
def test_a096_replay_rejects_ambiguous_observation_date(records: list) -> None:
    with pytest.raises(RuntimeError, match="observation"):
        proof._original_observation_date({"records": records})
