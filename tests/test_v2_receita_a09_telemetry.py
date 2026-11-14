"""A09.1 offline contracts: PII-free metrics, cache reasons and provenance."""
from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path
from types import SimpleNamespace

import requests

from api.sources import receita_cnpj_bulk as bulk
from api.sources import receita_cnpj_identity as identity
from api.sources import receita_cnpj_telemetry as telemetry
from api.v2 import build_consumer_gov_receita_identity as builder
from api.v2 import receita_a09_provenance as provenance


def _zip(content: str) -> bytes:
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("data.csv", content.encode("latin1"))
    return output.getvalue()


def test_a09_telemetry_redacts_unapproved_fields_and_records_resource_metrics(
    tmp_path, monkeypatch,
) -> None:
    output = tmp_path / "events.jsonl"
    monkeypatch.setenv("RECEITA_A09_TELEMETRY_PATH", str(output))
    telemetry.emit(
        "cache_decision", cache_decision="miss_period",
        reference_period="2026-10",
        cnpj="92751213000173", provider="PREVISUL", name="PRIVATE NAME",
        source_url="https://token.example/private", private_token="never-print",
    )
    raw = output.read_text(encoding="utf-8")
    item = json.loads(raw)
    assert item["cache_decision"] == "miss_period"
    assert item["reference_period"] == "2026-10"
    assert "disk_free_mib" in item and "peak_rss_mib" in item
    assert all(secret not in raw for secret in (
        "92751213000173", "PREVISUL", "PRIVATE NAME", "token.example",
        "never-print",
    ))


def test_a09_cache_decision_explains_same_contract_used_by_builder(tmp_path) -> None:
    output = tmp_path / "identity.json"
    expected = builder._cache_key(
        reference_period="2026-10", universe_hash="a" * 64,
        provider_hash="b" * 64,
    )
    assert builder._cache_decision(output, None, expected) == "miss_snapshot"
    output.write_text("{}", encoding="utf-8")
    assert builder._cache_decision(output, None, expected) == "miss_contract"
    assert builder._cache_decision(output, expected, expected) == "hit"
    cases = {
        "version": "miss_contract",
        "reference_period": "miss_period",
        "target_universe_hash": "miss_universe_hash",
        "target_universe_hash_method": "miss_contract",
        "unresolved_provider_hash": "miss_provider_hash",
    }
    for field, reason in cases.items():
        existing = dict(expected)
        existing[field] = "changed"
        assert builder._cache_decision(output, existing, expected) == reason


def test_a09_release_fingerprint_is_metadata_not_remote_byte_hash() -> None:
    resources = [
        {"name": "Empresas0.zip", "size": 12, "etag": "E0",
         "last_modified": "Fri"},
        {"name": "Estabelecimentos0.zip", "size": 24, "etag": "S0",
         "last_modified": "Fri"},
    ]
    original = telemetry.release_fingerprint("2026-10", resources)
    assert original == telemetry.release_fingerprint(
        "2026-10", list(reversed(resources))
    )
    changed = [dict(item) for item in resources]
    changed[0]["etag"] = "changed"
    assert telemetry.release_fingerprint("2026-10", changed) != original
    assert telemetry.release_fingerprint("2026-09", resources) != original


class _DownloadResponse:
    def __init__(self, content: bytes) -> None:
        self.content = content

    def __enter__(self):
        return self

    def __exit__(self, *_args) -> None:
        return None

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int):
        yield self.content


class _RetrySession:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.calls = 0

    def get(self, *_args, **_kwargs):
        self.calls += 1
        if self.calls == 1:
            raise requests.ConnectionError("temporary transport failure")
        return _DownloadResponse(self.payload)


def test_a09_zip_download_emits_retry_wait_and_bytes_without_identifiers(
    tmp_path, monkeypatch,
) -> None:
    monkeypatch.setenv("RECEITA_A09_TELEMETRY_PATH", str(tmp_path / "events.jsonl"))
    sleeps = []
    monkeypatch.setattr(bulk.time, "sleep", sleeps.append)
    data = _zip("header;value\n1;2\n")
    session = _RetrySession(data)
    output = bulk._download_zip(
        session, "https://official.example/Estabelecimentos0.zip",
        tmp_path / "Estabelecimentos0.zip", retries=2,
    )
    assert output.read_bytes() == data
    assert sleeps == [1]
    assert session.calls == 2
    lines = (tmp_path / "events.jsonl").read_text(encoding="utf-8").splitlines()
    record = json.loads(lines[-1])
    assert record["event"] == "zip_download"
    assert record["category"] == "estabelecimentos"
    assert record["file_index"] == 0
    assert record["attempts"] == 2
    assert record["retry_wait_s"] == 1
    assert record["received_bytes"] == len(data)
    assert record["zip_valid"] is True
    assert "official.example" not in lines[-1]


def test_a09_real_csv_partition_scan_records_counts_and_source_period(
    tmp_path, monkeypatch,
) -> None:
    payloads = {
        "Cnaes.zip": _zip("6511101;SEGURADORA\n"),
        "Naturezas.zip": _zip("2054;SOCIEDADE ANONIMA\n"),
        "Empresas0.zip": _zip("92751213;PREVISUL SEGURADORA SA;2054\n"),
        "Estabelecimentos0.zip": _zip(
            "92751213;0001;73;1;PREVISUL;02;20261001;;;;20010101;6511101;\n"
        ),
    }
    meta = {
        name: {"name": name, "size": len(data), "etag": f"etag-{i}",
               "last_modified": "2026-11-01"}
        for i, (name, data) in enumerate(payloads.items())
    }
    release = bulk.ReceitaOpenDataRelease(
        base_url="https://www.gov.br/",
        release_url="https://arquivos.receitafederal.gov.br/2026-10/",
        period="2026-10", establishment_files=("Estabelecimentos0.zip",),
    )
    monkeypatch.setattr(identity, "_release_files", lambda *_a, **_k: {
        "companies": ["Empresas0.zip"],
        "cnaes": "Cnaes.zip",
        "legal_natures": "Naturezas.zip",
        "metadata": meta,
    })
    def download(_sess, _url, destination):
        destination.write_bytes(payloads[destination.name])
        return destination

    monkeypatch.setattr(identity, "_download_zip", download)
    monkeypatch.setenv("RECEITA_A09_TELEMETRY_PATH", str(tmp_path / "trace.jsonl"))
    snapshot = identity.build_filtered_identity_snapshot(
        [{"cnpj": "92751213000173", "entity_id": "test:fip",
          "legal_name": "PREVISUL SEGURADORA SA"}],
        ["PREVISUL"], release=release, work_dir=tmp_path / "scratch",
    )
    events = [
        json.loads(line)
        for line in (tmp_path / "trace.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    parses = [e for e in events if e["event"] == "partition_parse"]
    assert len(parses) == 2
    assert {e["category"] for e in parses} == {
        "empresas", "estabelecimentos",
    }
    assert all(e["rows_scanned"] == 1 for e in parses)
    assert all(e["rows_retained"] == 1 for e in parses)
    assert snapshot["source"]["reference_period"] == "2026-10"
    assert len(snapshot["source"]["release_fingerprint"]) == 64
    assert snapshot["source"]["release_checked_at"]
    assert snapshot["meta"]["company_files_scanned"] == ["Empresas0.zip"]


def test_a09_provenance_matches_snapshot_and_rejects_false_current_period(
    tmp_path,
) -> None:
    snapshot = tmp_path / "identity.json"
    source = {
        "reference_period": "2026-10",
        "release_url": "https://official.example/2026-10/",
        "release_fingerprint": "b" * 64,
        "release_fingerprint_method": "metadata_only",
        "retrieved_at": "2026-11-12T12:00:00Z",
    }
    key = builder._cache_key(
        reference_period="2026-10", universe_hash="a" * 64,
        provider_hash="c" * 64,
    )
    snapshot.write_text(json.dumps({
        "source": source, "meta": {"gate4_cache_key": key},
        "canonical_records": [{"cnpj": "92751213000173"}],
    }), encoding="utf-8")
    digest = provenance._sha256(snapshot)
    lineage = SimpleNamespace(
        build_id="test-build", state="fresh", freshness_method="current_fetch",
        sha256=digest,
    )
    marker = snapshot.with_suffix(".a09.json")
    marker.write_text(json.dumps({
        "observed_release_period": "2026-10",
        "observed_release_url": source["release_url"],
        "release_checked_at": "2026-11-14T12:00:00Z",
        "snapshot_reference_period": "2026-10",
        "cache_decision": "miss_period",
        "cache_contract_version": key["version"],
        "target_universe_hash": key["target_universe_hash"],
        "unresolved_provider_hash": key["unresolved_provider_hash"],
    }), encoding="utf-8")
    proof = provenance.build_compact_provenance(snapshot, lineage, marker)
    assert proof["marker_consistency"] == "matched"
    assert proof["reference_period"] == "2026-10"
    assert proof["cache_decision_reason"] == "miss_period"
    assert proof["release_checked_at"] == "2026-11-14T12:00:00Z"
    assert proof["last_remote_fetch_at"] == "2026-11-12T12:00:00Z"
    assert "canonical_records" not in proof
    assert "92751213000173" not in json.dumps(proof)
    bad = json.loads(marker.read_text(encoding="utf-8"))
    bad["observed_release_period"] = "2026-11"
    marker.write_text(json.dumps(bad), encoding="utf-8")
    changed = provenance.build_compact_provenance(snapshot, lineage, marker)
    assert changed["marker_consistency"] == "mismatch"
    assert changed["release_checked_at"] is None
    assert changed["cache_decision_reason"] is None
