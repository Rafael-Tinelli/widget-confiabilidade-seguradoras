"""A09.6: real-proof reference validation, fail-closed SHA and local comparison.

Fixtures are small and generated offline. The real November reference values
are pinned in docs/evidence, not reconstructed from an invented example.
"""
from __future__ import annotations

import copy
import hashlib
import io
import json
import zipfile
from pathlib import Path

import pytest

from scripts import a096_homologation as proof


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _dump(path: Path, doc: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")


def _mini_zip(data: str = "hello") -> bytes:
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("data.csv", data)
    return stream.getvalue()


def _snapshot(source: str, *, volatile: str = "a") -> dict:
    base = {
        "artifact": proof.SOURCES[source]["artifact"],
        "generated_at": volatile,
        "source": {
            "reference_period": "2026-11",
            "release_fingerprint": "c" * 64,
            "release_fingerprint_method": "period_filename_size_etag_last_modified_sha256_metadata",
            "ingestion_method": "official_nextcloud_webdav_bulk_filtered_identity",
            "release_checked_at": volatile,
            "retrieved_at": volatile,
        },
    }
    if source == "receita_cnpj_lifecycle":
        base["records"] = [{"cnpj": "12345678000100", "cadastral_status": "active"}]
    else:
        base["canonical_records"] = [{"cnpj": "12345678000100", "legal_name_receita": "DEMO"}]
        base["provider_matches"] = [{"provider": "DEMO", "candidate_state": "unique_candidate"}]
        base["meta"] = {"gate4_cache_key": {
            "version": "v2-consumer-receita-identity-2",
            "reference_period": "2026-11",
            "target_universe_hash": "d" * 64,
            "unresolved_provider_hash": "e" * 64,
        }}
    return base


def _reference_fixture(tmp_path: Path) -> tuple[Path, dict]:
    filename = Path("docs/evidence/a096-november-2026-reference.json")
    assert filename.is_file()
    reference = json.loads(filename.read_text(encoding="utf-8"))
    assert reference["schema"] == proof.SCHEMA
    assert reference["reference_run"]["id"] == proof.RUN_ID
    assert reference["reference_run"]["source_head_sha"] == proof.RUN_SHA
    assert reference["proof_artifact"]["archive_sha256"] == proof.ARCHIVE_SHA
    assert reference["public_package"]["file_count"] == 816
    assert reference["source_snapshots"]["receita_cnpj_lifecycle"]["sha256"] == (
        "9e32fef301c6104bcf3c3394a55636a0a5f783ea0b47a1dc9636760a483e5382"
    )
    assert reference["source_snapshots"]["receita_consumer_identity"]["sha256"] == (
        "62928cf04a72f5476ac0ababc62e91124f8805cc5e2c3be966360b8e0f103e97"
    )
    for source, entry in reference["source_snapshots"].items():
        payload = _snapshot(source)
        if source == "receita_consumer_identity":
            payload["source"]["release_fingerprint"] = reference["identity_contract"][
                "release_fingerprint"
            ]
            key = payload["meta"]["gate4_cache_key"]
            key["target_universe_hash"] = reference["identity_contract"]["target_universe_hash"]
            key["unresolved_provider_hash"] = reference["identity_contract"]["unresolved_provider_hash"]
        serialized = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        digest = _sha(serialized)
        entry["sha256"] = digest
        name = entry["cache_name"]
        root = tmp_path / entry["cache_group"]
        root.mkdir(parents=True, exist_ok=True)
        (root / (name + ".json")).write_bytes(serialized)
        _dump(root / (name + ".meta.json"), {
            "source_id": source,
            "source_url": "https://www.gov.br/receitafederal/pt-br/acesso-a-informacao/dados-abertos/cadastros/cnpj",
            "sha256": digest, "fetched_at": "2026-11-16T22:00:00Z",
        })
    ref_path = tmp_path / "reference.json"
    _dump(ref_path, reference)
    return ref_path, reference


def test_a096_reference_verify_actual_snapshot_bytes_and_contracts(
    tmp_path: Path,
) -> None:
    reference_file, _ = _reference_fixture(tmp_path)
    result = proof.audit(reference_file, tmp_path)
    assert result["status"] == "insufficient_evidence"
    assert result["reference_snapshots_verified"] is True
    assert result["source_checks"] == {
        "receita_cnpj_lifecycle": "verified",
        "receita_consumer_identity": "verified",
    }
    assert result["reason"] == "candidate_or_same_input_zip_evidence_missing"
    assert not result["production_activation_authorized"]


def test_a096_reference_snapshot_corruption_never_approves(
    tmp_path: Path,
) -> None:
    ref, _ = _reference_fixture(tmp_path)
    target = tmp_path / "source/receita_cnpj_lifecycle.json"
    target.write_bytes(target.read_bytes() + b" ")
    result = proof.audit(ref, tmp_path)
    assert result["status"] == "insufficient_evidence"
    assert result["source_checks"]["receita_cnpj_lifecycle"] == "cache_sha256_mismatch"
    assert not result.get("reference_snapshots_verified")


def test_a096_reference_missing_cache_never_approves(
    tmp_path: Path,
) -> None:
    ref, _ = _reference_fixture(tmp_path)
    (tmp_path / "conduct/receita_consumer_identity.json").unlink()
    result = proof.audit(ref, tmp_path)
    assert result["status"] == "insufficient_evidence"
    assert result["source_checks"]["receita_consumer_identity"] == (
        "missing_cache_bytes_or_receipt"
    )


def _local_fixture(tmp_path: Path) -> dict:
    archive = _mini_zip("12345678;DEMO\n")
    digest = _sha(archive)
    roots = {
        "Empresas": tmp_path / "companies",
        "Estabelecimentos": tmp_path / "establishments",
    }
    rows = {}
    for family, root in roots.items():
        root.mkdir(parents=True)
        rows[family] = []
        for index in range(10):
            name = f"{family}{index}.zip"
            (root / name).write_bytes(archive)
            rows[family].append({
                "name": name, "size": len(archive), "sha256": digest,
                "etag": f"etag-{family}{index}",
            })
    serial = {}
    candidate = {}
    for source, suffix in (("receita_cnpj_lifecycle", "lifecycle"),
                           ("receita_consumer_identity", "identity")):
        serial[source] = tmp_path / f"serial-{suffix}.json"
        candidate[source] = tmp_path / f"candidate-{suffix}.json"
        _dump(serial[source], _snapshot(source, volatile="2026-11-16T01:00:00Z"))
        _dump(candidate[source], _snapshot(source, volatile="2026-11-17T02:00:00Z"))
    inp = {
        "schema": "a096-local-replay-input-v1",
        "build_id": "v2-gate4-full-local-example",
        "reference_period": "2026-11",
        "source_head_sha": "c" * 40,
        "release_url_sha256": "d" * 64,
        "input_mode": "same_exact_local_zip_bytes",
        "zip_files_verified": 20,
        "zip_partitions": rows,
        "serial_snapshot_sha256": {
            source: proof._hash_file(file) for source, file in serial.items()
        },
    }
    input_path = tmp_path / "input-proof.json"
    _dump(input_path, inp)
    return {
        "local_zip_proof": input_path,
        "companies": roots["Empresas"],
        "establishments": roots["Estabelecimentos"],
        "serial_lifecycle": serial["receita_cnpj_lifecycle"],
        "serial_identity": serial["receita_consumer_identity"],
        "candidate_lifecycle": candidate["receita_cnpj_lifecycle"],
        "candidate_identity": candidate["receita_consumer_identity"],
    }


def test_a096_local_replay_compares_only_exact_volatiles(
    tmp_path: Path,
) -> None:
    kw = _local_fixture(tmp_path)
    verdict = proof.compare_local(**kw)
    assert verdict["status"] == "equivalent"
    assert verdict["zip_files_checked"] == 20
    assert not verdict["production_activation_authorized"]
    assert all(not row["diff_paths_only"] for row in verdict["semantic_checks"].values())


def test_a096_local_replay_detects_real_business_difference(
    tmp_path: Path,
) -> None:
    kw = _local_fixture(tmp_path)
    candidate = kw["candidate_identity"]
    data = json.loads(candidate.read_text())
    data["canonical_records"][0]["legal_name_receita"] = "ALTERADO"
    _dump(candidate, data)
    verdict = proof.compare_local(**kw)
    assert verdict["status"] == "different"
    assert "receita_consumer_identity" in verdict["semantic_checks"]
    assert verdict["semantic_checks"]["receita_consumer_identity"]["diff_paths_only"]
    assert "ALTERADO" not in str(verdict)


@pytest.mark.parametrize("failure", ["missing", "corrupt", "remove_family"])
def test_a096_missing_or_changed_zip_never_approves(
    tmp_path: Path, failure: str,
) -> None:
    kw = _local_fixture(tmp_path)
    if failure == "missing":
        (kw["companies"] / "Empresas5.zip").unlink()
    elif failure == "corrupt":
        (kw["establishments"] / "Estabelecimentos8.zip").write_bytes(b"invalid")
    else:
        p = kw["local_zip_proof"]
        manifest = json.loads(p.read_text())
        manifest["zip_partitions"]["Empresas"].pop()
        _dump(p, manifest)
    verdict = proof.compare_local(**kw)
    assert verdict["status"] == "insufficient_evidence"
    assert verdict["zip_files_checked"] < 20


def test_a096_local_replay_rejects_snapshot_receipt_change(
    tmp_path: Path,
) -> None:
    kw = _local_fixture(tmp_path)
    serial = kw["serial_lifecycle"]
    serial.write_bytes(serial.read_bytes() + b" ")
    verdict = proof.compare_local(**kw)
    assert verdict["status"] == "insufficient_evidence"
    assert verdict["reason"] == "serial_snapshot_digest_mismatch"


def test_a096_local_replay_rejects_unmatched_source_contract(
    tmp_path: Path,
) -> None:
    kw = _local_fixture(tmp_path)
    candidate = kw["candidate_identity"]
    data = json.loads(candidate.read_text())
    data["meta"]["gate4_cache_key"]["unresolved_provider_hash"] = "INVALID"
    _dump(candidate, data)
    verdict = proof.compare_local(**kw)
    assert verdict["status"] == "insufficient_evidence"
    assert verdict["reason"] == "snapshot_contract_or_population_incompatible"


def test_a096_exact_volatile_allowlist_does_not_hide_business_fields(
    tmp_path: Path,
) -> None:
    kw = _local_fixture(tmp_path)
    candidate = kw["candidate_lifecycle"]
    data = json.loads(candidate.read_text())
    data["source"]["release_checked_at"] = "another-date"
    data["records"][0]["cadastral_status"] = "closed"
    _dump(candidate, data)
    verdict = proof.compare_local(**kw)
    assert verdict["status"] == "different"
    assert any("cadastral_status" in x for x in verdict["semantic_checks"][
        "receita_cnpj_lifecycle"
    ]["diff_paths_only"])
