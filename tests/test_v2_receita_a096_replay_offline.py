"""A09.6 actual replay() with real ZIP parsers and 20 local ZIPs, zero HTTP.

This intentionally does NOT mock either Receita CSV parsing implementation,
unlike the small stage-wiring tests. It exercises all replay path bindings,
the official-release identity metadata contract, the A09.4 reader and the
exact-input semantic comparison, including across an artificial UTC midnight.
"""
from __future__ import annotations

import hashlib
import io
import json
from datetime import date
from pathlib import Path

import pytest

from api.sources import receita_cnpj_bulk as bulk
from api.sources import receita_cnpj_identity as identity
from api.sources import receita_cnpj_run_zip_cache as a094
from api.v2 import refresh_receita_lifecycle as lifecycle_module
from scripts import a096_live_proof as proof


def _zip(csv: str) -> bytes:
    result = io.BytesIO()
    import zipfile
    with zipfile.ZipFile(result, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("data.csv", csv.encode("latin1"))
    return result.getvalue()


def _write(path: Path, obj: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


@pytest.fixture
def actual_local_replay(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    """Prepare a complete 20-partition snapshot from the real sequential parsers."""
    monkeypatch.setenv("V2_BUILD_ID", "a096-actual-parser-offline")
    monkeypatch.setenv("RUNNER_TEMP", str(tmp_path / "runner"))
    monkeypatch.setenv("RECEITA_A096_PREFETCH", "0")
    monkeypatch.setenv("RECEITA_A096_RETAIN_ZIPS", "0")
    monkeypatch.setattr(proof, "DERIVED", tmp_path / "derived")
    monkeypatch.setattr(proof, "CACHE_ROOT", tmp_path / "cache")

    root = proof._runner_workspace()
    companies = tmp_path / "local_companies"
    establishments = tmp_path / "local_establishments"
    companies.mkdir()
    establishments.mkdir()
    monkeypatch.setattr(proof, "_company_root", lambda: companies)
    monkeypatch.setattr(proof, "_cache_dir", lambda: establishments)
    monkeypatch.setattr(a094, "_cache_dir", lambda: establishments)
    monkeypatch.setattr(lifecycle_module, "validate_refresh_payload", lambda *_a, **_k: None)

    company_names = [f"Empresas{i}.zip" for i in range(10)]
    establishment_names = [f"Estabelecimentos{i}.zip" for i in range(10)]
    raw = {
        "Motivos.zip": _zip("01;Motivo valido\n"),
        "Cnaes.zip": _zip("6511101;Seguradoras\n"),
        "Naturezas.zip": _zip("2054;Sociedade anonima\n"),
    }
    for i in range(10):
        if i == 9:
            raw[company_names[i]] = _zip("12345678;DEMO SEGURADORA SA;2054\n")
            raw[establishment_names[i]] = _zip(
                "12345678;0001;00;1;DEMO SEGURADORA;02;20260110;01;;"
                ";20010101;6511101;\n"
            )
        else:
            raw[company_names[i]] = _zip("87654321;OUTRA EMPRESA SA;2054\n")
            raw[establishment_names[i]] = _zip(
                "87654321;0001;00;1;OUTRA EMPRESA;02;20260110;01;;"
                ";20010101;6511101;\n"
            )
    for name in company_names:
        (companies / name).write_bytes(raw[name])
    for name in establishment_names:
        (establishments / name).write_bytes(raw[name])

    metadata = {
        name: {"name": name, "size": len(content), "etag": f'"etag-{name}"',
               "last_modified": "Mon, 16 Nov 2026 10:04:47 GMT"}
        for name, content in raw.items()
    }
    root_url = "https://arquivos.receitafederal.gov.br/public.php/dav/files/fixture/"
    release = bulk.ReceitaOpenDataRelease(
        base_url="https://arquivos.receitafederal.gov.br/index.php/s/fixture",
        release_url=root_url + "2026-11/",
        period="2026-11",
        establishment_files=tuple(establishment_names),
        resource_metadata=tuple(
            metadata[name] for name in ("Motivos.zip", *establishment_names)
        ),
    )
    monkeypatch.setattr(proof.bulk, "discover_latest_release", lambda: release)
    monkeypatch.setattr(identity, "_release_files", lambda _s, _r: {
        "companies": company_names,
        "cnaes": "Cnaes.zip",
        "legal_natures": "Naturezas.zip",
        "metadata": metadata,
    })

    network_attempts: list[str] = []
    def local_source(_session, url: str, destination: Path, **_kwargs) -> Path:
        name = url.rsplit("/", 1)[-1]
        # This is the ORIGINAL downloader being called by the live proof.
        # Every single invocation must read only fixture bytes, no network.
        assert name in raw
        network_attempts.append(name)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(raw[name])
        return destination

    monkeypatch.setattr(bulk, "_download_zip", local_source)
    monkeypatch.setattr(identity, "_download_zip", local_source)
    monkeypatch.setattr(bulk, "_today_utc", lambda: date(2026, 11, 17))

    entities = [{
        "cnpj": "12345678000100",
        "entity_id": "demo",
        "legal_name": "DEMO SEGURADORA SA",
    }]
    providers = ["DEMO SEGURADORA"]
    monkeypatch.setattr(proof, "_load_inputs", lambda: (entities, providers))
    _write(proof.DERIVED / "source/classification_inventory.json", {
        "artifact": "v2_classification_inventory",
        "entities": entities,
    })
    eligibility = proof.DERIVED / "entity_eligibility_inventory.json"
    consumer = proof.DERIVED / "consumer_gov_identity_experiment.json"
    _write(eligibility, {"entities": entities})
    _write(consumer, {"unresolved_providers": [{"provider": providers[0]}]})
    monkeypatch.setattr(proof, "ELIGIBILITY_PATH", eligibility)
    monkeypatch.setattr(proof, "CONSUMER_IDENTITY_PATH", consumer)

    baseline_lifecycle = bulk.refresh_filtered_lifecycle(
        entities, release=release, work_dir=tmp_path / "baseline_lifecycle",
    )
    baseline_lifecycle["meta"].update({
        "target_universe_hash": lifecycle_module.regulatory_target_universe_hash(entities),
        "target_universe_hash_method": "sorted_normalized_cnpjs_v1",
        "gate4_cache_contract_version": lifecycle_module.RECEITA_LIFECYCLE_CACHE_CONTRACT_VERSION,
    })
    baseline_identity = identity.build_filtered_identity_snapshot(
        entities, providers, release=release, work_dir=tmp_path / "baseline_identity",
    )
    baseline_identity["meta"]["gate4_cache_key"] = proof._cache_key(
        reference_period=release.period,
        universe_hash=proof.target_universe_hash(entities),
        provider_hash=proof.unresolved_provider_hash(providers),
    )
    baseline_identity["meta"]["target_universe_hash_method"] = (
        proof.TARGET_UNIVERSE_HASH_METHOD
    )

    _write(proof._candidate_file(proof.NOVEMBER_SOURCE), baseline_lifecycle)
    _write(proof._candidate_file(proof.NOVEMBER_CONDUCT), baseline_identity)
    proof_hashes = {
        source: proof._hash_file(proof._candidate_file(source))
        for source in proof.EXPECTED
    }
    families = {
        group: [{
            "name": name,
            "size": len(raw[name]),
            "etag": metadata[name]["etag"],
            "last_modified": metadata[name]["last_modified"],
            "sha256": hashlib.sha256(raw[name]).hexdigest(),
        } for name in names]
        for group, names in (
            ("Empresas", company_names),
            ("Estabelecimentos", establishment_names),
        )
    }
    _write(root / "local-input-proof.json", {
        "schema": "a096-local-replay-input-v1",
        "reference_period": release.period,
        "input_mode": "same_exact_local_zip_bytes",
        "zip_files_verified": 20,
        "release_url_sha256": "f" * 64,
        "zip_partitions": families,
        "serial_snapshot_sha256": proof_hashes,
    })
    # Artificial UTC day change: replay must honor baseline record observation
    # date, restoring the caller clock afterwards.
    after_midnight = lambda: date(2026, 11, 18)
    monkeypatch.setattr(bulk, "_today_utc", after_midnight)
    return {
        "root": root, "companies": companies, "establishments": establishments,
        "network_attempts": network_attempts,
        "company_names": company_names, "establishment_names": establishment_names,
        "after_midnight": after_midnight,
    }


def test_a096_replay_real_csv_parsers_20_local_zip_no_national_get(
    actual_local_replay: dict,
) -> None:
    previous_attempts = len(actual_local_replay["network_attempts"])
    outcome = proof.replay()
    assert outcome["stage"] == "same_input_serial_replay"
    assert outcome["status"] == "equivalent", outcome
    assert outcome["zip_files_checked"] == 20
    assert outcome["production_publication_authorized"] is False
    assert all(
        not record["diff_paths_only"]
        for record in outcome["semantic_checks"].values()
    )
    # Only Motivos, Cnaes and Naturezas may reach the original downloader;
    # all 20 national partition bytes must come from retained local ZIPs.
    after = actual_local_replay["network_attempts"][previous_attempts:]
    assert after
    assert set(after) <= {"Motivos.zip", "Cnaes.zip", "Naturezas.zip"}
    assert bulk._today_utc is actual_local_replay["after_midnight"]
    assert proof._serial_file(proof.NOVEMBER_SOURCE).is_file()
    assert proof._serial_file(proof.NOVEMBER_CONDUCT).is_file()


def test_a096_replay_real_parser_rejects_changed_local_company_sha(
    actual_local_replay: dict,
) -> None:
    path = actual_local_replay["companies"] / "Empresas7.zip"
    raw = bytearray(path.read_bytes())
    raw[10] ^= 1
    path.write_bytes(raw)
    result = proof.replay()
    assert result["status"] == "insufficient_evidence"
    assert result["reason"] == "missing_or_corrupt_local_partition"
    assert result["production_publication_authorized"] is False
