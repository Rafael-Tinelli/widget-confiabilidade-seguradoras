"""A06: local cross-run reuse is evidence, never remote freshness."""
from __future__ import annotations

import csv
import io
import json
import shutil
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from api.probes import finalizadas_history as history
from api.probes import finalizadas_observational as obs
from api.probes import finalizadas_reuse as reuse

REPO = "Rafael-Tinelli/widget-confiabilidade-seguradoras"
URL = "https://www.consumidor.gov.br/pages/publicacao/externo/20261100000011098/download"
RUN_ID = 123456
CODE = "20261100000011098"
STAMP = "06/11/2026 às 15:13"


def _fixture(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(obs, "MIN_CSV_BYTES", 0)
    source = tmp_path / "real.zip"
    row = dict.fromkeys(obs.COLUMNS, "")
    row.update({
        "data_finalizacao": "14/10/2026", "segmento_de_mercado": obs.TARGET,
        "nome_fantasia": "Alfa Seguros", "respondida": "Sim",
        "situacao": "Finalizada avaliada",
        "avaliacao_reclamacao": "Resolvida", "nota_do_consumidor": "5",
    })
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=obs.COLUMNS, delimiter=";")
    writer.writeheader()
    writer.writerow(row)
    with zipfile.ZipFile(source, "w") as archive:
        archive.writestr(
            "finalizadas_2026-10.csv", buffer.getvalue().encode("utf-8-sig")
        )

    aggregate, evidence = obs.aggregate_zip(source, "2026-10", URL)
    acquired = datetime.now(timezone.utc).replace(microsecond=0)
    evidence.update({
        "publication_code": CODE, "published_at": STAMP, "modified_at": STAMP,
        "announced_bytes": source.stat().st_size, "catalog_status": "listed",
        "discovery": "official_publicacoes_json",
        "revision": False, "history_gap": False,
        "source_fetched_at": acquired.isoformat().replace("+00:00", "Z"),
        "acquisition_mode": "current_http_fetch",
    })
    downloaded = tmp_path / "downloaded"
    month_dir = downloaded / "2026-10"
    month_dir.mkdir(parents=True)
    shutil.copyfile(source, month_dir / "finalizadas_2026-10.zip")
    aggregate_path = month_dir / "agregado-experimental.json"
    validation_path = month_dir / "validacao.json"
    obs._write_json(aggregate_path, aggregate)
    obs._write_json(validation_path, evidence)
    evidence["aggregate_json_sha256"] = history.digest(aggregate_path)
    evidence["validation_json_sha256"] = history.digest(validation_path)
    manifest = {
        "schema": history.SCHEMA,
        "status": "experimental_no_publication",
        "months": {"2026-10": evidence},
        "revisions": [], "quality_alerts": [],
    }
    manifest_path = downloaded / "manifest.json"
    obs._write_json(manifest_path, manifest)
    sealed = tmp_path / "previous.json"
    shutil.copyfile(manifest_path, sealed)
    recovery = tmp_path / "recovery.json"
    recovery.write_text(json.dumps({
        "status": "restored", "source_run_id": RUN_ID, "source_attempt": 1,
        "source_artifact": f"finalizadas-observational-state-{RUN_ID}-a1",
        "manifest_sha256": history.digest(sealed),
    }), encoding="utf-8")
    return downloaded, sealed, recovery, evidence, acquired


def _mock_artifact(monkeypatch, downloaded):
    calls = []
    def fake_api(route):
        calls.append(route)
        assert route.endswith(f"/runs/{RUN_ID}/artifacts?per_page=100")
        return {"artifacts": [{
            "name": f"finalizadas-observational-{RUN_ID}-a1",
            "expired": False, "size_in_bytes": 2048,
        }]}
    def fake_download(run_id, name, repo, dest):
        assert (run_id, name, repo) == (
            RUN_ID, f"finalizadas-observational-{RUN_ID}-a1", REPO
        )
        for file in downloaded.rglob("*"):
            if file.is_file():
                target = dest / file.relative_to(downloaded)
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(file, target)
    monkeypatch.setattr(reuse.history, "_gh_json", fake_api)
    monkeypatch.setattr(reuse.history, "_gh_download", fake_download)
    return calls


def _live_item(evidence):
    return {
        "month": "2026-10", "url": URL, "expected_sha256": "",
        "publication_code": CODE, "announced_bytes": evidence["announced_bytes"],
        "published_at": STAMP, "modified_at": STAMP,
        "catalog_status": "listed", "discovery": "official_publicacoes_json",
    }


def test_a06_reuse_verifies_full_artifact_and_skips_official_zip(tmp_path, monkeypatch):
    downloaded, manifest, recovery, state, acquired = _fixture(tmp_path, monkeypatch)
    calls = _mock_artifact(monkeypatch, downloaded)
    cache = tmp_path / "verified-cache"
    assert reuse.prepare_cache(REPO, recovery, manifest, cache, now=acquired) == ["2026-10"]
    assert len(calls) == 1
    assert (cache / "2026-10" / "finalizadas_2026-10.zip").is_file()

    catalog = tmp_path / "catalog.json"
    catalog.write_text('{"publications":[]}', encoding="utf-8")
    live = []
    def catalog_reader(*_args, **_kwargs):
        live.append(True)
        return [_live_item(state)]

    monkeypatch.setattr(obs, "read_catalog", catalog_reader)
    monkeypatch.setattr(obs, "download_direct", lambda *_a, **_k:
                        pytest.fail("Reusable month must not redownload official ZIP"))
    out = tmp_path / "output"
    assert obs.main([
        "--catalog", str(catalog), "--output-dir", str(out),
        "--previous-manifest", str(manifest),
        "--recovery-report", str(recovery),
        "--reuse-dir", str(cache),
    ]) == 0
    assert len(live) == 1
    new = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
    evidence = new["months"]["2026-10"]
    assert evidence["acquisition_mode"] == "reused_local_verified"
    assert evidence["source_fetched_at"] == state["source_fetched_at"]
    assert evidence["catalog_checked_at"]
    assert evidence["source_fetched_at"] != evidence["catalog_checked_at"]
    assert not new["revisions"]
    assert history.validate_manifest(out / "manifest.json")["months"]


@pytest.mark.parametrize("tamper", ["zip", "aggregate", "validation"])
def test_a06_corrupted_or_adulterated_bytes_cannot_be_reused(
    tmp_path, monkeypatch, tamper,
):
    downloaded, manifest, recovery, _state, acquired = _fixture(tmp_path, monkeypatch)
    month = downloaded / "2026-10"
    target = {
        "zip": month / "finalizadas_2026-10.zip",
        "aggregate": month / "agregado-experimental.json",
        "validation": month / "validacao.json",
    }[tamper]
    bytes_before = target.read_bytes()
    target.write_bytes(bytes_before[:-1] + bytes([bytes_before[-1] ^ 1]))
    calls = _mock_artifact(monkeypatch, downloaded)
    cache = tmp_path / "reused"
    assert reuse.prepare_cache(REPO, recovery, manifest, cache, now=acquired) == []
    assert len(calls) == 1
    assert not cache.exists()


def test_a06_parser_mismatch_and_stale_ttl_skip_even_artifact_request(
    tmp_path, monkeypatch,
):
    _downloaded, manifest, recovery, _state, acquired = _fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(reuse.history, "_gh_json", lambda _route:
                        pytest.fail("No artifact query for ineligible cache"))
    stale = json.loads(manifest.read_text(encoding="utf-8"))
    stale["months"]["2026-10"]["source_fetched_at"] = (
        acquired - timedelta(days=8)
    ).isoformat()
    obs._write_json(manifest, stale)
    report = json.loads(recovery.read_text(encoding="utf-8"))
    report["manifest_sha256"] = history.digest(manifest)
    obs._write_json(recovery, report)
    assert reuse.prepare_cache(REPO, recovery, manifest, tmp_path / "out",
                               now=acquired) == []

    stale["months"]["2026-10"]["source_fetched_at"] = acquired.isoformat()
    stale["months"]["2026-10"]["parser_version"] = "obsolete-parser"
    obs._write_json(manifest, stale)
    report["manifest_sha256"] = history.digest(manifest)
    obs._write_json(recovery, report)
    assert reuse.prepare_cache(REPO, recovery, manifest, tmp_path / "out",
                               now=acquired) == []


def test_a06_changed_catalog_metadata_forces_new_download(tmp_path, monkeypatch):
    downloaded, manifest, recovery, state, acquired = _fixture(tmp_path, monkeypatch)
    calls = _mock_artifact(monkeypatch, downloaded)
    cache = tmp_path / "verified"
    assert reuse.prepare_cache(REPO, recovery, manifest, cache, now=acquired)
    assert len(calls) == 1

    catalog = tmp_path / "catalog.json"
    catalog.write_text('{"publications":[]}', encoding="utf-8")
    changed = _live_item(state)
    changed["modified_at"] = "14/11/2026 às 09:00"
    monkeypatch.setattr(obs, "read_catalog", lambda *_a, **_k: [changed])
    downloaded_now = []
    def fetch(_url, target, http=None):
        downloaded_now.append(True)
        shutil.copyfile(downloaded / "2026-10" / "finalizadas_2026-10.zip", target)
        return state["zip_sha256"]
    monkeypatch.setattr(obs, "download_direct", fetch)
    out = tmp_path / "new"
    assert obs.main([
        "--catalog", str(catalog), "--output-dir", str(out),
        "--previous-manifest", str(manifest),
        "--recovery-report", str(recovery),
        "--reuse-dir", str(cache),
    ]) == 0
    result = json.loads((out / "manifest.json").read_text())
    evidence = result["months"]["2026-10"]
    assert downloaded_now == [True]
    assert evidence["acquisition_mode"] == "current_http_fetch"
    assert evidence["revision"] is True
    assert evidence["status"] == "requires_manual_review"


def test_a06_history_gap_or_missing_full_artifact_never_claims_reuse(
    tmp_path, monkeypatch,
):
    _downloaded, manifest, recovery, _state, acquired = _fixture(tmp_path, monkeypatch)
    report = json.loads(recovery.read_text())
    report["status"] = "restored_with_gap"
    obs._write_json(recovery, report)
    assert reuse.prepare_cache(REPO, recovery, manifest, tmp_path / "out",
                               now=acquired) == []
    report["status"] = "restored"
    obs._write_json(recovery, report)
    monkeypatch.setattr(reuse.history, "_gh_json", lambda _route: {"artifacts": []})
    assert reuse.prepare_cache(REPO, recovery, manifest, tmp_path / "out",
                               now=acquired) == []


def test_a06_future_reuse_does_not_extend_original_remote_fetch(tmp_path, monkeypatch):
    _downloaded, _manifest, _recovery, state, acquired = _fixture(tmp_path, monkeypatch)
    assert obs._can_reuse(state, _live_item(state))
    old = dict(state)
    old["source_fetched_at"] = (acquired - timedelta(days=8)).isoformat()
    assert obs._can_reuse(old, _live_item(state)) is False
    changed = _live_item(state)
    changed["announced_bytes"] = state["announced_bytes"] + 1
    assert obs._can_reuse(state, changed) is False


def test_a06_lazy_restore_only_after_current_official_catalog(tmp_path, monkeypatch):
    downloaded, manifest, recovery, state, _acquired = _fixture(tmp_path, monkeypatch)
    calls = _mock_artifact(monkeypatch, downloaded)
    events = []
    original_prepare = reuse.prepare_cache

    def tracked_prepare(*args, **kwargs):
        events.append("artifact")
        return original_prepare(*args, **kwargs)

    monkeypatch.setattr(reuse, "prepare_cache", tracked_prepare)
    monkeypatch.setenv("GITHUB_REPOSITORY", REPO)
    monkeypatch.setattr(obs, "read_catalog", lambda *_a, **_k:
                        (events.append("official_catalog"), [_live_item(state)])[1])
    monkeypatch.setattr(obs, "download_direct", lambda *_a, **_k:
                        pytest.fail("Verified reuse must bypass direct ZIP download"))
    catalog = tmp_path / "catalog.json"
    catalog.write_text('{"publications":[]}', encoding="utf-8")
    cache = tmp_path / "lazy-cache"
    out = tmp_path / "result"
    assert obs.main([
        "--catalog", str(catalog), "--output-dir", str(out),
        "--previous-manifest", str(manifest),
        "--recovery-report", str(recovery),
        "--reuse-dir", str(cache),
    ]) == 0
    assert events == ["official_catalog", "artifact"]
    assert len(calls) == 1
    assert (cache / "2026-10" / "finalizadas_2026-10.zip").is_file()
    payload = json.loads((out / "manifest.json").read_text())
    assert payload["months"]["2026-10"]["acquisition_mode"] == "reused_local_verified"


def test_a06_catalog_504_does_not_download_unneeded_github_artifact(
    tmp_path, monkeypatch,
):
    _downloaded, manifest, recovery, _state, _acquired = _fixture(tmp_path, monkeypatch)
    monkeypatch.setenv("GITHUB_REPOSITORY", REPO)

    def offline_catalog(*_a, **_kw):
        raise obs.ObservationalError("simulated official HTTP 504")

    monkeypatch.setattr(obs, "read_catalog", offline_catalog)
    monkeypatch.setattr(reuse.history, "_gh_json", lambda _route:
                        pytest.fail("No prior full artifact transfer before catalog"))
    catalog = tmp_path / "catalog.json"
    catalog.write_text('{"publications":[]}', encoding="utf-8")
    out = tmp_path / "result"
    with pytest.raises(obs.ObservationalError, match="HTTP 504"):
        obs.main([
            "--catalog", str(catalog), "--output-dir", str(out),
            "--previous-manifest", str(manifest),
            "--recovery-report", str(recovery),
            "--reuse-dir", str(tmp_path / "unused-cache"),
        ])
    assert not out.exists()
