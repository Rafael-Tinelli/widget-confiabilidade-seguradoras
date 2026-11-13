"""The history recovery contract: no network, no Gate 4, no deployment."""
from __future__ import annotations

import csv
import hashlib
import io
import json
import shutil
import zipfile

import pytest

from api.probes import finalizadas_history as history
from api.probes import finalizadas_observational as obs

REPOSITORY = "Rafael-Tinelli/widget-confiabilidade-seguradoras"
GIT_SHA = "a" * 40


def _manifest(month="2026-10", *, pending=False, quality=False):
    return {
        "schema": history.SCHEMA,
        "status": "experimental_no_publication",
        "months": {
            month: {
                "month": month,
                "zip_sha256": "f" * 64,
                "status": "requires_manual_review" if pending else "validated_experimental",
                "source_url": "https://www.consumidor.gov.br/pages/publicacao/externo/20261100000011098/download",
            },
        },
        "revisions": [month] if pending and not quality else [],
        "quality_alerts": [month] if pending and quality else [],
    }


def _run(run_id, attempt=1):
    return {
        "id": run_id, "run_attempt": attempt, "head_sha": GIT_SHA,
        "head_branch": "main", "status": "completed", "conclusion": "success",
    }


def _make_sealed(root, run_id, *, pending=False, quality=False, attempt=1):
    root.mkdir(parents=True, exist_ok=True)
    manifest = root / "source.json"
    manifest.write_text(
        json.dumps(_manifest(pending=pending, quality=quality)), encoding="utf-8"
    )
    sealed = root / "sealed"
    history.seal(manifest, sealed, run_id, attempt, GIT_SHA)
    return sealed


def _fake_api(monkeypatch, runs, artifacts):
    def fake_api(route):
        if "/workflows/" in route:
            return {"workflow_runs": runs}
        for run_id, named in artifacts.items():
            if f"/runs/{run_id}/artifacts" in route:
                return {"artifacts": [
                    {"name": name, "expired": False}
                    for name in named
                ]}
        raise AssertionError(f"Unexpected API route: {route}")

    monkeypatch.setattr(history, "_gh_json", fake_api)


def _download_from(monkeypatch, bundles):
    def fake_download(run_id, name, repo, target):
        assert repo == REPOSITORY
        archive = bundles[(run_id, name)]
        for source in archive.rglob("*"):
            if source.is_file():
                relative = source.relative_to(archive)
                dest = target / relative
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, dest)

    monkeypatch.setattr(history, "_gh_download", fake_download)


def test_seal_checks_origin_and_digest(tmp_path):
    sealed = _make_sealed(tmp_path, 101, pending=True)
    assert history.validate_artifact(sealed, _run(101), state_only=True)["revisions"]
    modified = json.loads((sealed / "manifest.json").read_text(encoding="utf-8"))
    modified["months"]["2026-10"]["status"] = "validated_experimental"
    (sealed / "manifest.json").write_text(json.dumps(modified), encoding="utf-8")
    with pytest.raises(history.HistoryError, match="digest"):
        history.validate_artifact(sealed, _run(101), state_only=True)


def test_fallback_to_older_success_is_explicit_gap(tmp_path, monkeypatch):
    sealed = _make_sealed(tmp_path / "case", 99, pending=True)
    name = "finalizadas-observational-state-99-a1"
    _fake_api(monkeypatch, [_run(100), _run(99)],
              {100: [], 99: [name]})
    _download_from(monkeypatch, {(99, name): sealed})
    destination = tmp_path / "restored"
    report = history.restore(REPOSITORY, destination)
    assert report["status"] == "restored_with_gap"
    assert report["skipped_successful_runs"] == [100]
    assert report["source_run_id"] == 99
    copied = history.validate_manifest(destination / "manifest.json")
    assert copied["months"]["2026-10"]["status"] == "requires_manual_review"


def test_malformed_newer_artifact_does_not_replace_older(tmp_path, monkeypatch):
    latest = _make_sealed(tmp_path / "new", 202)
    older = _make_sealed(tmp_path / "old", 201)
    (latest / "origin.json").unlink()
    newname = "finalizadas-observational-state-202-a1"
    oldname = "finalizadas-observational-state-201-a1"
    _fake_api(monkeypatch, [_run(202), _run(201)],
              {202: [newname], 201: [oldname]})
    _download_from(monkeypatch, {(202, newname): latest, (201, oldname): older})
    report = history.restore(REPOSITORY, tmp_path / "restored")
    assert report["source_run_id"] == 201
    assert report["skipped_successful_runs"] == [202]


def test_all_successful_runs_without_artifacts_fail_closed(tmp_path, monkeypatch):
    _fake_api(monkeypatch, [_run(101), _run(100)], {101: [], 100: []})
    with pytest.raises(history.HistoryError, match="No intact baseline"):
        history.restore(REPOSITORY, tmp_path / "restored")
    assert not (tmp_path / "restored" / "recovery.json").exists()


def test_first_main_run_is_explicit_bootstrap(tmp_path, monkeypatch):
    _fake_api(monkeypatch, [], {})
    report = history.restore(REPOSITORY, tmp_path / "restored")
    assert report["status"] == "bootstrap"
    assert not (tmp_path / "restored" / "manifest.json").exists()


def test_previous_quality_alert_is_valid_history(tmp_path):
    folder = _make_sealed(tmp_path, 12, pending=True, quality=True)
    state = history.validate_artifact(folder, _run(12), state_only=True)
    assert state["quality_alerts"] == ["2026-10"]


def test_legacy_artifact_must_be_complete(tmp_path):
    root = tmp_path / "legacy"
    root.mkdir()
    month = "2026-10"
    data = b"data_finalizacao;nome_fantasia\n2026-10-02;Alfa\n"
    archive = root / month / "finalizadas_2026-10.zip"
    archive.parent.mkdir()
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr("finalizadas_2026-10.csv", data)
    zh = history.digest(archive)
    ch = hashlib.sha256(data).hexdigest()
    manifest = _manifest(month)
    state = manifest["months"][month]
    state["zip_sha256"] = zh
    state["csv_sha256"] = ch
    (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (archive.parent / "validacao.json").write_text(
        json.dumps({"month": month, "zip_sha256": zh}), encoding="utf-8"
    )
    aggregate = {"meta": {"source_family": "finalizadas",
                          "month": month, "source_zip_sha256": zh}}
    (archive.parent / "agregado-experimental.json").write_text(
        json.dumps(aggregate), encoding="utf-8"
    )
    assert history.validate_artifact(root, _run(20), state_only=False)["months"]
    (archive.parent / "validacao.json").unlink()
    with pytest.raises(history.HistoryError, match="Incomplete legacy"):
        history.validate_artifact(root, _run(20), state_only=False)


def test_recovery_report_hash_verified_before_observation(tmp_path, monkeypatch):
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps({"publications": [
        {"month": "2026-10",
         "url": "https://www.consumidor.gov.br/pages/publicacao/externo/20261100000011098/download"}
    ]}), encoding="utf-8")
    previous = tmp_path / "previous.json"
    previous.write_text(json.dumps(_manifest()), encoding="utf-8")
    recovery = tmp_path / "recovery.json"
    recovery.write_text(json.dumps({
        "status": "restored", "manifest_sha256": "0" * 64,
    }), encoding="utf-8")
    with pytest.raises(obs.ObservationalError, match="Hash do manifesto"):
        obs.main([
            "--catalog", str(catalog), "--output-dir", str(tmp_path / "out"),
            "--previous-manifest", str(previous),
            "--recovery-report", str(recovery),
        ])


def test_history_gap_keeps_new_observation_pending(tmp_path, monkeypatch):
    monkeypatch.setattr(obs, "MIN_CSV_BYTES", 0)
    url = "https://www.consumidor.gov.br/pages/publicacao/externo/20261100000011098/download"
    row = dict.fromkeys(obs.COLUMNS, "")
    row.update({
        "data_finalizacao": "14/10/2026",
        "segmento_de_mercado": obs.TARGET, "nome_fantasia": "Alfa",
        "situacao": "Finalizada avaliada", "avaliacao_reclamacao": "Resolvida",
        "respondida": "Sim", "nota_do_consumidor": "5",
    })
    src = tmp_path / "input.zip"
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=obs.COLUMNS, delimiter=";")
    writer.writeheader()
    writer.writerow(row)
    with zipfile.ZipFile(src, "w") as z:
        z.writestr("finalizadas_2026-10.csv", buffer.getvalue().encode("utf-8-sig"))
    prior = _manifest()
    prior["months"]["2026-10"]["zip_sha256"] = obs.sha256(src)
    prior_path = tmp_path / "prior.json"
    prior_path.write_text(json.dumps(prior), encoding="utf-8")
    recovery = tmp_path / "recovery.json"
    recovery.write_text(json.dumps({
        "status": "restored_with_gap",
        "manifest_sha256": history.digest(prior_path),
    }), encoding="utf-8")
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps({"publications": [
        {"month": "2026-10", "url": url}
    ]}), encoding="utf-8")
    monkeypatch.setattr(obs, "download_direct", lambda _url, dest, http=None:
                        (shutil.copyfile(src, dest), obs.sha256(src))[1])
    out = tmp_path / "out"
    assert obs.main([
        "--catalog", str(catalog), "--output-dir", str(out),
        "--previous-manifest", str(prior_path),
        "--recovery-report", str(recovery),
    ]) == 0
    current = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
    assert current["baseline_state"] == "restored_with_gap"
    assert current["months"]["2026-10"]["status"] == "requires_manual_review"
    assert current["months"]["2026-10"]["history_gap"] is True
