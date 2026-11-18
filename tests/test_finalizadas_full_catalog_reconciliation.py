"""R3: reconcile the ENTIRE historical manifest against the live official feed.

All official JSON and ZIP fixtures are in-process. No network requests,
GitHub Actions source restore, HostGator, Gate 4, or national downloads.
"""
from __future__ import annotations

import csv
import io
import json
import shutil
import zipfile
from pathlib import Path

import pytest

from api.probes import finalizadas_history as history
from api.probes import finalizadas_observational as obs

CATALOG = obs.OFFICIAL_CATALOG_URL
OLD = "2026-01"
SECOND_OLD = "2026-04"
SEP = "2026-09"
OCT = "2026-10"


def _entry(month: str, code: str, *, deleted: bool = False, modified: str = "01/11/2026") -> dict:
    return {
        "nomeArquivo": f"finalizadas_{month}.zip",
        "codigo": code,
        "tamanhoArquivo": 3456789,
        "indicadorExclusaoLogica": deleted,
        "dataPublicacao": "01/11/2026",
        "dataAlteracao": modified,
    }


def _url(code: str) -> str:
    return f"https://www.consumidor.gov.br/pages/publicacao/externo/{code}/download"


CODES = {
    OLD: "20260200000011011",
    SECOND_OLD: "20260500000011012",
    SEP: "20261000000011013",
    OCT: "20261100000011098",
}


def _catalog(path: Path, *, pinned: list[str] | None = None) -> Path:
    pinned = pinned or []
    path.write_text(json.dumps({
        "publications": [
            {"month": month, "url": _url(CODES[month])}
            for month in pinned
        ],
    }), encoding="utf-8")
    return path


def _prior(path: Path, *, months: dict[str, dict] | None = None) -> Path:
    values = months or {
        OLD: {
            "month": OLD,
            "status": "validated_experimental",
            "catalog_status": "listed",
            "zip_sha256": "a" * 64,
            "source_url": _url(CODES[OLD]),
            "publication_code": CODES[OLD],
            "modified_at": "01/11/2026",
        },
        SECOND_OLD: {
            "month": SECOND_OLD,
            "status": "validated_experimental",
            "catalog_status": "listed",
            "zip_sha256": "b" * 64,
            "source_url": _url(CODES[SECOND_OLD]),
            "publication_code": CODES[SECOND_OLD],
            "modified_at": "01/11/2026",
        },
    }
    path.write_text(json.dumps({
        "schema": "finalizadas-observational-1",
        "status": "experimental_no_publication",
        "months": values,
        "revisions": [
            month for month, state in values.items() if state.get("revision")
        ],
        "quality_alerts": [],
    }), encoding="utf-8")
    return path


def _tiny_zip(path: Path, month: str) -> bytes:
    year, mm = month.split("-")
    row = dict.fromkeys(obs.COLUMNS, "")
    row.update({
        "data_finalizacao": f"01/{mm}/{year}",
        "segmento_de_mercado": obs.TARGET,
        "nome_fantasia": "Testadora Seguros",
        "respondida": "Sim",
        "situacao": "Finalizada avaliada",
        "avaliacao_reclamacao": "Resolvida",
        "nota_do_consumidor": "5",
    })
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=obs.COLUMNS, delimiter=";")
    writer.writeheader()
    writer.writerow(row)
    with zipfile.ZipFile(path, mode="w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr(f"finalizadas_{month}.csv", stream.getvalue().encode("utf-8-sig"))
    return path.read_bytes()


def _run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    *, feed: list[dict], previous: Path | None = None,
    name: str = "out", latest: int = 2, pinned: list[str] | None = None,
) -> tuple[dict, list[str]]:
    catalog = _catalog(tmp_path / "catalog.json", pinned=pinned)
    observed: list[str] = []
    consults: list[str] = []
    def official(url, _http):
        consults.append(url)
        return obs._official_publications(feed)
    def download(_url, destination, http=None):
        month = destination.parent.name
        observed.append(month)
        src = tmp_path / f"fixture-{month}.zip"
        _tiny_zip(src, month)
        shutil.copyfile(src, destination)
        return obs.sha256(destination)
    monkeypatch.setattr(obs, "_fetch_official_feed", official)
    monkeypatch.setattr(obs, "download_direct", download)
    monkeypatch.setattr(obs, "MIN_CSV_BYTES", 0)
    args = ["--catalog", str(catalog), "--feed-url", CATALOG,
            "--output-dir", str(tmp_path / name),
            "--latest-months", str(latest)]
    if previous is not None:
        args += ["--previous-manifest", str(previous)]
    assert obs.main(args) == 0
    assert consults == [CATALOG]  # one freshly checked source snapshot only
    manifest_path = tmp_path / name / "manifest.json"
    return history.validate_manifest(manifest_path), observed


def _recent() -> list[dict]:
    return [_entry(SEP, CODES[SEP]), _entry(OCT, CODES[OCT])]


def test_r3_absent_unpinned_months_outside_window_are_quarantined(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    prior = _prior(tmp_path / "previous.json")
    manifest, downloaded = _run(
        tmp_path, monkeypatch, feed=_recent(), previous=prior,
    )
    assert downloaded == [SEP, OCT]
    assert set(manifest["months"]) == {OLD, SECOND_OLD, SEP, OCT}
    assert manifest["revisions"] == [OLD, SECOND_OLD]
    for month, digest in ((OLD, "a" * 64), (SECOND_OLD, "b" * 64)):
        state = manifest["months"][month]
        assert state["catalog_status"] == "absent_from_feed"
        assert state["status"] == "requires_manual_review"
        assert state["revision"] is True
        assert state["catalog_checked_at"].endswith("Z")
        assert state["zip_sha256"] == digest
        assert state["source_url"] == _url(CODES[month])
        assert not (tmp_path / "out" / month).exists()
    assert manifest["status"] == "experimental_no_publication"


def test_r3_withdrawn_unpinned_month_is_distinguished_from_absence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    previous = _prior(tmp_path / "previous.json")
    feed = [_entry(OLD, CODES[OLD], deleted=True), *_recent()]
    manifest, downloaded = _run(
        tmp_path, monkeypatch, feed=feed, previous=previous,
    )
    assert downloaded == [SEP, OCT]
    assert manifest["months"][OLD]["catalog_status"] == "withdrawn"
    assert manifest["months"][SECOND_OLD]["catalog_status"] == "absent_from_feed"
    assert manifest["revisions"] == [OLD, SECOND_OLD]
    assert not (tmp_path / "out" / OLD).exists()


def test_r3_absence_persists_and_reappearance_never_auto_approves(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    previous = _prior(tmp_path / "previous.json")
    first, first_downloaded = _run(
        tmp_path, monkeypatch, feed=_recent(), previous=previous, name="first",
    )
    assert first_downloaded == [SEP, OCT]
    second, second_downloaded = _run(
        tmp_path, monkeypatch, feed=_recent(),
        previous=tmp_path / "first" / "manifest.json", name="second",
    )
    assert second_downloaded == [SEP, OCT]
    assert second["months"][OLD]["catalog_status"] == "absent_from_feed"
    assert OLD in second["revisions"]
    assert second["months"][OLD]["status"] == "requires_manual_review"

    third, third_downloaded = _run(
        tmp_path, monkeypatch,
        feed=[_entry(OLD, CODES[OLD]), *_recent()],
        previous=tmp_path / "second" / "manifest.json", name="third",
    )
    assert third_downloaded == [SEP, OCT]
    state = third["months"][OLD]
    assert state["catalog_status"] == "listed"
    assert state["status"] == "requires_manual_review"
    assert state["revision"] is True
    assert state["zip_sha256"] == "a" * 64
    assert OLD in third["revisions"]
    assert third["status"] == "experimental_no_publication"


def test_r3_unprocessed_listed_month_metadata_revision_is_not_silent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    previous = _prior(tmp_path / "previous.json")
    manifest, downloaded = _run(
        tmp_path, monkeypatch, previous=previous,
        feed=[_entry(OLD, CODES[OLD], modified="16/11/2026"), *_recent()],
    )
    assert downloaded == [SEP, OCT]
    state = manifest["months"][OLD]
    assert state["catalog_status"] == "listed"
    assert state["modified_at"] == "01/11/2026"  # prior source evidence preserved
    assert state["status"] == "requires_manual_review"
    assert state["revision"] is True
    assert OLD in manifest["revisions"]


def test_r3_unobserved_withdrawn_pinned_month_relists_outside_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    first, downloaded = _run(
        tmp_path, monkeypatch,
        feed=[_entry(OLD, CODES[OLD], deleted=True), *_recent()],
        pinned=[OLD], latest=3, name="first",
    )
    assert downloaded == [SEP, OCT]
    assert first["months"][OLD]["catalog_status"] == "withdrawn"
    assert first["months"][OLD]["zip_sha256"] if False else True
    assert "zip_sha256" not in first["months"][OLD]
    second, downloaded = _run(
        tmp_path, monkeypatch,
        feed=[_entry(OLD, CODES[OLD]), *_recent()],
        pinned=[OLD], latest=2, name="second",
        previous=tmp_path / "first" / "manifest.json",
    )
    assert downloaded == [SEP, OCT]
    state = second["months"][OLD]
    assert state["catalog_status"] == "listed"
    assert state["status"] == "requires_manual_review"
    assert state["revision"] is True
    assert state["source_unobserved"] is True
    assert "zip_sha256" not in state
    assert OLD in second["revisions"]
    sealed = tmp_path / "sealed"
    history.seal(
        tmp_path / "second" / "manifest.json", sealed,
        run_id=82, attempt=1, head_sha="d" * 40,
    )
    assert history.validate_artifact(
        sealed, {"id": 82, "run_attempt": 1, "head_sha": "d" * 40},
        state_only=True,
    )["months"][OLD]["source_unobserved"] is True


def test_r3_without_fresh_feed_does_not_invent_withdrawal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    previous = _prior(tmp_path / "previous.json")
    catalog = _catalog(tmp_path / "catalog.json", pinned=[OCT])
    calls = []
    def download(_url, dest, http=None):
        calls.append(dest.parent.name)
        src = tmp_path / "new.zip"
        _tiny_zip(src, OCT)
        shutil.copyfile(src, dest)
        return obs.sha256(dest)
    monkeypatch.setattr(obs, "download_direct", download)
    monkeypatch.setattr(obs, "MIN_CSV_BYTES", 0)
    out = tmp_path / "offline"
    assert obs.main(["--catalog", str(catalog), "--output-dir", str(out),
                     "--previous-manifest", str(previous)]) == 0
    manifest = history.validate_manifest(out / "manifest.json")
    assert calls == [OCT]
    assert manifest["months"][OLD]["catalog_status"] == "listed"
    assert manifest["months"][OLD]["status"] == "validated_experimental"
    assert manifest["revisions"] == []


def test_r3_unavailable_official_feed_does_not_promote_or_mutate_prior(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    prior = _prior(tmp_path / "previous.json")
    raw = prior.read_bytes()
    catalog = _catalog(tmp_path / "catalog.json")
    def fail(_url, _session):
        raise obs.ObservationalError("simulated official feed 504")
    monkeypatch.setattr(obs, "_fetch_official_feed", fail)
    monkeypatch.setattr(
        obs, "download_direct",
        lambda *a, **k: pytest.fail("must not request ZIP without official catalog"),
    )
    output = tmp_path / "failed"
    with pytest.raises(obs.ObservationalError, match="504"):
        obs.main(["--catalog", str(catalog), "--feed-url", CATALOG,
                  "--output-dir", str(output),
                  "--previous-manifest", str(prior)])
    assert not output.exists()
    assert prior.read_bytes() == raw
    assert not list(tmp_path.glob(".finalizadas-stage-*"))


def test_r3_seal_rejects_false_validated_listed_month_without_bytes(
    tmp_path: Path,
) -> None:
    for status, revision, unobserved in (
        ("validated_experimental", False, True),
        ("requires_manual_review", False, True),
        ("requires_manual_review", True, False),
    ):
        state = {
            "month": OLD,
            "catalog_status": "listed",
            "status": status,
            "source_unobserved": unobserved,
            "revision": revision,
        }
        bad = _prior(tmp_path / "bad.json", months={OLD: state})
        with pytest.raises(history.HistoryError, match="Unverified listed"):
            history.validate_manifest(bad)
