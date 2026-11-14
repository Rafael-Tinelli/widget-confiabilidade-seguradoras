"""A05: source completeness is separate from ZIP/CSV structural validity."""
from __future__ import annotations

import csv
import io
import json
import shutil
import zipfile
from datetime import date
from pathlib import Path

import pytest

from api.probes import finalizadas_completeness as complete
from api.probes import finalizadas_history as history
from api.probes import finalizadas_observational as obs

URL = "https://www.consumidor.gov.br/pages/publicacao/externo/20261100000011098/download"
CODE = "20261100000011098"


def _row(day: int) -> dict[str, str]:
    row = dict.fromkeys(obs.COLUMNS, "")
    row.update({
        "data_finalizacao": f"{day:02d}/10/2026",
        "segmento_de_mercado": obs.TARGET,
        "nome_fantasia": "Alfa Seguros",
        "respondida": "Sim",
        "situacao": "Finalizada avaliada",
        "avaliacao_reclamacao": "Resolvida",
        "nota_do_consumidor": "5",
    })
    return row


def _zip(tmp_path: Path, days: list[int], *, times: int = 1) -> Path:
    text = io.StringIO(newline="")
    writer = csv.DictWriter(text, fieldnames=obs.COLUMNS, delimiter=";")
    writer.writeheader()
    rows = [_row(day) for day in days]
    for _ in range(times):
        writer.writerows(rows)
    archive = tmp_path / "source.zip"
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("finalizadas_2026-10.csv", text.getvalue().encode("utf-8-sig"))
    return archive


def _catalog(tmp_path: Path, archive: Path) -> Path:
    catalog = tmp_path / "catalog.json"
    catalog.write_text(json.dumps({"publications": [
        {"month": "2026-10", "url": URL},
    ]}), encoding="utf-8")
    return catalog


def test_a05_structurally_valid_partial_csv_must_raise_population_warning(
    tmp_path, monkeypatch,
):
    """A full CRC and CSV parsing do not rescue a source missing most dates."""
    monkeypatch.setattr(obs, "MIN_CSV_BYTES", 0)
    source = _zip(tmp_path, [1, 2, 3, 4, 5], times=200)
    aggregate, evidence = obs.aggregate_zip(source, "2026-10", URL)
    assert evidence["rows_total"] == 1_000
    assert aggregate["meta"]["lines_total"] == 1_000
    assert evidence["population_completeness"]["status"] == "not_demonstrated"
    assert evidence["population_completeness"]["calendar"]["last_day"] == 5
    assert evidence["population_completeness"]["calendar"]["missing_days"][-1] == 31
    assert evidence["population_completeness"]["warnings"] == [
        "calendar_coverage_gap_requires_review"
    ]
    assert evidence["status"] == "requires_manual_review"


def test_a05_full_calendar_coverage_is_still_not_proof_of_completeness(
    tmp_path, monkeypatch,
):
    monkeypatch.setattr(obs, "MIN_CSV_BYTES", 0)
    source = _zip(tmp_path, list(range(1, 32)), times=40)
    aggregate, evidence = obs.aggregate_zip(source, "2026-10", URL)
    proof = evidence["population_completeness"]
    assert proof["calendar"]["distinct_days"] == 31
    assert proof["calendar"]["missing_days"] == []
    assert proof["warnings"] == []
    assert proof["status"] == "not_demonstrated"
    assert proof["official_monthly_rows"] is None
    assert aggregate["meta"]["population_completeness_status"] == "not_demonstrated"
    assert evidence["status"] == "validated_experimental"


def test_a05_small_sparse_corpus_is_not_declared_complete(tmp_path, monkeypatch):
    monkeypatch.setattr(obs, "MIN_CSV_BYTES", 0)
    src = _zip(tmp_path, [14])
    _, evidence = obs.aggregate_zip(src, "2026-10", URL)
    assert evidence["population_completeness"]["status"] == "not_demonstrated"
    assert evidence["population_completeness"]["warnings"] == []


def test_a05_official_zip_size_and_local_hash_are_only_consistency_checks():
    base = complete.observed_population("2026-10", {1: 500, 31: 500}, 1_000)
    result = complete.compare_publication(
        base, zip_bytes=120, zip_sha256="a" * 64,
        publication={
            "url": URL, "publication_code": CODE,
            "announced_bytes": 999,
            "expected_sha256": "b" * 64,
        },
        previous={},
    )
    assert result["checks"]["published_zip_bytes_match"] is False
    assert result["checks"]["pinned_observation_sha256_match"] is False
    assert "published_zip_bytes_mismatch" in result["warnings"]
    assert "pinned_observation_sha256_mismatch" in result["warnings"]
    assert result["status"] == "not_demonstrated"
    assert result["official_monthly_rows"] is None


def test_a05_same_publication_changed_rows_signals_review_but_not_causation():
    base = complete.observed_population("2026-10", {1: 550, 31: 550}, 1_100)
    result = complete.compare_publication(
        base, zip_bytes=120, zip_sha256="b" * 64,
        publication={"url": URL, "publication_code": CODE,
                     "announced_bytes": 120},
        previous={"source_url": URL, "publication_code": CODE,
                  "rows_total": 1_200},
    )
    assert result["checks"]["previous_observation_rows_match"] is False
    assert "previous_observation_rows_changed" in result["warnings"]
    assert result["status"] == "not_demonstrated"


def test_a05_partial_csv_propagates_review_to_sealed_observation(
    tmp_path, monkeypatch,
):
    monkeypatch.setattr(obs, "MIN_CSV_BYTES", 0)
    source = _zip(tmp_path, [1, 2, 3, 4, 5], times=200)
    catalog = _catalog(tmp_path, source)
    monkeypatch.setattr(obs, "read_catalog", lambda *_a, **_k: [{
        "month": "2026-10", "url": URL, "expected_sha256": "",
        "publication_code": CODE, "announced_bytes": source.stat().st_size,
        "catalog_status": "listed", "published_at": "10/11/2026",
        "modified_at": "10/11/2026",
    }])

    def download(_url, dest, http=None):
        shutil.copyfile(source, dest)
        return obs.sha256(source)

    monkeypatch.setattr(obs, "download_direct", download)
    out = tmp_path / "isolated"
    assert obs.main(["--catalog", str(catalog), "--output-dir", str(out)]) == 0
    manifest = history.validate_manifest(out / "manifest.json")
    item = manifest["months"]["2026-10"]
    assert manifest["revisions"] == []  # partial-looking is NOT a proven source revision
    assert manifest["quality_alerts"] == ["2026-10"]
    assert item["status"] == "requires_manual_review"
    assert item["population_completeness"]["checks"]["published_zip_bytes_match"]
    assert item["population_completeness"]["warnings"] == [
        "calendar_coverage_gap_requires_review"
    ]
    assert item["acquisition_mode"] == "current_http_fetch"
    assert item["parser_version"] == obs.PARSER_CONTRACT_VERSION


def test_a05_review_stays_pending_if_same_partial_candidate_is_seen_again(
    tmp_path, monkeypatch,
):
    monkeypatch.setattr(obs, "MIN_CSV_BYTES", 0)
    source = _zip(tmp_path, [1, 2, 3, 4, 5], times=200)
    catalog = _catalog(tmp_path, source)
    monkeypatch.setattr(obs, "read_catalog", lambda *_a, **_k: [{
        "month": "2026-10", "url": URL, "expected_sha256": "",
        "publication_code": CODE, "announced_bytes": source.stat().st_size,
        "catalog_status": "listed",
    }])
    monkeypatch.setattr(obs, "download_direct", lambda _url, dest, http=None: (
        shutil.copyfile(source, dest), obs.sha256(source)
    )[1])
    first = tmp_path / "first"
    second = tmp_path / "second"
    assert obs.main(["--catalog", str(catalog), "--output-dir", str(first)]) == 0
    assert obs.main(["--catalog", str(catalog), "--output-dir", str(second),
                     "--previous-manifest", str(first / "manifest.json")]) == 0
    state = history.validate_manifest(second / "manifest.json")
    assert state["months"]["2026-10"]["status"] == "requires_manual_review"
    assert state["quality_alerts"] == ["2026-10"]


def test_a05_no_independent_row_total_even_when_size_and_hash_match():
    observed = complete.observed_population("2026-10", {1: 750, 31: 750}, 1_500)
    checks = complete.compare_publication(
        observed, zip_bytes=153, zip_sha256="a" * 64,
        publication={"url": URL, "publication_code": CODE,
                     "announced_bytes": 153, "expected_sha256": "a" * 64},
        previous={"source_url": URL, "publication_code": CODE,
                  "rows_total": 1_500},
    )
    assert all(value is True for value in checks["checks"].values())
    assert checks["warnings"] == ["calendar_coverage_gap_requires_review"]
    assert checks["status"] == "not_demonstrated"
    assert checks["official_monthly_rows"] is None
