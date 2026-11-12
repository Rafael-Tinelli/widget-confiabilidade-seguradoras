from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from api.sources import consumer_gov_direct as direct


MONTH = "2026-03"


def _valid_row(month: str = MONTH) -> dict[str, str]:
    year, number = month.split("-")
    return {
        "Nome Fantasia": "Empresa Teste",
        "Segmento de Mercado": "Seguros, Capitalização e Previdência",
        "Respondida": "S",
        "Situação": "Finalizada avaliada",
        "Avaliação Reclamação": "Resolvida",
        "Nota do Consumidor": "4",
        "Tempo Resposta": "2",
        "Procurou Empresa": "S",
        "Área": "Serviços Financeiros",
        "Assunto": "Seguro",
        "Grupo Problema": "Contrato",
        "Problema": "Cobertura",
        "Como Comprou Contratou": "Internet",
        "Ano Abertura": year,
        "Mês Abertura": str(int(number)),
    }


def _write_csv(
    path: Path,
    rows: list[dict[str, str]],
    *,
    columns: list[str] | None = None,
) -> None:
    fields = columns or list(_valid_row())
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, delimiter=";")
        writer.writeheader()
        writer.writerows(rows)


@pytest.mark.parametrize(
    ("column", "value", "failure"),
    [
        ("Respondida", "Sim", direct.ConsumerGovSchemaMismatch),
        ("Respondida", "?", direct.ConsumerGovSchemaMismatch),
        ("Procurou Empresa", "Não", direct.ConsumerGovSchemaMismatch),
        ("Avaliação Reclamação", "Indefinida", direct.ConsumerGovSchemaMismatch),
        ("Nota do Consumidor", "6", direct.ConsumerGovSchemaMismatch),
        ("Nota do Consumidor", "nan", direct.ConsumerGovSchemaMismatch),
        ("Nota do Consumidor", "inf", direct.ConsumerGovSchemaMismatch),
        ("Tempo Resposta", "-0,1", direct.ConsumerGovSchemaMismatch),
        ("Tempo Resposta", "nan", direct.ConsumerGovSchemaMismatch),
        ("Mês Abertura", "4", direct.ConsumerGovMonthMismatch),
        ("Ano Abertura", "", direct.ConsumerGovMonthMismatch),
    ],
)
def test_invalid_values_fail_closed(
    tmp_path: Path, monkeypatch, column: str, value: str, failure: type[Exception]
) -> None:
    monkeypatch.setattr(direct, "MIN_MONTH_BYTES", 1)
    data = _valid_row()
    data[column] = value
    path = tmp_path / "sample.csv"
    _write_csv(path, [data])

    with pytest.raises(failure):
        direct.validate_month_csv(path, MONTH)


def test_date_of_finalization_does_not_substitute_opening_date(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(direct, "MIN_MONTH_BYTES", 1)
    data = _valid_row()
    del data["Ano Abertura"]
    del data["Mês Abertura"]
    data["Data Finalização"] = "03/03/2026"
    path = tmp_path / "sample.csv"
    _write_csv(path, [data])

    with pytest.raises(direct.ConsumerGovMonthMismatch, match="opening-date"):
        direct.validate_month_csv(path, MONTH)


def test_duplicate_normalized_headers_fail_closed(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(direct, "MIN_MONTH_BYTES", 1)
    path = tmp_path / "sample.csv"
    row = _valid_row()
    row["Situacao"] = row["Situação"]
    _write_csv(path, [row])
    with pytest.raises(direct.ConsumerGovSchemaMismatch, match="duplicate"):
        direct.validate_month_csv(path, MONTH)


def test_ragged_csv_row_fails_closed(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(direct, "MIN_MONTH_BYTES", 1)
    path = tmp_path / "sample.csv"
    _write_csv(path, [_valid_row()])
    with path.open("a", encoding="utf-8") as handle:
        handle.write("one;extra;field;too;many;for;any;valid;row;"
                     "a;b;c;d;e;f;g;h;extra\n")
    with pytest.raises(direct.ConsumerGovSchemaMismatch, match="extra or missing"):
        direct.validate_month_csv(path, MONTH)


def _publication(month: str, *, family: str = "base") -> direct.Publication:
    label = "Base Completa" if family == "base" else "Reclamações finalizadas"
    filename = (
        f"basecompleta{month}.zip"
        if family == "base"
        else f"finalizadas_{month}.zip"
    )
    return direct.Publication(
        code="20260400000012345",
        title=f"{label} {month}",
        filename=filename,
        published_at="2026-04-01",
        month=month,
        discovery_method="test",
    )


def test_finalizadas_cannot_replace_base_completa_selection() -> None:
    with pytest.raises(direct.ConsumerGovPublicationNotFound):
        direct._select_publication([_publication(MONTH, family="finalizadas")], MONTH)


def test_partial_multimonth_update_does_not_activate_any_month(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(direct, "MIN_MONTH_BYTES", 1)
    raw = tmp_path / "raw"
    raw.mkdir()
    manifest = raw / "manifest.json"
    old = raw / "basecompleta_2026-02.csv"
    _write_csv(old, [_valid_row("2026-02")])
    direct.ensure_months(["2026-02"], raw_dir=raw, manifest_path=manifest)
    before_manifest = manifest.read_bytes()
    before_old = old.read_bytes()

    months = ["2026-02", "2026-03", "2026-04"]
    monkeypatch.setattr(
        direct, "discover_publications",
        lambda wanted: [_publication(month) for month in sorted(wanted)],
    )

    def fake_download(_publication_arg, target, month):
        if month == "2026-04":
            raise direct.ConsumerGovDownloadFailed("last month offline")
        _write_csv(target, [_valid_row(month)])
        return direct.validate_month_csv(target, month)

    monkeypatch.setattr(direct, "download_publication", fake_download)
    with pytest.raises(direct.ConsumerGovDownloadFailed, match="offline"):
        direct.ensure_months(months, raw_dir=raw, manifest_path=manifest)

    assert manifest.read_bytes() == before_manifest
    assert old.read_bytes() == before_old
    active = json.loads(manifest.read_text(encoding="utf-8"))["months"]
    assert sorted(active) == ["2026-02"]
    assert not list(raw.glob(".consumer-gov-window-*"))
    # Immutable unreferenced candidates are inert: only the manifest is live.


def test_successful_multimonth_update_has_atomic_manifest_and_immutable_objects(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(direct, "MIN_MONTH_BYTES", 1)
    raw = tmp_path / "raw"
    raw.mkdir()
    manifest = raw / "manifest.json"
    months = ["2026-02", "2026-03"]
    monkeypatch.setattr(
        direct, "discover_publications",
        lambda wanted: [_publication(month) for month in sorted(wanted)],
    )

    def fake_download(_publication_arg, target, month):
        _write_csv(target, [_valid_row(month)])
        return direct.validate_month_csv(target, month)

    monkeypatch.setattr(direct, "download_publication", fake_download)
    result = direct.ensure_months(months, raw_dir=raw, manifest_path=manifest)
    active = json.loads(manifest.read_text(encoding="utf-8"))["months"]
    assert sorted(active) == months
    for month in months:
        selected = result[month]["path"]
        assert selected.parent == raw / "objects"
        assert selected.is_file()
        assert active[month]["source_family"] == "base_completa"
        assert active[month]["reference_event"] == "abertura"
        assert active[month]["object_path"] == f"objects/{selected.name}"
        assert direct._manifest_cache_hit(selected, month, active[month])

    def unexpected_discovery(_wanted):
        raise AssertionError("complete manifest must not trigger discovery")

    monkeypatch.setattr(direct, "discover_publications", unexpected_discovery)
    repeated = direct.ensure_months(months, raw_dir=raw, manifest_path=manifest)
    assert all(row["acquisition"] == "cache_manifest" for row in repeated.values())


def test_orphan_object_never_enters_selected_window(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(direct, "MIN_MONTH_BYTES", 1)
    raw = tmp_path / "raw"
    raw.mkdir()
    path = raw / f"basecompleta_{MONTH}.csv"
    _write_csv(path, [_valid_row()])
    manifest = raw / "manifest.json"
    direct.ensure_months([MONTH], raw_dir=raw, manifest_path=manifest)
    objects = raw / "objects"
    objects.mkdir()
    (objects / ("a" * 64 + ".csv")).write_bytes(b"not a real month")
    selected = direct.ensure_months([MONTH], raw_dir=raw, manifest_path=manifest)
    assert selected[MONTH]["path"] == path
    assert selected[MONTH]["acquisition"] == "cache_manifest"
