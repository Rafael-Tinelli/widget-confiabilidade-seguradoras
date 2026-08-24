from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from api.sources.susep_conduct_exposure import (
    load_susep_conduct_exposure,
    probe_susep_conduct_exposure,
)


def _write_zip(path: Path, *, include_branch: bool = True) -> None:
    columns = ["damesano", "coenti"]
    if include_branch:
        columns.append("coramo")
    columns.extend(["premio_direto", "premio_ganho"])
    rows = [
        ["202601", "1", "1001", "100,00", "90,00"],
        ["202601", "1", "1001", "50,00", "45,00"],
        ["202601", "1", "2001", "25,00", "20,00"],
        ["202602", "1", "1001", "200,00", "180,00"],
        ["202601", "2", "1001", "999,00", "900,00"],
    ]
    if not include_branch:
        rows = [[row[0], row[1], row[3], row[4]] for row in rows]
    payload = ";".join(columns) + "\n" + "\n".join(";".join(row) for row in rows) + "\n"
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("Ses_seguros.csv", payload.encode("latin1"))


def test_probe_accepts_required_ses_conduct_fields(tmp_path: Path) -> None:
    path = tmp_path / "BaseCompleta.zip"
    _write_zip(path)
    probe = probe_susep_conduct_exposure(path)
    assert probe["state"] == "available"
    assert probe["branch_dimension"] == "coramo"
    assert probe["candidate_denominators"] == ["premium_direct", "premium_earned"]


def test_probe_fails_closed_when_branch_dimension_is_missing(tmp_path: Path) -> None:
    path = tmp_path / "BaseCompleta.zip"
    _write_zip(path, include_branch=False)
    probe = probe_susep_conduct_exposure(path)
    assert probe["state"] == "schema_incompatible"
    assert probe["missing_columns"] == ["coramo"]


def test_loader_preserves_month_and_branch_mix_without_selecting_denominator(
    tmp_path: Path,
) -> None:
    path = tmp_path / "BaseCompleta.zip"
    _write_zip(path)
    payload = load_susep_conduct_exposure(["000001"], path)

    assert payload["reference_period"] == 202602
    jan = payload["entities"]["000001"]["months"][202601]
    assert jan["premium_direct"] == pytest.approx(175.0)
    assert jan["premium_earned"] == pytest.approx(155.0)
    assert jan["branches"][1001]["premium_direct"] == pytest.approx(150.0)
    assert jan["branches"][2001]["premium_direct"] == pytest.approx(25.0)
    assert payload["source"]["denominator_selected"] is None
    assert payload["source"]["scoring"] == "forbidden_in_source_artifact"


def test_loader_filters_to_requested_fip(tmp_path: Path) -> None:
    path = tmp_path / "BaseCompleta.zip"
    _write_zip(path)
    payload = load_susep_conduct_exposure(["000001"], path)
    assert set(payload["entities"]) == {"000001"}
