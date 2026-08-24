from __future__ import annotations

import zipfile
from pathlib import Path

import pytest

from api.sources.susep_conduct_exposure import (
    load_susep_conduct_exposure,
    probe_susep_conduct_exposure,
)


def _write_zip(
    path: Path,
    *,
    include_branch: bool = True,
    include_pension: bool = True,
) -> None:
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
    insurance = ";".join(columns) + "\n" + "\n".join(";".join(row) for row in rows) + "\n"

    pension = (
        "coenti;damesano;tipoProd;contrib;benef\n"
        "1;202601;VGBL;30,00;0,00\n"
        "1;202601;PGBL;20,00;0,00\n"
        "1;202602;VGBL;40,00;0,00\n"
        "2;202601;VGBL;999,00;0,00\n"
    )

    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("Ses_seguros.csv", insurance.encode("latin1"))
        if include_pension:
            z.writestr("Ses_Contrib_Benef.csv", pension.encode("latin1"))


def test_probe_accepts_required_ses_conduct_fields(tmp_path: Path) -> None:
    path = tmp_path / "BaseCompleta.zip"
    _write_zip(path)
    probe = probe_susep_conduct_exposure(path)
    assert probe["state"] == "available"
    assert probe["dimensions"]["insurance_branch"] == "coramo"
    assert probe["dimensions"]["pension_product"] == "tipoProd"
    assert probe["candidate_exposure_components"] == [
        "insurance_premium_direct",
        "insurance_premium_earned",
        "pension_contributions",
    ]


def test_probe_fails_closed_when_branch_dimension_is_missing(tmp_path: Path) -> None:
    path = tmp_path / "BaseCompleta.zip"
    _write_zip(path, include_branch=False)
    probe = probe_susep_conduct_exposure(path)
    assert probe["state"] == "schema_incompatible"
    assert probe["missing_columns"]["insurance"] == ["coramo"]


def test_probe_fails_closed_when_pension_component_is_missing(tmp_path: Path) -> None:
    path = tmp_path / "BaseCompleta.zip"
    _write_zip(path, include_pension=False)
    probe = probe_susep_conduct_exposure(path)
    assert probe["state"] == "source_invalid"


def test_loader_preserves_components_without_combining_denominator(tmp_path: Path) -> None:
    path = tmp_path / "BaseCompleta.zip"
    _write_zip(path)
    payload = load_susep_conduct_exposure(["000001"], path)

    assert payload["reference_periods"] == {
        "insurance": 202602,
        "pension": 202602,
    }
    jan = payload["entities"]["000001"]["months"][202601]
    assert jan["insurance_premium_direct"] == pytest.approx(175.0)
    assert jan["insurance_premium_earned"] == pytest.approx(155.0)
    assert jan["pension_contributions"] == pytest.approx(50.0)
    assert jan["insurance_branches"][1001]["premium_direct"] == pytest.approx(150.0)
    assert jan["insurance_branches"][2001]["premium_direct"] == pytest.approx(25.0)
    assert jan["pension_products"]["VGBL"]["contributions"] == pytest.approx(30.0)
    assert jan["pension_products"]["PGBL"]["contributions"] == pytest.approx(20.0)
    assert payload["source"]["denominator_selected"] is None
    assert payload["source"]["combination_policy"] == "not_calibrated"
    assert payload["source"]["scoring"] == "forbidden_in_source_artifact"


def test_loader_filters_to_requested_fip(tmp_path: Path) -> None:
    path = tmp_path / "BaseCompleta.zip"
    _write_zip(path)
    payload = load_susep_conduct_exposure(["000001"], path)
    assert set(payload["entities"]) == {"000001"}
