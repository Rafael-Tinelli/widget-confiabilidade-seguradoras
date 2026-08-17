from __future__ import annotations

import zipfile
from pathlib import Path

from api.sources.susep_financial_evidence import load_susep_financial_evidence


def _write_zip(path: Path) -> None:
    with zipfile.ZipFile(path, "w") as z:
        z.writestr(
            "Ses_pl_margem.csv",
            "coenti;damesano;plajustado;margem;pl;AjustesContabeis;"
            "AjustesEconomicos;NovoPla;CMR\n"
            "1;202605;1.234,56;100;1200;0;0;1234,56;900\n"
            "1;202606;-100,50;100;1200;0;0;-100,50;800\n"
            "2;202606;;100;1200;0;0;;700\n",
        )
        z.writestr(
            "SES_Balanco.csv",
            "coenti;damesano;cmpid;valor;seq;quadro\n"
            "1;202606;1479;1000;1;22A\n"
            "1;202606;11160;10;2;22A\n"
            "1;202606;351;5;3;22A\n"
            "1;202606;1040;500;4;22P\n"
            "1;202606;331;100;5;22A\n"
            "1;202606;11187;0;6;22A\n"
            "1;202606;5503;0;7;22A\n"
            "1;202606;6449;50;8;22P\n"
            "2;202606;1479;100;1;22A\n"
            "2;202606;1040;50;2;22P\n",
        )
        z.writestr(
            "Ses_seguros.csv",
            "damesano;coenti;premio_ganho\n"
            "202605;1;10\n"
            "202606;1;20\n"
            "202606;2;0\n",
        )


def test_reader_preserves_missing_negative_and_formula_components(tmp_path: Path) -> None:
    path = tmp_path / "BaseCompleta.zip"
    _write_zip(path)

    payload = load_susep_financial_evidence(["000001", "000002"], path)

    assert payload["reference_periods"] == {
        "capital": 202606,
        "balance": 202606,
        "insurance_operations": 202606,
    }
    one = payload["entities"]["000001"]
    assert one["capital_history"][202605]["pla_adjusted"] == 1234.56
    assert one["capital_history"][202606]["pla_adjusted"] == -100.5
    assert one["capital_history"][202606]["cmr"] == 800.0
    assert set(one["balance_values"][202606]) >= {1479, 11160, 351, 1040}
    assert one["nonzero_premium_periods"] == {202605, 202606}

    two = payload["entities"]["000002"]
    assert two["capital_history"][202606]["pla_adjusted"] is None
    assert two["capital_history"][202606]["cmr"] == 700.0
    assert two["nonzero_premium_periods"] == set()
