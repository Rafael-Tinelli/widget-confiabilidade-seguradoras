import zipfile
from pathlib import Path

from api.sources.susep_groups import load_susep_economic_groups


def test_reads_group_membership_from_ses_cias(tmp_path: Path):
    zip_path = tmp_path / "BaseCompleta.zip"
    content = (
        "Coenti;Noenti;Cogrupo;Nogrupo\n"
        "4367;PRUDENTIAL DO BRASIL SEGUROS S.A.;77;GRUPO PRUDENTIAL\n"
        "5886;PORTO SEGURO COMPANHIA DE SEGUROS GERAIS;88;GRUPO PORTO\n"
    )
    with zipfile.ZipFile(zip_path, "w") as z:
        z.writestr("Ses_cias.csv", content.encode("latin1"))

    records = load_susep_economic_groups(zip_path)

    assert records[0]["fip_code"] == "004367"
    assert records[0]["group_code"] == "77"
    assert records[0]["group_name"] == "GRUPO PRUDENTIAL"
    assert records[1]["fip_code"] == "005886"
