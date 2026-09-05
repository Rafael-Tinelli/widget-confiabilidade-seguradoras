import zipfile
from pathlib import Path

from api.sources.susep_groups import load_susep_economic_groups


def test_reads_latest_group_membership_from_history(tmp_path: Path):
    zip_path = tmp_path / "BaseCompleta.zip"
    content = (
        "damesano;coenti;noenti;cogrupo;nogrupo\n"
        "202401;03182;ITAU AUTO;00035;ITAÚ\n"
        "202402;03182;ITAU AUTO;00035;ITAÚ\n"
        "202605;03182;ITAU AUTO;00051;PORTO SEGURO\n"
        "202606;03182;ITAU AUTO;00051;PORTO SEGURO\n"
        "202606;05886;PORTO SEGURO;00051;PORTO SEGURO\n"
        "202606;04367;PRUDENTIAL;01225;INDEPENDENTE\n"
    )
    with zipfile.ZipFile(zip_path, "w") as z:
        z.writestr("Ses_grupos_economicos.csv", content.encode("latin1"))

    records = load_susep_economic_groups(zip_path)
    by_fip = {item["fip_code"]: item for item in records}

    itau = by_fip["003182"]
    assert itau["group_code"] == "00051"
    assert itau["group_name"] == "PORTO SEGURO"
    assert itau["observed_period"] == "202606"
    assert itau["is_specific_group"] is True
    assert itau["group_history"] == [
        {
            "group_code": "00035",
            "group_name": "ITAÚ",
            "from_period": "202401",
            "to_period": "202402",
            "is_specific_group": True,
        },
        {
            "group_code": "00051",
            "group_name": "PORTO SEGURO",
            "from_period": "202605",
            "to_period": "202606",
            "is_specific_group": True,
        },
    ]
    assert by_fip["005886"]["is_specific_group"] is True
    assert by_fip["004367"]["group_name"] == "INDEPENDENTE"
    assert by_fip["004367"]["is_specific_group"] is False
