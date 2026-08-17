import zipfile
from pathlib import Path

from api.sources.susep_groups import load_susep_economic_groups


def test_reads_latest_group_membership_from_history(tmp_path: Path):
    zip_path = tmp_path / "BaseCompleta.zip"
    content = (
        "damesano;coenti;noenti;cogrupo;nogrupo\n"
        "202401;03182;ITAU AUTO;00035;ITAÚ\n"
        "202606;03182;ITAU AUTO;00051;PORTO SEGURO\n"
        "202606;05886;PORTO SEGURO;00051;PORTO SEGURO\n"
        "202606;04367;PRUDENTIAL;01225;INDEPENDENTE\n"
    )
    with zipfile.ZipFile(zip_path, "w") as z:
        z.writestr("Ses_grupos_economicos.csv", content.encode("latin1"))

    records = load_susep_economic_groups(zip_path)
    by_fip = {item["fip_code"]: item for item in records}

    assert by_fip["003182"]["group_code"] == "00051"
    assert by_fip["003182"]["group_name"] == "PORTO SEGURO"
    assert by_fip["003182"]["observed_period"] == "202606"
    assert by_fip["003182"]["is_specific_group"] is True
    assert by_fip["005886"]["is_specific_group"] is True
    assert by_fip["004367"]["group_name"] == "INDEPENDENTE"
    assert by_fip["004367"]["is_specific_group"] is False
