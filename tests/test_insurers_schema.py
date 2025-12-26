import json
from pathlib import Path

INSURERS = Path("api/v1/insurers.json")


def test_insurers_json_exists_and_nonempty():
    assert INSURERS.exists(), "api/v1/insurers.json não encontrado. Rodou api/build_insurers.py?"
    assert INSURERS.stat().st_size > 50, "api/v1/insurers.json está vazio ou muito pequeno."


def test_insurers_schema_minimum():
    data = json.loads(INSURERS.read_text(encoding="utf-8"))

    assert data.get("schemaVersion") == "1.0.0"
    assert "generatedAt" in data
    assert "period" in data
    assert "sources" in data
    assert "insurers" in data
    assert "meta" in data
    assert data["meta"]["count"] == len(data["insurers"])
    assert data["meta"]["count"] > 0

    # valida 5 primeiros para evitar teste pesado
    for it in data["insurers"][:5]:
        assert "id" in it
        assert "name" in it
        assert it.get("segment") in {"S1", "S2", "S3", "S4"}
        assert isinstance(it.get("products"), list)
        assert "data" in it
        assert "premiums" in it["data"]
        assert "claims" in it["data"]
        assert "flags" in it
        assert isinstance(it["flags"].get("openInsuranceParticipant"), bool)
