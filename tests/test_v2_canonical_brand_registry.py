from json import loads
from pathlib import Path

from api.v2.relationships import load_verified_relationship_registry


CONSUMER_REGISTRY = Path("data/reference/v2/consumer_gov_provider_resolutions.json")


def _brands_by_id():
    registry = load_verified_relationship_registry()
    return {item["brand_id"]: item for item in registry["brands"]}


def test_bb_seguro_auto_is_canonical_risk_carrier_relationship():
    brand = _brands_by_id()["brand:bb-seguro-auto"]
    assert brand["name"] == "BB Seguro Auto"
    relation = brand["relationships"][0]
    assert relation["relationship_type"] == "risk_carrier"
    assert relation["target_cnpj"] == "61074175000138"
    assert relation["status"] == "current"
    assert relation["evidence"]["authority"] == "BB Seguros"


def test_caixa_residencial_is_canonical_brand_relationship():
    brand = _brands_by_id()["brand:caixa-residencial"]
    assert "XS3 Seguros" in brand["aliases"]
    relation = brand["relationships"][0]
    assert relation["relationship_type"] == "brand_of"
    assert relation["target_cnpj"] == "38155802000143"


def test_obvious_trade_names_live_in_canonical_registry():
    brands = _brands_by_id()
    expected = {
        "brand:axa": "AXA Seguros",
        "brand:aig-brasil": "AIG Seguros",
        "brand:sulamerica-pessoas-previdencia": "SulAmérica Seguros de Pessoas e Previdência",
        "brand:bp-seguradora": "BP Seguradora",
        "brand:mag-seguros": "Mongeral Aegon Seguros e Previdência - MAG",
        "brand:metlife-brasil": "Metlife Seguros e Previdência Privada",
        "brand:zurich-santander-vida-previdencia": "Zurich Santander Vida e Previdência",
        "brand:cardif-vida": "Cardif Vida",
    }
    for brand_id, label in expected.items():
        brand = brands[brand_id]
        assert label == brand["name"] or label in brand["aliases"]
        assert brand["relationships"][0]["evidence"]


def test_consumer_registry_does_not_duplicate_reusable_metlife_alias():
    payload = loads(CONSUMER_REGISTRY.read_text(encoding="utf-8"))
    provider_names = {row["provider_name"] for row in payload["resolutions"]}
    assert "Metlife Seguros e Previdência Privada" not in provider_names
    assert "source-specific" in payload["note"]
