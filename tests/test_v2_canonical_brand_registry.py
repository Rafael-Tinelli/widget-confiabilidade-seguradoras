import json
from pathlib import Path

from api.utils.name_cleaner import normalize_name_key
from api.v2.consumer_gov_identity import load_provider_resolution_registry
from api.v2.relationships import load_verified_relationship_registry

SANDBOX_BRAND_REGISTRY = Path("data/reference/v2/sandbox_brand_relationships.json")


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


def test_loovi_is_canonical_sandbox_risk_carrier_relationship():
    brand = _brands_by_id()["brand:loovi"]
    assert brand["name"] == "Loovi"
    assert "Loovi Seguros" in brand["aliases"]
    relation = brand["relationships"][0]
    assert relation["relationship_type"] == "risk_carrier"
    assert relation["target_cnpj"] == "47006254000180"
    assert relation["status"] == "current"
    assert relation["evidence"]["authority"] == "Loovi"


def test_azos_is_canonical_market_identity_with_excelsior_risk_carrier():
    brand = _brands_by_id()["brand:azos"]
    assert brand["name"] == "Azos"
    market = brand["market_identity"]
    assert market["kind"] == "insurtech_platform"
    assert market["cnpj"] == "39520039000175"
    assert "insurtech" in market["public_note"].lower()
    assert market["evidence"]["authority"] == "Azos"

    relation = brand["relationships"][0]
    assert relation["relationship_type"] == "risk_carrier"
    assert relation["target_cnpj"] == "33054826000192"
    assert relation["status"] == "current"
    assert "Excelsior" in relation["evidence"]["authority"]


def test_sandbox_brand_wrapper_cannot_drift_from_canonical_relationship():
    canonical = _brands_by_id()["brand:loovi"]
    canonical_relation = canonical["relationships"][0]
    wrapper = json.loads(SANDBOX_BRAND_REGISTRY.read_text(encoding="utf-8"))
    sandbox = next(
        item for item in wrapper["brands"] if item["brand_id"] == "brand:loovi"
    )

    assert sandbox["risk_carrier_cnpj"] == canonical_relation["target_cnpj"]
    assert sandbox["name"] == canonical["name"]
    assert set(sandbox["aliases"]) <= set(canonical["aliases"])
    assert sandbox["evidence"]


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
    registry = load_provider_resolution_registry()
    key = normalize_name_key("Metlife Seguros e Previdência Privada")
    assert key not in registry
