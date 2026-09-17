from __future__ import annotations

import json
from pathlib import Path

REGISTRY_PATH = Path("data/reference/v2/verified_relationships.json")

COLD_START_IDS = {
    "brand:affinity-seguro-viagem",
    "brand:assistente-de-viagem",
    "brand:allsafe",
    "brand:assist-card",
    "brand:ituran",
    "brand:universal-assistance",
    "brand:vital-card",
}

EXPECTED_MARKET_CNPJS = {
    "brand:affinity-seguro-viagem": "17846647000131",
    "brand:assistente-de-viagem": "13711169000100",
    "brand:allsafe": "33897006000162",
    "brand:assist-card": "00027571000110",
    "brand:ituran": "16613061000164",
    "brand:universal-assistance": "07139957000162",
    "brand:vital-card": "04628135000157",
}

EXPECTED_CARRIERS = {
    "brand:affinity-seguro-viagem": "87163234000138",
    "brand:allsafe": "43505273000109",
    "brand:assist-card": "17341270000169",
    "brand:universal-assistance": "17197385000121",
    "brand:vital-card": "67865360000127",
}


def _registry() -> dict:
    return json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))


def _brand_index(payload: dict) -> dict[str, dict]:
    brands = payload.get("brands") or []
    ids = [str(row.get("brand_id") or "") for row in brands]
    assert all(ids)
    assert len(ids) == len(set(ids)), "brand_id must remain globally unique"
    return {str(row["brand_id"]): row for row in brands}


def test_cold_start_entries_are_unique_and_durable_registry_members() -> None:
    payload = _registry()
    brands = _brand_index(payload)

    assert payload["schema_version"] == "1.4"
    assert COLD_START_IDS <= brands.keys()
    assert "brand:loovi" in brands
    assert "brand:azos" in brands

    note = str(payload.get("note") or "")
    assert "cold-start" in note.casefold()
    assert "does not depend on Sanida query volume" in note

    cnpjs = []
    for brand_id in COLD_START_IDS:
        market = brands[brand_id].get("market_identity") or {}
        assert market.get("cnpj") == EXPECTED_MARKET_CNPJS[brand_id]
        cnpjs.append(market["cnpj"])
    assert len(cnpjs) == len(set(cnpjs))


def test_insurers_are_relationship_targets_not_new_cold_start_brands() -> None:
    brands = _brand_index(_registry())

    # These regulated entities must continue to come from the SUSEP entity universe,
    # not be duplicated as new market-identity brand records by this cold start.
    forbidden_new_ids = {
        "brand:sabemi",
        "brand:split-risk",
        "brand:starr",
        "brand:zurich-minas-brasil",
        "brand:allseg",
    }
    assert not (COLD_START_IDS & forbidden_new_ids)
    assert not (forbidden_new_ids & brands.keys())

    for brand_id, carrier_cnpj in EXPECTED_CARRIERS.items():
        relations = brands[brand_id].get("relationships") or []
        risk_relations = [
            row for row in relations if row.get("relationship_type") == "risk_carrier"
        ]
        assert len(risk_relations) == 1
        assert risk_relations[0].get("target_cnpj") == carrier_cnpj
        assert risk_relations[0].get("status") == "current"


def test_multi_provider_marketplaces_do_not_get_a_fake_global_carrier() -> None:
    brands = _brand_index(_registry())

    assistente = brands["brand:assistente-de-viagem"]
    assert assistente.get("relationships") == []
    assistente_market = assistente["market_identity"]
    assert assistente_market["distribution_role"] == "insurance_marketplace_consolidator"
    assert assistente_market["distribution_role_state"] == "verified"

    ituran = brands["brand:ituran"]
    assert ituran.get("relationships") == []
    ituran_market = ituran["market_identity"]
    assert ituran_market["distribution_role_state"] == "unverified"
    assert not ituran_market.get("distribution_role")


def test_affinity_travel_does_not_claim_the_ambiguous_plain_affinity_alias() -> None:
    brands = _brand_index(_registry())
    affinity = brands["brand:affinity-seguro-viagem"]

    assert affinity["name"] == "Affinity Seguro Viagem"
    assert "Affinity Viagem" in affinity.get("aliases", [])
    assert "Affinity" not in affinity.get("aliases", [])
    assert "brand:affinity-yelum" not in brands


def test_service_and_broker_context_are_not_promoted_to_risk_carriers() -> None:
    brands = _brand_index(_registry())

    assist_card = brands["brand:assist-card"]
    service = assist_card["market_identity"]["service_context"]
    assert service["international_assistance_provider"] == "ASSIST CARD SMALLINE S.A."
    assert service["national_assistance_provider_cnpj"] == "31772960000101"
    assist_card_targets = {
        row.get("target_cnpj") for row in assist_card.get("relationships") or []
    }
    assert "31772960000101" not in assist_card_targets

    vital = brands["brand:vital-card"]
    broker = vital["market_identity"]["broker_context"]
    assert broker["cnpj"] == "12353171000183"
    vital_targets = {row.get("target_cnpj") for row in vital.get("relationships") or []}
    assert "12353171000183" not in vital_targets
    assert "67865360000127" in vital_targets
