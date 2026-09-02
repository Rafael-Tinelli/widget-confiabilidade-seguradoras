from api.v2.public_information_projection import _project_verified_market_identities


def test_verified_broker_market_identity_is_searchable_without_carrier_or_complaint_inference():
    contract = {
        "profiles": [
            {
                "profile_id": "brand:broker-teste",
                "profile_kind": "brand",
                "identity": {
                    "brand_id": "brand:broker-teste",
                    "name": "Broker Teste",
                    "aliases": ["Broker Teste Seguros"],
                    "entity_type": "brand",
                },
                "relationships": [],
                "assessment": {
                    "availability": "not_applicable",
                    "reason": "brand_never_inherits_entity_assessment",
                },
                "public_summary": {},
            }
        ],
        "search_index": [
            {
                "profile_id": "brand:broker-teste",
                "name": "Broker Teste",
                "aliases": ["Broker Teste Seguros"],
                "search_text": "broker teste broker teste seguros",
                "entity_type": "brand",
            }
        ],
    }
    registry = {
        "brands": [
            {
                "brand_id": "brand:broker-teste",
                "market_identity": {
                    "kind": "broker",
                    "public_label": "Corretora / intermediária",
                    "legal_name": "BROKER TESTE CORRETORA DE SEGUROS LTDA",
                    "cnpj": "12345678000199",
                    "public_note": (
                        "A empresa atua como corretora/intermediária e não é presumida "
                        "como a seguradora que assume o risco."
                    ),
                    "evidence": {
                        "authority": "SUSEP broker registry",
                        "source": "fixture",
                    },
                },
            }
        ]
    }

    assert _project_verified_market_identities(contract, registry) == 1
    profile = contract["profiles"][0]
    search = contract["search_index"][0]

    assert profile["identity"]["market_identity"]["kind"] == "broker"
    assert profile["identity"]["market_identity"]["assessment_inheritance"] == "forbidden"
    assert profile["assessment"]["availability"] == "not_applicable"
    assert profile["relationships"] == []
    assert search["entity_type"] == "broker"
    assert search["cnpj"] == "12345678000199"
    assert "broker teste corretora de seguros ltda" in search["search_text"]
    assert "complaint" not in profile
    assert "risk_carrier" not in profile
