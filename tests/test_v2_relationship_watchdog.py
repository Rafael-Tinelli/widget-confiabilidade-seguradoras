import pytest

from api.v2.relationship_watchdog import (
    RelationshipWatchdogError,
    build_relationship_watchdog,
    validate_relationship_watchdog,
)


def _entity(entity_id, cnpj, name, state="current_ordinary_insurer"):
    return {
        "entity_id": entity_id,
        "cnpj": cnpj,
        "legal_name": name,
        "entity_type": "insurer",
        "regulatory_regime": "ordinary",
        "regulatory_status": "active_licensed",
        "query_context": {
            "entity_state": state,
            "filter_bucket": (
                "insurers" if state == "current_ordinary_insurer" else "historical"
            ),
        },
        "relationships": [],
    }


def _lifecycle(entities, *, brands=None, groups=None):
    return {
        "artifact": "v2_lifecycle_relationship_inventory",
        "entities": entities,
        "brands": brands or [],
        "groups": groups or [],
        "corporate_relationships": [],
    }


def test_closed_entity_without_successor_is_discovered_but_never_asserted():
    old = _entity(
        "fip:old",
        "11111111000111",
        "OLD INSURER",
        state="historical_closed_entity",
    )
    old["legal_lifecycle"] = {
        "cadastral_status": "closed",
        "status_reason": "incorporation",
    }

    payload = build_relationship_watchdog(_lifecycle([old]))

    assert payload["status"] == "review_candidates"
    assert payload["summary"]["blocking_registry_drift_count"] == 0
    candidate = payload["candidates"][0]
    assert candidate["candidate_type"] == "closed_entity_without_verified_successor"
    assert candidate["blocking"] is False
    assert candidate["assertion_effect"] == "none"
    validate_relationship_watchdog(payload)


def test_unresolved_consumer_provider_becomes_stable_review_candidate():
    lifecycle = _lifecycle(
        [_entity("fip:carrier", "22222222000122", "KNOWN CARRIER")]
    )
    identity = {
        "artifact": "v2_consumer_gov_identity_experiment",
        "unresolved_providers": [
            {
                "provider": "Nova Marca Seguro",
                "complaints": 17,
                "candidate_suggestions_non_authoritative": [
                    {"entity_id": "fip:carrier", "score": 0.83}
                ],
            }
        ],
        "ambiguous_providers": [],
        "temporal_brand_unresolved_providers": [],
        "temporal_brand_ambiguous_providers": [],
    }

    first = build_relationship_watchdog(lifecycle, consumer_identity=identity)
    second = build_relationship_watchdog(lifecycle, consumer_identity=identity)

    candidate = first["candidates"][0]
    assert candidate["candidate_type"] == "consumer_provider_unresolved"
    assert candidate["blocking"] is False
    assert candidate["signals"]["complaints"] == 17
    assert first["candidates"][0]["candidate_id"] == second["candidates"][0]["candidate_id"]
    assert first["policy"]["automatic_registry_mutation"] == "forbidden"


def test_verified_sandbox_carrier_drift_fails_closed():
    lti = _entity(
        "sandbox:lti",
        "47006254000180",
        "LTI SEGUROS S.A.",
        state="sandbox_experimental_participant",
    )
    brand = {
        "brand_id": "brand:loovi",
        "name": "Loovi",
        "aliases": ["Loovi Seguros"],
        "relationships": [
            {
                "relationship_type": "risk_carrier",
                "target_entity_id": "sandbox:lti",
                "target_cnpj": "47006254000180",
                "status": "current",
                "evidence": {"authority": "Loovi"},
            }
        ],
    }
    sandbox = {
        "brands": [
            {
                "brand_id": "brand:loovi",
                "name": "Loovi",
                "representative_cnpj": "34504257000100",
                "risk_carrier_cnpj": "99999999000199",
                "risk_carrier_name": "WRONG CARRIER",
                "regulatory_scope": "sandbox",
            }
        ]
    }

    payload = build_relationship_watchdog(
        _lifecycle([lti], brands=[brand]),
        sandbox_registry=sandbox,
    )

    assert payload["status"] == "blocking_registry_drift"
    assert any(
        item["candidate_type"] == "sandbox_brand_carrier_registry_drift"
        for item in payload["candidates"]
    )
    with pytest.raises(RelationshipWatchdogError, match="registry drift"):
        validate_relationship_watchdog(payload)


def test_current_verified_brand_cannot_point_to_historical_target():
    historical = _entity(
        "fip:old-carrier",
        "33333333000133",
        "OLD CARRIER",
        state="historical_closed_entity",
    )
    brand = {
        "brand_id": "brand:test",
        "name": "Test Brand",
        "aliases": [],
        "relationships": [
            {
                "relationship_type": "risk_carrier",
                "target_entity_id": "fip:old-carrier",
                "target_cnpj": "33333333000133",
                "status": "current",
                "evidence": {"authority": "Official source"},
            }
        ],
    }

    payload = build_relationship_watchdog(_lifecycle([historical], brands=[brand]))

    assert any(
        item["candidate_type"] == "verified_brand_current_target_is_historical"
        and item["blocking"]
        for item in payload["candidates"]
    )


def test_alias_collision_between_different_verified_targets_fails_closed():
    first = _entity("fip:a", "44444444000144", "CARRIER A")
    second = _entity("fip:b", "55555555000155", "CARRIER B")
    brands = [
        {
            "brand_id": "brand:a",
            "name": "Alpha",
            "aliases": ["Shared Insurance"],
            "relationships": [
                {
                    "relationship_type": "brand_of",
                    "target_entity_id": "fip:a",
                    "target_cnpj": "44444444000144",
                    "status": "current",
                }
            ],
        },
        {
            "brand_id": "brand:b",
            "name": "Beta",
            "aliases": ["Shared Insurance"],
            "relationships": [
                {
                    "relationship_type": "brand_of",
                    "target_entity_id": "fip:b",
                    "target_cnpj": "55555555000155",
                    "status": "current",
                }
            ],
        },
    ]

    payload = build_relationship_watchdog(_lifecycle([first, second], brands=brands))

    assert any(
        item["candidate_type"] == "verified_brand_alias_collision"
        and item["blocking"]
        for item in payload["candidates"]
    )


def test_known_combined_company_patterns_are_normalized_without_inference():
    caixa = _entity("fip:caixa", "34020354000110", "CAIXA SEGURADORA S.A.")
    lti = _entity(
        "sandbox:lti",
        "47006254000180",
        "LTI SEGUROS S.A.",
        state="sandbox_experimental_participant",
    )
    mapfre = _entity("fip:mapfre", "61074175000138", "MAPFRE SEGUROS GERAIS S.A.")
    hdi = _entity("fip:hdi", "29143039000128", "HDI SEGUROS S.A.")
    hdi_global = _entity(
        "fip:hdi-global",
        "18026075000152",
        "HDI GLOBAL SEGUROS S.A.",
    )
    brands = [
        {
            "brand_id": "brand:loovi",
            "name": "Loovi",
            "aliases": ["Loovi Seguros", "Loovi Technology"],
            "relationships": [
                {
                    "relationship_type": "risk_carrier",
                    "target_entity_id": "sandbox:lti",
                    "target_cnpj": "47006254000180",
                    "status": "current",
                    "evidence": {"authority": "Loovi"},
                }
            ],
        },
        {
            "brand_id": "brand:bb-seguro-auto",
            "name": "BB Seguro Auto",
            "aliases": [],
            "relationships": [
                {
                    "relationship_type": "risk_carrier",
                    "target_entity_id": "fip:mapfre",
                    "target_cnpj": "61074175000138",
                    "status": "current",
                    "evidence": {"authority": "BB Seguros"},
                }
            ],
        },
    ]
    groups = [
        {
            "group_id": "susep-group:talanx",
            "group_code": "999",
            "group_name": "TALANX",
            "observed_period": "202606",
            "member_entity_ids": ["fip:hdi", "fip:hdi-global"],
        }
    ]
    conduct = {
        "relationships": [
            {
                "relationship_id": "conduct:youse-to-caixa-seguradora",
                "relationship_type": "consumer_subject_single_risk_carrier",
                "subject_cnpj": "24856160000103",
                "target_cnpjs": ["34020354000110"],
                "effective_from": "2022-01-01",
                "pressure_policy": "brand_specific_exposure_required",
                "reconciliation_state": "usable_with_guardrail",
            }
        ]
    }
    sandbox = {
        "brands": [
            {
                "brand_id": "brand:loovi",
                "name": "Loovi",
                "aliases": ["Loovi Seguros"],
                "representative_cnpj": "34504257000100",
                "risk_carrier_cnpj": "47006254000180",
                "risk_carrier_name": "LTI Seguros S.A.",
                "regulatory_scope": "sandbox",
                "conduct_context_policy": (
                    "carrier_level_context_not_brand_exclusive_attribution"
                ),
            }
        ]
    }

    payload = build_relationship_watchdog(
        _lifecycle(
            [caixa, lti, mapfre, hdi, hdi_global],
            brands=brands,
            groups=groups,
        ),
        conduct_registry=conduct,
        sandbox_registry=sandbox,
    )

    assert payload["summary"]["blocking_registry_drift_count"] == 0
    observations = payload["observations"]

    talanx = next(
        item
        for item in observations
        if item["observation_type"] == "official_economic_group_membership"
    )
    assert {
        member["entity_id"] for member in talanx["relationship"]["members"]
    } == {"fip:hdi", "fip:hdi-global"}

    youse = next(
        item
        for item in observations
        if item["observation_type"] == "verified_conduct_subject_relationship"
    )
    assert youse["relationship"]["target_cnpjs"] == ["34020354000110"]

    brand_observations = [
        item
        for item in observations
        if item["observation_type"] == "verified_brand_relationship"
    ]
    assert any(
        item["subject"]["brand_id"] == "brand:loovi"
        and item["relationship"]["target_cnpj"] == "47006254000180"
        for item in brand_observations
    )
    assert any(
        item["subject"]["brand_id"] == "brand:bb-seguro-auto"
        and item["relationship"]["target_cnpj"] == "61074175000138"
        for item in brand_observations
    )
