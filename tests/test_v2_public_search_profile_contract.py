from api.v2.build_public_search_profile_contract import (
    build_public_search_profile_contract,
)


def _base_entity(
    entity_id,
    *,
    cnpj,
    legal_name,
    entity_type="insurer",
    regime="ordinary",
    status="active_licensed",
    group=None,
):
    query_state = (
        "sandbox_experimental_participant"
        if regime == "sandbox"
        else "current_ordinary_insurer"
    )
    bucket = "sandbox" if regime == "sandbox" else "insurers"
    return {
        "entity_id": entity_id,
        "fip_code": entity_id.split(":")[-1] if entity_id.startswith("fip:") else None,
        "cnpj": cnpj,
        "legal_entity_id": f"cnpj:{cnpj}",
        "legal_name": legal_name,
        "display_name": None,
        "entity_type": entity_type,
        "regulatory_regime": regime,
        "regulatory_status": status,
        "economic_group": group,
        "relationships": (
            [
                {
                    "relationship_type": "member_of_group",
                    "target_group_id": group["group_id"],
                }
            ]
            if group
            else []
        ),
        "query_context": {
            "entity_state": query_state,
            "filter_bucket": bucket,
        },
    }


def _explorer_row(
    entity_id,
    *,
    legal_name,
    premium=100.0,
    complaints=1,
    expected=1.0,
    ratio=1.0,
    comparable_months=12,
    comparability="direct_one_to_one_candidate",
    conduct_state="not_distinguishable_from_expected",
    reason_code=None,
):
    return {
        "entity_id": entity_id,
        "fip_code": entity_id.split(":")[-1],
        "legal_name": legal_name,
        "display_name": None,
        "assessment": {
            "eligible": expected is not None,
            "state": "eligible" if expected is not None else "not_eligible",
            "completeness": "complete" if expected is not None else "incomplete",
            "matrix_state": (
                "no_current_core_adverse_signal"
                if expected is not None
                else "evidence_incomplete_for_joint_assessment"
            ),
            "public_class": "favorable_reading" if expected is not None else None,
            "title": "Leitura",
            "summary": "Resumo metodológico.",
            "why_it_matters": None,
            "mandatory_limit": None,
        },
        "financial": {
            "reference_period": "202605",
            "core_signal": "core_indicators_without_current_shortfall",
            "capital": {
                "state": "capital_meets_or_exceeds_cmr",
                "pla_cmr_ratio": 1.5,
            },
            "liquidity": {
                "state": "ilt_at_or_above_arithmetic_parity",
                "value": 1.2,
            },
            "operating_context": {"signal": "balanced_persistent"},
            "evidence_confidence": "established_core_history",
        },
        "conduct": {
            "state": conduct_state,
            "summary": "Resumo de Conduta.",
            "comparability_state": comparability,
            "reason_code": reason_code,
            "observed_complaints_12m": complaints,
            "expected_complaints_12m": expected,
            "pressure_ratio": ratio,
            "comparable_months": comparable_months,
            "persistence": None,
            "trend": None,
        },
        "market_context": {
            "insurance_premium_direct_12m": premium,
            "complaints_12m": complaints,
        },
        "explore_memberships": {"leaderboards": [], "collections": []},
    }


def _empty_sandbox():
    return {
        "artifact": "v2_sandbox_brand_conduct_evidence",
        "carriers": [],
        "brands": [],
    }


def _lifecycle(entities, *, brands=None, groups=None):
    return {
        "artifact": "v2_lifecycle_relationship_inventory",
        "entities": entities,
        "brands": brands or [],
        "groups": groups or [],
        "corporate_relationships": [],
    }


def _explorer(entities):
    return {
        "artifact": "v2_public_insurer_explorer",
        "entities": entities,
    }


def _profiles(payload):
    return {profile["profile_id"]: profile for profile in payload["profiles"]}


def test_missing_values_remain_null_and_youse_zero_is_not_operation_size():
    group = {
        "group_id": "susep-group:00230",
        "group_code": "00230",
        "group_name": "CAIXA ECONÔMICA S/A",
        "observed_period": "202606",
        "source": "SUSEP SES",
        "member_entity_ids": ["fip:001121", "fip:005631"],
    }
    group_ref = {
        key: group[key]
        for key in ("group_id", "group_code", "group_name", "observed_period", "source")
    }
    youse = _base_entity(
        "fip:001121",
        cnpj="24856160000103",
        legal_name="YOUSE SEGURADORA S.A.",
        group=group_ref,
    )
    caixa = _base_entity(
        "fip:005631",
        cnpj="34020354000110",
        legal_name="CAIXA SEGURADORA S.A.",
        group=group_ref,
    )
    youse_explorer = _explorer_row(
        "fip:001121",
        legal_name="YOUSE SEGURADORA S.A.",
        premium=0.0,
        complaints=1367,
        expected=None,
        ratio=None,
        comparable_months=None,
        comparability="consumer_subject_single_carrier_exposure_not_brand_specific",
        conduct_state="pressure_unavailable_not_comparable",
        reason_code="brand_specific_exposure_required",
    )
    caixa_explorer = _explorer_row(
        "fip:005631",
        legal_name="CAIXA SEGURADORA S.A.",
    )
    relationships = {
        "relationships": [
            {
                "relationship_id": "conduct:youse-to-caixa-seguradora",
                "relationship_type": "consumer_subject_single_risk_carrier",
                "subject_cnpj": "24856160000103",
                "target_cnpjs": ["34020354000110"],
                "pressure_policy": "brand_specific_exposure_required",
                "reconciliation_state": (
                    "consumer_subject_single_carrier_exposure_not_brand_specific"
                ),
                "evidence": [{"authority": "Youse"}],
            }
        ]
    }

    payload = build_public_search_profile_contract(
        _lifecycle([youse, caixa], groups=[group]),
        _explorer([youse_explorer, caixa_explorer]),
        _empty_sandbox(),
        relationships,
    )
    profile = _profiles(payload)["entity:fip:001121"]
    technical = profile["assessment"]["conduct"]["technical"]

    assert technical["observed_complaints_12m"]["value"] == 1367
    assert technical["expected_complaints_12m"]["value"] is None
    assert technical["expected_complaints_12m"]["availability"] == "unavailable"
    assert technical["observed_expected_ratio"]["value"] is None
    assert technical["comparable_months"]["value"] is None

    operation = profile["assessment"]["operation_context"][
        "insurance_premium_direct_12m"
    ]
    assert operation["value"] == 0.0
    assert operation["public_use"] == "do_not_render_as_operation_size"
    assert "CAIXA SEGURADORA" in profile["assessment"]["conduct"]["plain_language"]


def test_loovi_brand_resolves_to_lti_without_inheriting_assessment():
    lti = _base_entity(
        "cnpj:47006254000180",
        cnpj="47006254000180",
        legal_name="LTI Seguros S.A.",
        entity_type="sandbox_participant",
        regime="sandbox",
        status="temporary_authorized",
    )
    brand = {
        "brand_id": "brand:loovi",
        "name": "Loovi",
        "aliases": ["Loovi Seguros", "Loovi Technology"],
        "relationships": [
            {
                "relationship_type": "risk_carrier",
                "target_entity_id": "cnpj:47006254000180",
                "status": "current",
                "scope": "seguro automotivo",
                "evidence": {"authority": "Loovi"},
            }
        ],
    }
    totals = {
        "complaints": 1329,
        "responded": 1286,
        "response_rate": 0.9676,
        "finalized": 1329,
        "finalized_rate": 1.0,
        "satisfaction_count": 619,
        "average_satisfaction": 2.6446,
    }
    sandbox = {
        "artifact": "v2_sandbox_brand_conduct_evidence",
        "carriers": [
            {
                "entity_id": "cnpj:47006254000180",
                "cnpj": "47006254000180",
                "legal_name": "LTI Seguros S.A.",
                "totals": totals,
                "consumer_gov_provider_labels_observed": [
                    {"provider": "LTI Seguros", "complaints": 1329}
                ],
                "film": {"months_observed": 12},
            }
        ],
        "brands": [
            {
                "brand_id": "brand:loovi",
                "risk_carrier_entity_id": "cnpj:47006254000180",
                "risk_carrier_cnpj": "47006254000180",
                "risk_carrier_name": "LTI Seguros S.A.",
                "product_scope": "seguro automotivo",
                "conduct_scope": "carrier_level_context_for_verified_brand_relationship",
                "attribution_note": "Carrier-level context, not brand-exclusive.",
                "carrier_conduct_summary": {**totals, "film": {"months_observed": 12}},
                "evidence": [{"authority": "Loovi"}],
            }
        ],
    }

    payload = build_public_search_profile_contract(
        _lifecycle([lti], brands=[brand]),
        _explorer([]),
        sandbox,
        {"relationships": []},
    )
    profiles = _profiles(payload)
    loovi = profiles["brand:loovi"]
    lti_profile = profiles["entity:cnpj:47006254000180"]

    assert loovi["assessment"]["availability"] == "not_applicable"
    assert loovi["relationships"][0]["target_profile_id"] == (
        "entity:cnpj:47006254000180"
    )
    assert loovi["sandbox_conduct_context"]["metrics"]["complaints"]["value"] == 1329
    assert "LTI Seguros" in loovi["sandbox_conduct_context"]["plain_language"]
    assert lti_profile["sandbox_conduct"]["metrics"]["complaints"]["value"] == 1329
    assert lti_profile["assessment"]["availability"] == "not_applicable"


def test_same_susep_group_keeps_distinct_hdi_entities():
    group = {
        "group_id": "susep-group:01230",
        "group_code": "01230",
        "group_name": "TALANX AG",
        "observed_period": "202606",
        "source": "SUSEP SES",
        "member_entity_ids": ["fip:006572", "fip:001571"],
    }
    group_ref = {
        key: group[key]
        for key in ("group_id", "group_code", "group_name", "observed_period", "source")
    }
    hdi = _base_entity(
        "fip:006572",
        cnpj="29980158000157",
        legal_name="HDI SEGUROS S.A.",
        group=group_ref,
    )
    hdi_global = _base_entity(
        "fip:001571",
        cnpj="18096627000153",
        legal_name="HDI GLOBAL SEGUROS S.A",
        group=group_ref,
    )

    payload = build_public_search_profile_contract(
        _lifecycle([hdi, hdi_global], groups=[group]),
        _explorer(
            [
                _explorer_row("fip:006572", legal_name="HDI SEGUROS S.A."),
                _explorer_row("fip:001571", legal_name="HDI GLOBAL SEGUROS S.A"),
            ]
        ),
        _empty_sandbox(),
        {"relationships": []},
    )
    profiles = _profiles(payload)
    hdi_context = profiles["entity:fip:006572"]["relationship_context"]["economic_group"]

    assert hdi_context["group_name"] == "TALANX AG"
    assert hdi_context["related_entities"] == [
        {
            "profile_id": "entity:fip:001571",
            "entity_id": "fip:001571",
            "name": "HDI GLOBAL SEGUROS S.A",
            "entity_type": "insurer",
            "regulatory_regime": "ordinary",
        }
    ]
    assert profiles["entity:fip:006572"]["identity"]["cnpj"] != (
        profiles["entity:fip:001571"]["identity"]["cnpj"]
    )


def test_search_index_contains_entities_and_brands_without_fuzzy_identity_decision():
    insurer = _base_entity(
        "fip:000001",
        cnpj="11111111000111",
        legal_name="SEGURADORA TESTE S.A.",
    )
    brand = {
        "brand_id": "brand:teste",
        "name": "Marca Teste",
        "aliases": ["Teste Seguros"],
        "relationships": [
            {
                "relationship_type": "brand_of",
                "target_entity_id": "fip:000001",
                "status": "current",
                "evidence": {"authority": "Teste"},
            }
        ],
    }

    payload = build_public_search_profile_contract(
        _lifecycle([insurer], brands=[brand]),
        _explorer(
            [_explorer_row("fip:000001", legal_name="SEGURADORA TESTE S.A.")]
        ),
        _empty_sandbox(),
        {"relationships": []},
    )

    assert payload["population"]["profiles"] == 2
    assert payload["population"]["search_entries"] == 2
    assert {row["profile_id"] for row in payload["search_index"]} == {
        "entity:fip:000001",
        "brand:teste",
    }
    assert payload["publication_policy"][
        "frontend_may_use_fuzzy_search_to_decide_identity"
    ] is False
    brand_entry = next(
        row for row in payload["search_index"] if row["profile_id"] == "brand:teste"
    )
    assert "teste seguros" in brand_entry["search_text"]
