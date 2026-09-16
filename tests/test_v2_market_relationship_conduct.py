from api.v2.build_market_relationship_conduct_evidence import (
    build_legacy_sandbox_projection,
    build_market_relationship_conduct_evidence,
)

MONTHS = [f"2026-{month:02d}" for month in range(1, 13)]


def _entity(entity_id, cnpj, name, *, regime="ordinary", entity_type="insurer"):
    return {
        "entity_id": entity_id,
        "cnpj": cnpj,
        "legal_name": name,
        "display_name": name,
        "entity_type": entity_type,
        "regulatory_regime": regime,
        "regulatory_status": (
            "temporary_authorized" if regime == "sandbox" else "active_licensed"
        ),
        "eligibility": {
            "regulatory_universe_eligible": regime == "ordinary"
            and entity_type == "insurer",
            "reason_codes": [],
        },
        "query_context": {},
    }


def _entry(provider, cnpj, complaints):
    return {
        "display_name": provider,
        "name": provider,
        "cnpj": cnpj,
        "statistics": {
            "complaintsCount": complaints,
            "respondedCount": complaints,
            "finalizedCount": complaints,
            "resolvedCount": max(complaints - 1, 0),
            "evaluatedCount": complaints,
            "scoreSum": complaints * 4.0,
            "responseTimeCount": complaints,
            "responseTimeSum": complaints * 2.0,
        },
    }


def _lifecycle():
    ordinary = _entity(
        "ordinary-a",
        "11111111000111",
        "SEGURADORA ORDINARIA A S.A.",
    )
    sandbox = _entity(
        "sandbox-b",
        "22222222000122",
        "SEGURADORA SANDBOX B S.A.",
        regime="sandbox",
        entity_type="sandbox_participant",
    )
    market_identity = {
        "kind": "insurance_distribution_company",
        "public_label": "Representante de seguros",
        "legal_name": "DISTRIBUIDORA X LTDA.",
        "trade_name": "MARCA X",
        "cnpj": "33333333000133",
        "distribution_role": "insurance_representative",
        "distribution_role_state": "verified",
        "evidence": {"authority": "Documento oficial", "fact": "papel verificado"},
    }
    brand_x = {
        "brand_id": "brand:marca-x",
        "name": "Marca X",
        "aliases": ["Marca X Seguros"],
        "market_identity": market_identity,
        "relationships": [
            {
                "relationship_type": "risk_carrier",
                "target_entity_id": "ordinary-a",
                "status": "current",
                "scope": "produto A",
                "evidence": {"authority": "Fonte A", "fact": "carrier A"},
            },
            {
                "relationship_type": "risk_carrier",
                "target_entity_id": "sandbox-b",
                "status": "current",
                "scope": "produto B",
                "evidence": {"authority": "Fonte B", "fact": "carrier B"},
            },
        ],
    }
    brand_y = {
        "brand_id": "brand:marca-y",
        "name": "Marca Y",
        "aliases": [],
        "market_identity": market_identity,
        "relationships": [
            {
                "relationship_type": "risk_carrier",
                "target_entity_id": "ordinary-a",
                "status": "current",
                "scope": "produto Y",
                "evidence": {"authority": "Fonte Y", "fact": "carrier A"},
            }
        ],
    }
    return {"entities": [ordinary, sandbox], "brands": [brand_x, brand_y]}


def _identity():
    return {"source": {"months": MONTHS}}


def test_market_relationship_conduct_is_regime_agnostic_and_multi_carrier():
    entries = {month: {} for month in MONTHS}
    entries[MONTHS[0]] = {
        "distributor": _entry("Marca X", "33333333000133", 3),
        "ordinary": _entry("SEGURADORA ORDINARIA A S.A.", "11111111000111", 5),
        "sandbox": _entry("SEGURADORA SANDBOX B S.A.", "22222222000122", 7),
        # Exact brand label cannot override an unrelated source CNPJ.
        "mismatch": _entry("Marca X", "44444444000144", 11),
    }

    payload = build_market_relationship_conduct_evidence(
        _lifecycle(),
        _identity(),
        entry_loader=lambda month: entries[month],
    )

    assert payload["artifact"] == "v2_market_relationship_conduct_evidence"
    assert payload["policy"]["regulatory_regime_is_attribute_not_relationship_model"]
    assert payload["policy"]["multi_carrier_relationships_must_remain_separate"]
    assert payload["summary"]["verified_market_identities"] == 1
    assert payload["summary"]["brands_with_market_relationship_context"] == 2
    assert payload["summary"]["direct_market_subject_complaints"] == 3

    brand = next(row for row in payload["brands"] if row["brand_id"] == "brand:marca-x")
    assert brand["market_identity_conduct_summary"]["complaints"] == 3
    assert len(brand["risk_bearers"]) == 2

    bearers = {row["entity_id"]: row for row in brand["risk_bearers"]}
    assert bearers["ordinary-a"]["regulatory_class"] == "Seguradora"
    assert bearers["ordinary-a"]["conduct_summary"]["complaints"] == 5
    assert bearers["sandbox-b"]["regulatory_class"] == (
        "Participante do Sandbox regulatório"
    )
    assert bearers["sandbox-b"]["conduct_summary"]["complaints"] == 7

    # Two brands may legitimately share one legal distributor. Its complaints are
    # legal-subject conduct, not duplicated into separate brand-level populations.
    brand_y = next(row for row in payload["brands"] if row["brand_id"] == "brand:marca-y")
    assert brand_y["market_identity_key"] == brand["market_identity_key"]
    assert brand_y["market_identity_conduct_summary"]["complaints"] == 3


def test_legacy_projection_is_only_a_compatibility_view_of_generic_model():
    entries = {month: {} for month in MONTHS}
    entries[MONTHS[0]] = {
        "distributor": _entry("Marca X", "33333333000133", 2),
        "sandbox": _entry("SEGURADORA SANDBOX B S.A.", "22222222000122", 4),
    }
    payload = build_market_relationship_conduct_evidence(
        _lifecycle(),
        _identity(),
        entry_loader=lambda month: entries[month],
    )
    legacy = build_legacy_sandbox_projection(payload)

    assert legacy["artifact"] == "v2_sandbox_brand_conduct_evidence"
    assert legacy["status"] == "compatibility_projection"
    assert legacy["policy"]["authoritative_relationship_model"] == (
        "v2_market_relationship_conduct_evidence"
    )
    # Marca X is multi-carrier, so a singular Sandbox compatibility card would be
    # misleading and must not be synthesized.
    assert legacy["brands"] == []
    assert len(legacy["carriers"]) == 1
    assert legacy["carriers"][0]["entity_id"] == "sandbox-b"
