from api.v2.build_market_relationship_conduct_evidence import (
    build_legacy_sandbox_projection,
    build_market_relationship_conduct_evidence,
)
from api.v2.consumer_gov_identity import load_provider_resolution_registry

MONTHS = [f"2025-{month:02d}" for month in range(7, 13)] + [
    f"2026-{month:02d}" for month in range(1, 7)
]


def _entity(entity_id, cnpj, name, *, regime="sandbox", entity_type="sandbox_participant"):
    return {
        "entity_id": entity_id,
        "entity_type": entity_type,
        "cnpj": cnpj,
        "legal_name": name,
        "display_name": name,
        "regulatory_regime": regime,
        "regulatory_status": "temporary_authorized",
        "eligibility": {"regulatory_universe_eligible": False, "reason_codes": []},
        "query_context": {},
    }


def _lifecycle() -> dict:
    return {
        "entities": [
            _entity("sandbox:lti", "47006254000180", "LTI SEGUROS S.A."),
        ],
        "brands": [
            {
                "brand_id": "brand:loovi",
                "name": "Loovi",
                "aliases": ["Loovi Seguros", "Loovi Technology"],
                "market_identity": {
                    "identity_state": "verified",
                    "kind": "consumer_facing_company",
                    "public_label": "Representante de seguros",
                    "legal_name": "CW TECHNOLOGY LTDA.",
                    "trade_name": "LOOVI TECHNOLOGY",
                    "cnpj": "34504257000100",
                    "distribution_role": "insurance_representative",
                    "distribution_role_state": "verified",
                    "evidence": {"authority": "Loovi", "fact": "verified role"},
                },
                "relationships": [
                    {
                        "relationship_type": "risk_carrier",
                        "target_entity_id": "sandbox:lti",
                        "status": "current",
                        "scope": "seguro automotivo",
                        "evidence": {"authority": "Loovi", "fact": "verified"},
                    }
                ],
            }
        ],
    }


def _identity() -> dict:
    return {"source": {"months": MONTHS}}


def _entry(complaints: int, *, display_name: str, cnpj: str | None = None) -> dict:
    payload = {
        "display_name": display_name,
        "statistics": {
            "complaintsCount": complaints,
            "respondedCount": complaints,
            "finalizedCount": complaints,
            "resolvedCount": 0,
            "evaluatedCount": complaints,
            "scoreSum": float(complaints * 3),
        },
    }
    if cnpj:
        payload["cnpj"] = cnpj
    return payload


def _build(first_month_entries: dict) -> tuple[dict, dict]:
    by_month = {month: {} for month in MONTHS}
    by_month[MONTHS[0]] = first_month_entries
    generic = build_market_relationship_conduct_evidence(
        _lifecycle(),
        _identity(),
        entry_loader=lambda month: by_month[month],
    )
    return generic, build_legacy_sandbox_projection(generic)


def test_loovi_compatibility_projection_preserves_separate_subjects() -> None:
    generic, legacy = _build(
        {
            "loovi": _entry(
                7, display_name="CW Technology Ltda", cnpj="34504257000100"
            ),
            "lti": _entry(100, display_name="LTI Seguros", cnpj="47006254000180"),
        }
    )

    row = generic["brands"][0]
    assert row["market_identity_conduct_summary"]["complaints"] == 7
    assert row["risk_bearers"][0]["conduct_summary"]["complaints"] == 100
    assert row["risk_bearers"][0]["regulatory_class"] == (
        "Participante do Sandbox regulatório"
    )

    assert legacy["status"] == "compatibility_projection"
    brand = legacy["brands"][0]
    assert brand["brand_subject_conduct_summary"]["complaints"] == 7
    assert brand["carrier_conduct_summary"]["complaints"] == 100
    assert brand["ordinary_ranking_effect"] == "none"


def test_source_cnpj_blocks_loovi_label_override() -> None:
    generic, _ = _build(
        {
            "loovi": _entry(
                8, display_name="Loovi", cnpj="47006254000180"
            )
        }
    )
    brand = generic["brands"][0]
    assert brand["market_identity_conduct_summary"]["complaints"] == 0
    assert brand["risk_bearers"][0]["conduct_summary"]["complaints"] == 8


def test_generic_noncarrier_brand_guards_are_source_backed() -> None:
    registry = load_provider_resolution_registry()
    for provider in ("Sicoob", "Crefisa", "Loovi"):
        row = registry[provider.casefold()]
        assert row.resolution_state == "outside_157"
        assert row.target_cnpj is None
        assert row.evidence
