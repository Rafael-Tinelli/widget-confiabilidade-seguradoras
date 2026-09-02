from api.v2.public_information_projection import apply_public_information_projection

MONTHS = [f"2025-{month:02d}" for month in range(7, 13)] + [
    f"2026-{month:02d}" for month in range(1, 7)
]


def test_verified_current_product_carrier_context_reaches_public_profile():
    contract = {
        "artifact": "v2_public_search_profile_contract",
        "profiles": [
            {
                "profile_id": "entity:fip:001121",
                "profile_kind": "entity",
                "relationship_context": {
                    "conduct_reconciliation": [
                        {
                            "relationship_id": "conduct:youse-to-caixa-seguradora",
                            "relationship_type": "consumer_subject_single_risk_carrier",
                            "role": "subject",
                            "pressure_policy": "brand_specific_exposure_required",
                        }
                    ]
                },
                "assessment": {
                    "availability": "incomplete",
                    "conduct": {
                        "state": "pressure_unavailable_not_comparable",
                        "reason_code": "brand_specific_exposure_required",
                        "plain_language": "old generic relationship wording",
                    },
                },
                "sandbox_conduct": None,
            }
        ],
        "search_index": [],
    }
    explorer = {
        "artifact": "v2_public_insurer_explorer",
        "entities": [
            {"entity_id": "fip:001121", "conduct": {"state": "pressure_unavailable_not_comparable"}}
        ],
    }
    conduct = {
        "artifact": "v2_conduct_methodology_closure",
        "candidate_entities": [
            {
                "entity_id": "fip:001121",
                "direct_pressure": {
                    "monthly": [{"month": month} for month in MONTHS]
                },
            }
        ],
    }
    sandbox = {
        "artifact": "v2_sandbox_brand_conduct_evidence",
        "source": {"months": MONTHS},
    }
    public_context = (
        "Youse Seguradora S.A. é preservada como identidade regulatória própria, "
        "enquanto produtos atuais documentam a Caixa Seguradora como garantidora. "
        "A relação não autoriza transferir reclamações nem exposição."
    )
    conduct_registry = {
        "relationships": [
            {
                "relationship_id": "conduct:youse-to-caixa-seguradora",
                "pressure_policy": "brand_specific_exposure_required",
                "effective_to": None,
                "verification_state": "current_product_carrier_context_verified",
                "verified_as_of": "2026-09-02",
                "scope": "Produtos Youse atuais com Caixa como garantidora.",
                "public_context": public_context,
                "evidence": [{"authority": "Youse"}],
            }
        ]
    }

    projected, _ = apply_public_information_projection(
        contract,
        explorer,
        conduct,
        sandbox,
        conduct_relationship_registry=conduct_registry,
    )

    profile = projected["profiles"][0]
    relation = profile["relationship_context"]["conduct_reconciliation"][0]
    assert relation["verification_state"] == "current_product_carrier_context_verified"
    assert relation["verified_as_of"] == "2026-09-02"
    assert relation["scope"] == "Produtos Youse atuais com Caixa como garantidora."
    assert relation["public_context"] == public_context
    assert profile["assessment"]["conduct"]["plain_language"] == public_context
    assert profile["assessment"]["conduct"]["relationship_context"] == public_context

    projection = projected["public_information_projection"]
    assert projection["policy"]["frontend_may_infer_conduct_relationship_context"] is False
    assert projection["policy"]["conduct_relationship_context_changes_complaint_attribution"] is False
    assert projection["counts"]["verified_conduct_relationship_contexts"] == 1
