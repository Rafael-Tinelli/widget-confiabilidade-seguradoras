from __future__ import annotations

from pathlib import Path

from api.v2.build_sandbox_brand_conduct_evidence import (
    build_sandbox_brand_conduct_evidence,
)
from api.v2.consumer_gov_identity import load_provider_resolution_registry

MONTHS = [f"2025-{month:02d}" for month in range(7, 13)] + [
    f"2026-{month:02d}" for month in range(1, 7)
]


def _eligibility() -> dict:
    return {
        "entities": [
            {
                "entity_id": "sandbox:lti",
                "entity_type": "sandbox_participant",
                "cnpj": "47006254000180",
                "legal_name": "LTI SEGUROS S.A.",
                "display_name": "LTI Seguros",
                "eligibility": {"regulatory_universe_eligible": False},
            },
            {
                "entity_id": "fip:crefisa-seguros",
                "entity_type": "insurer",
                "cnpj": "50662436000114",
                "legal_name": "CREFISA SEGUROS S.A.",
                "display_name": "Crefisa Seguros",
                "eligibility": {"regulatory_universe_eligible": True},
            },
        ]
    }


def _identity() -> dict:
    return {"source": {"months": MONTHS}}


def _brand_registry() -> dict:
    return {
        "brands": [
            {
                "brand_id": "brand:loovi",
                "name": "Loovi",
                "aliases": ["Loovi Seguros"],
                "representative_cnpj": "34504257000100",
                "risk_carrier_cnpj": "47006254000180",
                "risk_carrier_name": "LTI Seguros S.A.",
                "product_scope": "seguro automotivo",
                "conduct_context_policy": (
                    "carrier_level_context_not_brand_exclusive_attribution"
                ),
                "evidence": [{"authority": "Loovi", "fact": "verified"}],
            }
        ]
    }


def _entry(complaints: int) -> dict:
    return {
        "display_name": "LTI Seguros",
        "statistics": {
            "complaintsCount": complaints,
            "respondedCount": complaints,
            "finalizedCount": complaints,
            "resolvedCount": 0,
            "satisfactionCount": complaints,
            "scoreSum": float(complaints * 3),
        },
    }


def _patch_monthly_loader(monkeypatch, by_month: dict[str, dict]) -> None:
    def fake_load(month: str):
        return {}, by_month[month], Path(f"{month}.json.gz")

    monkeypatch.setattr(
        "api.v2.build_sandbox_brand_conduct_evidence.load_monthly_entries",
        fake_load,
    )


def test_loovi_gets_lti_carrier_context_without_entering_ordinary_ranking(
    monkeypatch,
) -> None:
    by_month = {
        month: {"lti": _entry(100 if index == 0 else 0)}
        for index, month in enumerate(MONTHS)
    }
    _patch_monthly_loader(monkeypatch, by_month)
    payload = build_sandbox_brand_conduct_evidence(
        _eligibility(),
        _identity(),
        brand_registry=_brand_registry(),
    )

    assert payload["ordinary_ranking_effect"] == "none"
    assert payload["summary"]["sandbox_complaints_resolved"] == 100
    brand = payload["brands"][0]
    assert brand["brand_id"] == "brand:loovi"
    assert brand["risk_carrier_entity_id"] == "sandbox:lti"
    assert brand["carrier_conduct_summary"]["complaints"] == 100
    assert brand["ordinary_ranking_effect"] == "none"
    assert "not brand-exclusive" in brand["attribution_note"]


def test_generic_loovi_provider_label_is_not_transferred_to_lti(monkeypatch) -> None:
    by_month = {
        month: {
            "loovi": {
                "display_name": "Loovi",
                "statistics": {"complaintsCount": 50},
            }
        }
        for month in MONTHS
    }
    _patch_monthly_loader(monkeypatch, by_month)
    payload = build_sandbox_brand_conduct_evidence(
        _eligibility(),
        _identity(),
        brand_registry=_brand_registry(),
    )
    assert payload["summary"]["sandbox_complaints_resolved"] == 0
    assert payload["brands"][0]["carrier_conduct_summary"]["complaints"] == 0


def test_generic_noncarrier_brand_guards_are_source_backed() -> None:
    registry = load_provider_resolution_registry()
    for provider in ("Sicoob", "Crefisa", "Loovi"):
        row = registry[provider.casefold()]
        assert row.resolution_state == "outside_157"
        assert row.target_cnpj is None
        assert row.evidence


def test_specific_insurer_names_are_not_replaced_by_generic_guards() -> None:
    registry = load_provider_resolution_registry()
    assert "sicoob seguradora" in registry
    assert registry["sicoob seguradora"].resolution_state == "matched_current_insurer"
    assert "crefisa seguros" not in registry
