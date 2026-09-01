from __future__ import annotations

from copy import deepcopy

import pytest

from api.v2.public_profile_regulatory_semantics import (
    SSPE_ASSESSMENT_REASON,
    SSPE_LABEL,
    apply_regulatory_profile_semantics,
)
from api.v2.validate_public_search_profile_contract import (
    PublicProfileValidationError,
    _profile_maps,
    _validate_profile_references,
)


def _payload() -> dict:
    return {
        "artifact": "v2_public_search_profile_contract",
        "version": "base",
        "publication_policy": {
            "sandbox_enters_ordinary_ranking": False,
        },
        "population": {
            "lifecycle_entities": 2,
            "brands": 0,
            "profiles": 2,
            "search_entries": 2,
            "ordinary_current_insurer_profiles": 1,
            "ordinary_profiles_with_assessment_payload": 1,
            "sandbox_entity_profiles": 0,
            "sandbox_profiles_with_conduct_context": 0,
        },
        "profiles": [
            {
                "profile_id": "entity:fip:003191",
                "profile_kind": "entity",
                "identity": {
                    "entity_id": "fip:003191",
                    "fip_code": "003191",
                    "cnpj": "11111111000111",
                    "legal_name": (
                        "BTG PACTUAL SOCIEDADE SEGURADORA DE PROPÓSITO ESPECÍFICO S.A."
                    ),
                    "display_name": None,
                    "entity_type": "insurer",
                },
                "regulatory": {
                    "regime": "ordinary",
                    "status": "active_licensed",
                    "label": "Seguradora",
                    "query_state": "special_purpose_insurer",
                    "filter_bucket": "other",
                },
                "public_summary": {
                    "headline": "Seguradora",
                    "quick_answer": "Texto genérico.",
                },
                "assessment": {
                    "availability": "not_applicable",
                    "reason": "ordinary_entity_not_present_in_current_assessment_payload",
                },
                "limits": [],
            },
            {
                "profile_id": "entity:fip:000001",
                "profile_kind": "entity",
                "identity": {
                    "entity_id": "fip:000001",
                    "fip_code": "000001",
                    "cnpj": "22222222000122",
                    "legal_name": "SEGURADORA ORDINÁRIA S.A.",
                    "display_name": None,
                    "entity_type": "insurer",
                },
                "regulatory": {
                    "regime": "ordinary",
                    "status": "active_licensed",
                    "label": "Seguradora",
                    "query_state": "current_ordinary_insurer",
                    "filter_bucket": "insurers",
                },
                "public_summary": {
                    "headline": "Seguradora",
                    "quick_answer": "Texto ordinário.",
                },
                "assessment": {
                    "availability": "available",
                    "headline": "Leitura disponível",
                },
                "limits": [],
            },
        ],
        "search_index": [
            {
                "profile_id": "entity:fip:003191",
                "filter_bucket": "other",
                "disambiguation": "Seguradora · CNPJ 11111111000111 · SUSEP 003191",
            },
            {
                "profile_id": "entity:fip:000001",
                "filter_bucket": "insurers",
                "disambiguation": "Seguradora · CNPJ 22222222000122 · SUSEP 000001",
            },
        ],
    }


def test_sspe_is_publicly_disambiguated_without_changing_legal_superclass():
    source = _payload()
    payload = apply_regulatory_profile_semantics(source)
    profiles = {row["profile_id"]: row for row in payload["profiles"]}
    sspe = profiles["entity:fip:003191"]

    assert sspe["identity"]["entity_type"] == "insurer"
    assert sspe["regulatory"]["regime"] == "ordinary"
    assert sspe["regulatory"]["query_state"] == "special_purpose_insurer"
    assert sspe["regulatory"]["label"] == SSPE_LABEL
    assert sspe["public_summary"]["headline"] == SSPE_LABEL
    assert "propósito específico" in sspe["public_summary"]["quick_answer"]
    assert "não recebe avaliação conjunta" in sspe["public_summary"]["quick_answer"]
    assert "posição de ranking" in sspe["public_summary"]["quick_answer"]
    assert sspe["assessment"] == {
        "availability": "not_applicable",
        "reason": SSPE_ASSESSMENT_REASON,
    }

    search = {row["profile_id"]: row for row in payload["search_index"]}
    assert SSPE_LABEL in search["entity:fip:003191"]["disambiguation"]
    assert search["entity:fip:003191"]["filter_bucket"] == "other"
    assert payload["population"]["special_purpose_insurer_profiles"] == 1
    assert payload["publication_policy"]["sspe_enters_ordinary_assessment"] is False
    assert payload["publication_policy"]["sspe_enters_ordinary_ranking"] is False


def test_ordinary_insurer_semantics_are_not_rewritten():
    payload = apply_regulatory_profile_semantics(_payload())
    ordinary = next(
        row for row in payload["profiles"] if row["profile_id"] == "entity:fip:000001"
    )

    assert ordinary["regulatory"]["label"] == "Seguradora"
    assert ordinary["assessment"]["availability"] == "available"
    assert ordinary["public_summary"]["quick_answer"] == "Texto ordinário."


def test_adapter_does_not_mutate_input_payload():
    source = _payload()
    original = deepcopy(source)

    apply_regulatory_profile_semantics(source)

    assert source == original


def test_public_validator_rejects_dangling_profile_reference():
    payload = apply_regulatory_profile_semantics(_payload())
    payload["profiles"][1]["relationships"] = [
        {
            "relationship_type": "brand_carrier",
            "target_profile_id": "entity:fip:999999",
        }
    ]
    profiles, _ = _profile_maps(payload)

    with pytest.raises(
        PublicProfileValidationError,
        match="dangling public profile reference",
    ):
        _validate_profile_references(profiles)


def test_public_validator_accepts_internal_profile_reference():
    payload = apply_regulatory_profile_semantics(_payload())
    payload["profiles"][1]["relationships"] = [
        {
            "relationship_type": "successor",
            "target_profile_id": "entity:fip:003191",
        }
    ]
    profiles, _ = _profile_maps(payload)

    _validate_profile_references(profiles)
